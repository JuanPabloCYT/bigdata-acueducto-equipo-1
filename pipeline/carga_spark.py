"""Primera carga con Spark: agregado diario por sector de la capa refinada del lago.

Este archivo es un ESQUELETO: leer_refinada, agregar y escribir_salida estan por
completar (busquen TODO). La sesion de Spark (crear_sesion), los parametros, la lectura de
la interfaz de Spark, el hash de la salida, la reconciliacion y la bitacora ya funcionan;
el resto del texto de este encabezado describe el programa terminado.

Que hace
    Lee la capa refinada (Parquet) del lago del acueducto, calcula el consumo total y el
    numero de lecturas por dia y sector, y escribe el resultado en Parquet:
      1. Crea una sesion local de Spark (local[N]) con la zona horaria fijada en UTC.
      2. Lee la carpeta refinada completa. La marca temporal viene guardada en
         NANOSEGUNDOS y Spark 3.5 no la lee como marca temporal: se pide como entero
         largo (spark.sql.legacy.parquet.nanosAsLong=true) y se convierte a segundos.
      3. Agrega por (fecha, sector) con reduceByKey (combina dentro de cada particion
         antes de mezclar) o con groupByKey (mezcla todos los valores): se elige con
         --metodo para comparar los bytes de mezcla en la interfaz de Spark.
      4. Reconcilia el resultado SIN filtro de calidad contra una referencia
         independiente: la capa consolidada del lago (volumen base) o los totales de
         control de la ampliacion (volumen ampliacion).
      5. Escribe el resultado con mode("overwrite"), de modo que repetir la carga deja
         el mismo contenido y no duplica filas.
      6. Anexa una linea a bitacora_carga_spark.jsonl con la configuracion usada y lo
         observado en la interfaz de Spark (particiones, etapas, tareas, bytes de
         mezcla, tiempo, hash de la salida y resultado de la reconciliacion).

Que significa "misma salida"
    Mismo contenido: mismo numero de filas, mismas sumas y mismo hash de las filas
    ordenadas por (fecha, sector) con el consumo redondeado a 6 decimales. Los nombres de
    los archivos Parquet que escribe Spark cambian en cada corrida; el contenido no.
    La suma de numeros reales puede variar en la ultima cifra segun el orden en que se
    combinan las particiones: por eso el hash redondea y la reconciliacion usa una
    tolerancia de 1e-6.

Como usarlo (desde la carpeta raiz del paquete, dentro del contenedor del perfil s15)
    python pipeline/carga_spark.py --equipo equipo-3 --volumen base
    python pipeline/carga_spark.py --equipo equipo-3 --volumen ampliacion --metodo groupByKey
    python pipeline/carga_spark.py --volumen ampliacion --particiones-mezcla 8 --aqe false
    Para el contraste con la capa cruda (CSV leida con textFile): --ruta csv_contraste

Ejercicio didactico y produccion
    Aqui se usa la API de RDD para ver el mecanismo (etapas, mezcla, particiones). En
    produccion se prefieren DataFrames con esquema declarado, que Spark optimiza solo, y
    la sobrescritura se limita a las particiones que cambian (por fecha) en vez de
    reescribir todo. Con 576 000 filas, Spark en una maquina no es mas rapido que pandas
    o DuckDB: se usa para aprender el modelo de ejecucion, no por necesidad de escala.
"""
import argparse
import datetime as dt
import glob
import hashlib
import json
import time
import urllib.request
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession

TOLERANCIA = 1e-6
RUTAS = ["parquet_refinada", "csv_contraste"]
METODOS = ["reduceByKey", "groupByKey"]
VOLUMENES = ["base", "ampliacion"]


# ---------------------------------------------------------------- parametros
def leer_parametros(argv=None):
    ap = argparse.ArgumentParser(description="Carga de la capa refinada con Spark (agregado diario por sector).")
    ap.add_argument("--equipo", default="sin-equipo", help="Identificador del equipo (se anota en la bitacora)")
    ap.add_argument("--volumen", choices=VOLUMENES, default="base",
                    help="base: datalake/ (200 medidores) · ampliacion: datalake_equipo/ (su ampliacion)")
    ap.add_argument("--ruta", choices=RUTAS, default="parquet_refinada",
                    help="parquet_refinada: la ruta principal · csv_contraste: capa cruda con textFile")
    ap.add_argument("--master", default="local[4]", help="Modo local con N ranuras de tareas (por omision local[4])")
    ap.add_argument("--driver-memory", default="1g", help="Memoria de la JVM del controlador (por omision 1g)")
    ap.add_argument("--aqe", choices=["true", "false"], default="true", help="spark.sql.adaptive.enabled")
    ap.add_argument("--metodo", choices=METODOS, default="reduceByKey", help="Operacion de agregacion")
    ap.add_argument("--particiones-mezcla", type=int, default=None,
                    help="Particiones despues de la mezcla (por omision, las de entrada)")
    ap.add_argument("--raiz", default=".", help="Carpeta raiz del paquete (donde estan datalake/ y pipeline/)")
    ap.add_argument("--salida", default=None, help="Carpeta de salida (por omision <capa>/consolidada_spark)")
    ap.add_argument("--bitacora", default="bitacora_carga_spark.jsonl", help="Archivo de la bitacora (se anexa)")
    return ap.parse_args(argv)


def carpeta_volumen(raiz, volumen):
    return Path(raiz) / ("datalake" if volumen == "base" else "datalake_equipo")


# -------------------------------------------------------------------- sesion
def crear_sesion(master, driver_memory, aqe):
    """Crea la sesion de Spark. spark.driver.memory solo tiene efecto ANTES de que
    arranque la JVM: getOrCreate() devuelve la sesion existente e ignora una memoria
    nueva. En modo local no hay ejecutores aparte, asi que spark.executor.memory no
    aplica: la memoria disponible es la del controlador."""
    spark = (SparkSession.builder.master(master).appName("carga_spark")
             .config("spark.driver.memory", driver_memory)
             .config("spark.sql.adaptive.enabled", aqe)
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.legacy.parquet.nanosAsLong", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ------------------------------------------------------------------- lectura
def leer_refinada(spark, carpeta_refinada):
    """Lee la capa refinada y devuelve un RDD de pares ((fecha, sector), consumo_m3).

    TODO (equipo):
      1. Lean la carpeta completa con spark.read.parquet. Revisen el esquema: que tipo
         tiene la marca temporal y que columnas agrego Spark a partir de la ruta?
      2. Conviertan la marca temporal (nanosegundos desde la epoca) a segundos y de ahi a
         fecha, en la zona horaria de la sesion. Escriban la conversion con una expresion
         de Spark SQL y verifiquen una fila a mano.
      3. Pasen a RDD (.rdd) y armen los pares con la fecha como texto AAAA-MM-DD.
    Una transformacion no ejecuta nada: el error de una conversion mal escrita aparece
    al ejecutar la accion, no aqui.

    Decision (equipo-1): la marca llega como entero largo de nanosegundos (por
    spark.sql.legacy.parquet.nanosAsLong=true). Se divide ENTERO entre 10^9 (div, no /,
    para no pasar por double) y timestamp_seconds la interpreta en UTC, la zona de la
    sesion; date_format da el texto AAAA-MM-DD. Verificacion a mano: 1704067200000000000 ns
    -> 1704067200 s -> 2024-01-01 00:00:00 UTC -> '2024-01-01'. Las columnas anio, mes y
    dia que Spark agrega desde la ruta (descubrimiento de particiones) no se usan: la
    fecha sale del dato, no de la carpeta. No hay filtro de calidad: se suman todas las
    lecturas, como exige la reconciliacion."""
    df = spark.read.parquet(str(carpeta_refinada))
    df = df.selectExpr(
        "date_format(timestamp_seconds(`timestamp` div 1000000000), 'yyyy-MM-dd') AS fecha",
        "sector",
        "consumo_m3",
    )
    return df.rdd.map(lambda r: ((r.fecha, r.sector), float(r.consumo_m3)))


def leer_cruda_csv(spark, carpeta_cruda):
    """Contraste: lee la capa cruda (un CSV por dia) con textFile. Cada archivo da al
    menos una particion, y la fila de encabezado de cada CSV llega como dato: hay que
    descartarla. El texto 'AAAA-MM-DD HH:MM:SS' ya trae la fecha en los 10 primeros
    caracteres."""
    lineas = spark.sparkContext.textFile(str(carpeta_cruda / "acueducto" / "*" / "*" / "*" / "lecturas.csv"))

    def parsear(linea):
        meter_id, ts, sector, consumo, presion, flag = linea.split(",")
        return ((ts[:10], sector), float(consumo))

    return lineas.filter(lambda linea: not linea.startswith("meter_id")).map(parsear)


# ----------------------------------------------------------------- agregacion
def agregar(pares, metodo, particiones_mezcla=None):
    """Suma el consumo y cuenta las lecturas por (fecha, sector).

    Devuelve un RDD de tuplas (fecha, sector, consumo_total, n_lecturas).

    TODO (equipo): implementen las dos opciones de metodo.
      - "reduceByKey": combina dentro de cada particion antes de mezclar. Piensen que
        valor viaja por clave para poder sumar el consumo Y contar las lecturas.
      - "groupByKey": mezcla todos los valores y suma despues.
    Ambas reciben particiones_mezcla (None = por omision). Justifiquen en su registro de
    decisiones por que reduceByKey es valido aqui (funcion asociativa y conmutativa).

    Con reduceByKey viaja por clave el par (suma_consumo, n_lecturas): combinar dos
    parciales (a1, n1) y (a2, n2) en (a1 + a2, n1 + n2) es asociativo y conmutativo, asi
    que Spark puede combinar dentro de cada particion antes de mezclar (combinador del
    lado del mapa) y en cualquier orden despues. groupByKey, en cambio, mezcla cada
    consumo individual y suma al final."""
    if metodo == "reduceByKey":
        agregado = (pares.mapValues(lambda c: (c, 1))
                    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]), numPartitions=particiones_mezcla))
        return agregado.map(lambda kv: (kv[0][0], kv[0][1], kv[1][0], kv[1][1]))
    if metodo == "groupByKey":
        agrupado = pares.groupByKey(numPartitions=particiones_mezcla)
        return agrupado.map(lambda kv: (kv[0][0], kv[0][1], float(sum(kv[1])), len(kv[1])))
    raise ValueError(f"metodo desconocido: {metodo}")


# ------------------------------------------------------------ interfaz Spark
def _rest(spark, ruta):
    puerto = spark.sparkContext.uiWebUrl.rsplit(":", 1)[-1]
    url = f"http://localhost:{puerto}/api/v1/applications/{spark.sparkContext.applicationId}/{ruta}"
    with urllib.request.urlopen(url) as respuesta:
        return json.load(respuesta)


def siguiente_etapa(spark):
    """Identificador de la proxima etapa: sirve para medir solo lo que ocurre despues."""
    return max([e["stageId"] for e in _rest(spark, "stages")], default=-1) + 1


def metricas_interfaz(spark, desde_etapa):
    """Etapas, tareas y bytes de mezcla de las etapas ejecutadas desde desde_etapa,
    leidos de la misma API que alimenta la interfaz de Spark."""
    etapas = [e for e in _rest(spark, "stages") if e["stageId"] >= desde_etapa and e["status"] == "COMPLETE"]
    return {
        "etapas": len(etapas),
        "tareas": sum(e["numCompleteTasks"] for e in etapas),
        "mezcla_escritura_bytes": sum(e["shuffleWriteBytes"] for e in etapas),
        "mezcla_lectura_bytes": sum(e["shuffleReadBytes"] for e in etapas),
    }


# -------------------------------------------------------------------- hash
def hash_salida(filas):
    """Hash del CONTENIDO: filas ordenadas por (fecha, sector) y consumo redondeado a
    6 decimales. Dos corridas que producen el mismo contenido producen el mismo hash."""
    ordenadas = sorted(filas, key=lambda f: (f[0], f[1]))
    texto = "".join(f"{f[0]},{f[1]},{f[2]:.6f},{f[3]}\n" for f in ordenadas)
    return hashlib.sha256(texto.encode("utf-8")).hexdigest()


# ------------------------------------------------------------ reconciliacion
def referencia_base(carpeta_lago):
    """Capa consolidada del lago base: un Parquet por dia con (sector, consumo_total_m3,
    presion_media_bar, n_lecturas). La fecha esta en la ruta anio=AAAA/mes=MM/dia=DD."""
    filas = []
    patron = str(carpeta_lago / "consolidada" / "acueducto" / "anio=*" / "mes=*" / "dia=*" / "consumo_diario.parquet")
    for archivo in sorted(glob.glob(patron)):
        partes = Path(archivo).parts
        fecha = f"{partes[-4][5:]}-{partes[-3][4:]}-{partes[-2][4:]}"
        for r in pd.read_parquet(archivo).itertuples():
            filas.append((fecha, r.sector, float(r.consumo_total_m3), int(r.n_lecturas)))
    return filas


def referencia_ampliacion(carpeta_equipo):
    """Totales de control de la ampliacion: filas y suma de consumo por dia y sector,
    sin filtro de calidad."""
    t = pd.read_csv(carpeta_equipo / "totales_control.csv")
    return [(r.fecha, r.sector, float(r.consumo_total_m3), int(r.n_lecturas)) for r in t.itertuples()]


def reconciliar(filas, referencia):
    """Compara el resultado con la referencia: mismas claves, mismo numero de lecturas y
    suma de consumo igual dentro de la tolerancia. Devuelve (ok, detalle)."""
    calc = {(f[0], f[1]): (f[2], f[3]) for f in filas}
    ref = {(f[0], f[1]): (f[2], f[3]) for f in referencia}
    if set(calc) != set(ref):
        return False, f"claves distintas: {len(calc)} calculadas frente a {len(ref)} de referencia"
    malas_n = [k for k in ref if calc[k][1] != ref[k][1]]
    malas_c = [k for k in ref if abs(calc[k][0] - ref[k][0]) > TOLERANCIA]
    if malas_n or malas_c:
        return False, f"{len(malas_n)} grupos con distinto numero de lecturas y {len(malas_c)} con distinta suma"
    return True, f"{len(ref)} grupos iguales a la referencia (tolerancia {TOLERANCIA})"


# ------------------------------------------------------------------ escritura
def escribir_salida(spark, filas, carpeta_salida):
    """Escribe el resultado en carpeta_salida de forma que repetir la carga NO duplique filas.

    TODO (equipo):
      - Conviertan las filas a un DataFrame con nombres de columna fecha, sector,
        consumo_total_m3 y n_lecturas (fecha como DATE).
      - Decidan cuantos archivos escribir (480 filas) y con que modo de escritura; el
        modo debe hacer idempotente la carga. Expliquen la decision en su registro.

    Decision (equipo-1): un solo archivo (coalesce(1)) y mode("overwrite"). Son 480 filas
    (unos 10 KiB): partirlas por fecha dejaria 120 archivos diminutos y 120 carpetas que
    cuestan mas en metadatos que en datos. overwrite reemplaza la carpeta completa, asi
    que repetir la carga deja el mismo contenido; un append duplicaria las 480 filas en
    cada corrida.
    """
    from pyspark.sql.types import DateType, DoubleType, LongType, StringType, StructField, StructType
    esquema = StructType([
        StructField("fecha", DateType(), False),
        StructField("sector", StringType(), False),
        StructField("consumo_total_m3", DoubleType(), False),
        StructField("n_lecturas", LongType(), False),
    ])
    datos = [(dt.date.fromisoformat(f[0]), f[1], round(float(f[2]), 6), int(f[3]))
             for f in sorted(filas, key=lambda f: (f[0], f[1]))]
    (spark.createDataFrame(datos, schema=esquema)
     .coalesce(1)
     .write.mode("overwrite")
     .parquet(str(carpeta_salida)))


# ------------------------------------------------------------------- bitacora
def registrar_bitacora(ruta_bitacora, registro):
    """Anexa una linea JSON por corrida. La bitacora es un historial: se anexa y no
    forma parte de 'la misma salida' que se compara entre corridas."""
    with open(ruta_bitacora, "a", encoding="utf-8") as f:
        f.write(json.dumps(registro, ensure_ascii=False) + "\n")


# --------------------------------------------------------------------- principal
def main(argv=None):
    a = leer_parametros(argv)
    carpeta = carpeta_volumen(a.raiz, a.volumen)
    salida = Path(a.salida) if a.salida else carpeta / "consolidada_spark"

    spark = crear_sesion(a.master, a.driver_memory, a.aqe)
    try:
        if a.ruta == "parquet_refinada":
            pares = leer_refinada(spark, carpeta / "refinada" / "acueducto")
        else:
            pares = leer_cruda_csv(spark, carpeta / "cruda")
        particiones_entrada = pares.getNumPartitions()
        resultado = agregar(pares, a.metodo, a.particiones_mezcla)
        particiones_mezcla = resultado.getNumPartitions()

        etapa0 = siguiente_etapa(spark)
        t0 = time.time()
        filas = resultado.collect()          # la accion: aqui se ejecuta todo el grafo
        tiempo = round(time.time() - t0, 3)
        interfaz = metricas_interfaz(spark, etapa0)

        referencia = (referencia_base(carpeta) if a.volumen == "base" else referencia_ampliacion(carpeta))
        ok, detalle = reconciliar(filas, referencia)
        escribir_salida(spark, filas, salida)

        registro = {
            "marca_tiempo": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "equipo": a.equipo, "volumen": a.volumen, "ruta": a.ruta, "metodo": a.metodo,
            "master": a.master, "driver_memory": spark.sparkContext.getConf().get("spark.driver.memory"),
            "aqe": a.aqe,
            "particiones_entrada": particiones_entrada, "particiones_mezcla": particiones_mezcla,
            **interfaz,
            "tiempo_s": tiempo,
            "filas_leidas": sum(f[3] for f in filas), "filas_salida": len(filas),
            "hash_salida": hash_salida(filas),
            "reconciliacion": "OK" if ok else "FALLA", "reconciliacion_detalle": detalle,
        }
        registrar_bitacora(Path(a.raiz) / a.bitacora, registro)
        print(json.dumps(registro, ensure_ascii=False, indent=2))
        return 0 if ok else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
