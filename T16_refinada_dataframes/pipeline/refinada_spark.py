"""Capa refinada del acueducto con DataFrames de Spark: esquema declarado, tipos y funciones propias.

Este archivo es un ESQUELETO: definir_esquema, columnas_derivadas, clasificar_nativa y
escribir_capa las escriben ustedes (busquen TODO). La sesion de Spark, la lectura de los CSV, las
dos funciones propias (por fila y vectorizada), la firma por grupo, la reconciliacion y la
bitacora ya funcionan; el resto del texto de este encabezado describe el programa terminado.

Que hace
    Construye la capa refinada_spark/ del lago desde la capa CRUDA (CSV), no desde la refinada
    heredada, y la deja lista para que cualquier lector la entienda:
      1. Crea una sesion local de Spark (local[N]) con la zona horaria fijada en UTC.
      2. Lee los CSV con un ESQUEMA DECLARADO (tipos explicitos, ninguna columna inferida) y
         modo PERMISSIVE. Un tipo mal declarado deja nulos sin error: por eso se cuentan los
         nulos por columna antes de escribir.
      3. Deriva columnas: fecha, hora, es_confiable, nivel_presion (baja, normal o alta) y las
         tres partes de la ruta (anio, mes, dia) como TEXTO con ceros, para respetar la
         convencion de rutas anio=AAAA/mes=MM/dia=DD del lago.
      4. Escribe Parquet con la marca temporal en MICROSEGUNDOS (INT64 con tipo logico de marca
         temporal), particionado por dia y con sobrescritura dinamica de particiones: reprocesar
         unos dias reemplaza solo esos dias.
      5. Reconcilia SIN filtro de calidad la capa escrita contra la refinada heredada (que guarda
         la marca temporal en nanosegundos), contra consolidada/ o totales_control.csv, y contra
         consolidada_spark/ si existe. La comparacion es por grupo (fecha, sector, calidad):
         filas, suma de consumo, suma de presion y marca temporal minima y maxima.
      6. Anexa una linea a bitacora_refinada_spark.jsonl con la configuracion y lo observado.
    Con --comparar-variantes mide las tres formas de clasificar la presion (expresion nativa,
    funcion propia por fila y funcion propia vectorizada) con el protocolo: una corrida de
    calentamiento descartada y tres corridas medidas, con una accion que CONSUME la clase.

Que significa "misma salida"
    Mismo contenido: mismas filas, mismas sumas (redondeadas a 6 decimales) y mismas marcas
    temporales por grupo. Los nombres de los archivos part-*.parquet cambian en cada corrida; el
    contenido no. El hash de la firma por grupo es la prueba.

Como usarlo (desde la carpeta raiz del paquete, dentro del contenedor del servicio spark)
    python pipeline/refinada_spark.py --equipo equipo-3 --volumen base
    python pipeline/refinada_spark.py --equipo equipo-3 --volumen ampliacion
    python pipeline/refinada_spark.py --volumen base --desde 2024-01-01 --hasta 2024-01-06 \\
           --salida copia_refinada_spark --sobrescritura estatica
    python pipeline/refinada_spark.py --volumen ampliacion --comparar-variantes

Ejercicio didactico y produccion
    Aqui el volumen es pequeno (de 576 000 a 1 641 600 filas) y las diferencias de tiempo entre
    variantes se miden en segundos. En produccion la expresion nativa es la opcion por omision,
    la funcion vectorizada es el segundo recurso y la funcion por fila el ultimo; los archivos se
    compactan a un tamano mucho mayor; y cuando la escritura debe ser atomica se usa un formato
    de tabla transaccional en lugar de carpetas de Parquet.
"""
import argparse
import contextlib
import datetime as dt
import glob
import hashlib
import io
import json
import os
import statistics
import time
import urllib.request
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

TOLERANCIA = 1e-6
UMBRAL_BAJA = 2.5      # bar: menos que esto es presion baja
UMBRAL_ALTA = 3.5      # bar: mas que esto es presion alta
VOLUMENES = ["base", "ampliacion"]
VARIANTES = ["nativa", "udf_fila", "udf_pandas"]
SOBRESCRITURAS = ["dinamica", "estatica"]
COLUMNAS_REQUERIDAS = ["meter_id", "timestamp", "sector", "consumo_m3", "presion_bar", "quality_flag"]


# ---------------------------------------------------------------- parametros
def leer_parametros(argv=None):
    ap = argparse.ArgumentParser(description="Capa refinada con DataFrames: esquema declarado y clasificacion de presion.")
    ap.add_argument("--equipo", default="sin-equipo", help="Identificador del equipo (se anota en la bitacora)")
    ap.add_argument("--volumen", choices=VOLUMENES, default="base",
                    help="base: datalake/ (200 medidores) · ampliacion: datalake_equipo/ (su ampliacion)")
    ap.add_argument("--carpeta", default=None, help="Carpeta del lago a leer (por omision datalake/ o datalake_equipo/)")
    ap.add_argument("--master", default="local[4]", help="Modo local con N ranuras de tareas (por omision local[4])")
    ap.add_argument("--driver-memory", default="1g", help="Memoria de la JVM del controlador (por omision 1g)")
    ap.add_argument("--variante", choices=VARIANTES, default="nativa", help="Como se calcula nivel_presion")
    ap.add_argument("--sobrescritura", choices=SOBRESCRITURAS, default="dinamica",
                    help="dinamica: reemplaza solo las particiones que se escriben · estatica: borra toda la tabla")
    ap.add_argument("--desde", default=None, help="Primer dia a procesar AAAA-MM-DD (reproceso de una ventana)")
    ap.add_argument("--hasta", default=None, help="Ultimo dia a procesar AAAA-MM-DD (incluido)")
    ap.add_argument("--comparar-variantes", action="store_true",
                    help="Mide las tres variantes de clasificacion (1 calentamiento + 3 corridas) y no escribe la capa")
    ap.add_argument("--corridas", type=int, default=3, help="Corridas medidas por variante (mas 1 de calentamiento)")
    ap.add_argument("--raiz", default=".", help="Carpeta raiz del paquete (donde estan datalake/ y pipeline/)")
    ap.add_argument("--salida", default=None, help="Carpeta de la capa nueva (por omision <lago>/refinada_spark)")
    ap.add_argument("--bitacora", default="bitacora_refinada_spark.jsonl", help="Archivo de la bitacora (se anexa)")
    return ap.parse_args(argv)


def carpeta_volumen(raiz, volumen, carpeta=None):
    if carpeta:
        return Path(carpeta)
    return Path(raiz) / ("datalake" if volumen == "base" else "datalake_equipo")


# -------------------------------------------------------------------- sesion
def crear_sesion(master="local[4]", driver_memory="1g"):
    """Crea la sesion de Spark. spark.driver.memory solo tiene efecto ANTES de que arranque la
    JVM. La zona horaria UTC hace que el texto sin zona '2024-01-01 13:00:00' se interprete como
    ese mismo instante en UTC. La opcion nanosAsLong permite LEER la refinada heredada, que guarda
    la marca temporal en nanosegundos, como un entero largo para compararla."""
    spark = (SparkSession.builder.master(master).appName("refinada_spark")
             .config("spark.driver.memory", driver_memory)
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.legacy.parquet.nanosAsLong", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ------------------------------------------------------------------- esquema
def definir_esquema():
    """Esquema DECLARADO de la capa cruda.

    TODO (equipo): devuelvan un StructType con las seis columnas del CSV, en el MISMO orden
    del encabezado: meter_id, timestamp, sector, consumo_m3, presion_bar y quality_flag.
      - Elijan el tipo de cada una (texto, marca temporal, numero) y justifiquenlo en su
        registro de decisiones: que pasa con un tipo entero para consumo_m3? y con un
        float en lugar de un double?
      - Con esquema declarado los tipos se asignan por POSICION, no por nombre.
      - Ninguna columna se infiere.

    Decision (equipo-1): meter_id, sector y quality_flag como texto (son codigos, no numeros:
    'MED0001' o 'S-SUR' no admiten aritmetica); timestamp como TimestampType (instante en UTC
    con precision de microsegundos, la de la capa); consumo_m3 y presion_bar como DoubleType.
    Un entero truncaria 0,209 m3 a 0 y el consumo diario se perderia casi entero; un float de
    32 bits tiene ~7 cifras significativas y la suma de 1 200 lecturas por grupo acumularia un
    error del orden de 1e-4 m3, mayor que la tolerancia de 1e-6 de la reconciliacion."""
    return StructType([
        StructField("meter_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("sector", StringType(), True),
        StructField("consumo_m3", DoubleType(), True),
        StructField("presion_bar", DoubleType(), True),
        StructField("quality_flag", StringType(), True),
    ])


def leer_cruda(spark, carpeta_cruda, esquema):
    """Lee la carpeta cruda completa con el esquema declarado y modo PERMISSIVE (un valor que no
    convierte queda nulo; no detiene la lectura). El formato de la marca temporal se declara
    de forma explicita. Con enforceSchema=False Spark compara el encabezado de cada archivo con el
    esquema declarado, por posicion: si una fuente cambia el orden de las columnas, la lectura falla
    en lugar de asignar los tipos a la columna equivocada (con el valor por omision, True, el
    encabezado se ignora y los datos quedan en la columna equivocada sin ningun error). Al leer la carpeta, Spark agrega anio, mes y dia como enteros tomados de
    la ruta: se descartan, porque la verdad esta en la marca temporal y se deriva de ella."""
    df = (spark.read.csv(str(carpeta_cruda), header=True, schema=esquema, mode="PERMISSIVE",
                         timestampFormat="yyyy-MM-dd HH:mm:ss", enforceSchema=False))
    return df.drop("anio", "mes", "dia")


# ---------------------------------------------------- clasificacion de presion
@F.udf(returnType=StringType())
def clasificar_udf_fila(presion):
    """Funcion propia por fila: Spark serializa cada fila hacia un proceso de Python y la
    invoca una vez por fila. Un valor nulo llega como None."""
    if presion is None:
        return None
    if presion < UMBRAL_BAJA:
        return "baja"
    if presion > UMBRAL_ALTA:
        return "alta"
    return "normal"


@F.pandas_udf(StringType())
def clasificar_udf_pandas(presion: pd.Series) -> pd.Series:
    """Funcion propia vectorizada: recibe una serie completa (un lote de filas en formato Arrow) y
    se invoca una vez por lote, con operaciones vectorizadas de pandas."""
    clase = pd.Series("normal", index=presion.index, dtype="object")
    clase[presion < UMBRAL_BAJA] = "baja"
    clase[presion > UMBRAL_ALTA] = "alta"
    clase[presion.isna()] = None
    return clase


def clasificar_nativa(presion):
    """Expresion nativa de columna para clasificar la presion.

    TODO (equipo): reciban una COLUMNA (presion) y devuelvan una columna con 'baja' si la
    presion es menor que UMBRAL_BAJA, 'alta' si es mayor que UMBRAL_ALTA y 'normal' en otro
    caso. Usen when(...).when(...) de pyspark.sql.functions, sin funciones de Python.
    Decidan que clase le corresponde a una presion nula y que sea la misma que dan las
    dos funciones propias de abajo: las tres variantes deben producir el mismo conteo por clase.

    Decision (equipo-1): una presion nula queda con clase NULA, igual que en las dos funciones
    propias (None). Inventarle 'normal' ocultaria un dato faltante como si fuera sano."""
    return (F.when(presion < UMBRAL_BAJA, "baja")
             .when(presion > UMBRAL_ALTA, "alta")
             .when(presion.isNotNull(), "normal"))


def columna_nivel_presion(variante):
    presion = F.col("presion_bar")
    if variante == "nativa":
        return clasificar_nativa(presion)
    if variante == "udf_fila":
        return clasificar_udf_fila(presion)
    if variante == "udf_pandas":
        return clasificar_udf_pandas(presion)
    raise ValueError(f"variante desconocida: {variante}")


# ------------------------------------------------------------- transformacion
def columnas_derivadas(df):
    """Deriva las columnas de la capa desde la marca temporal y la bandera de calidad.

    TODO (equipo): agreguen estas columnas con withColumn, todas a partir de las existentes:
      - fecha: la fecha de la lectura (tipo fecha).
      - hora: la hora del dia, de 0 a 23.
      - es_confiable: verdadero si quality_flag es 'OK'. La capa NO filtra filas.
      - anio, mes y dia: las tres partes de la ruta como TEXTO con ceros ('2024', '01',
        '05'), tomadas de la marca temporal. La convencion del lago es anio=AAAA/mes=MM/dia=DD:
        que ocurre si mes queda como entero?
    Devuelvan el DataFrame con las columnas nuevas; nivel_presion se agrega despues.

    Si mes quedara entero, la ruta seria mes=1 en lugar de mes=01: rompe la convencion del lago,
    el orden lexicografico de las carpetas (mes=10 antes de mes=2) y cualquier lector que
    construya la ruta con ceros."""
    ts = F.col("timestamp")
    return (df.withColumn("fecha", F.to_date(ts))
              .withColumn("hora", F.hour(ts))
              .withColumn("es_confiable", F.col("quality_flag") == F.lit("OK"))
              .withColumn("anio", F.date_format(ts, "yyyy"))
              .withColumn("mes", F.date_format(ts, "MM"))
              .withColumn("dia", F.date_format(ts, "dd")))


def transformar(df, variante="nativa"):
    """Columnas derivadas mas la clase de presion con la variante elegida."""
    return columnas_derivadas(df).withColumn("nivel_presion", columna_nivel_presion(variante))


def resumen_calidad(df, columnas=COLUMNAS_REQUERIDAS):
    """En UNA pasada cuenta las filas, los nulos de cada columna y las filas no conformes (con algun
    nulo en una columna requerida). Referenciar la columna obliga a Spark a convertirla (el lector
    solo convierte lo que el plan necesita), de modo que un tipo mal declarado se ve aqui y no con
    un simple count()."""
    alguno = F.lit(False)
    for c in columnas:
        alguno = alguno | F.col(c).isNull()
    fila = df.agg(F.count("*").alias("filas"),
                  F.sum(alguno.cast("int")).alias("no_conformes"),
                  *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in columnas]).collect()[0]
    return {"filas": int(fila["filas"]), "no_conformes": int(fila["no_conformes"] or 0),
            "nulos": {c: int(fila[c] or 0) for c in columnas}}


# ------------------------------------------------------------------ escritura
def escribir_capa(df, ruta, sobrescritura="dinamica"):
    """Escribe la capa en Parquet particionado por dia.

    TODO (equipo):
      - Marca temporal en MICROSEGUNDOS: fijen spark.sql.parquet.outputTimestampType en
        TIMESTAMP_MICROS (el valor por omision es INT96, que otros lectores interpretan distinto).
      - Particion por anio, mes y dia (partitionBy) con modo overwrite.
      - Sobrescritura: fijen spark.sql.sources.partitionOverwriteMode en 'dynamic' o 'static'
        segun el parametro `sobrescritura` ('dinamica' o 'estatica'). Con la estatica, escribir
        una ventana de dias borra toda la tabla. Despues de escribir, guarden en su registro
        cuantas filas quedaron en cada caso.
    Usen df.sparkSession para acceder a la configuracion.

    Decision (equipo-1): un archivo por dia (repartition por anio, mes y dia antes de escribir):
    4 800 filas por dia en la base y de 8 352 a 13 680 en la ampliacion caben en un archivo de
    unos cientos de KiB; varios archivos por carpeta solo multiplicarian metadatos."""
    spark = df.sparkSession
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode",
                   {"dinamica": "dynamic", "estatica": "static"}[sobrescritura])
    (df.repartition("anio", "mes", "dia")
       .write.mode("overwrite")
       .partitionBy("anio", "mes", "dia")
       .parquet(str(ruta)))


def resumen_arbol(ruta):
    """Por carpeta de dia: nombres de sus archivos part-*.parquet. Sirve para comprobar que un
    reproceso dejo intactas las particiones que no tocaba (sus nombres no cambian)."""
    arbol = {}
    for carpeta in sorted(glob.glob(str(Path(ruta) / "anio=*" / "mes=*" / "dia=*"))):
        arbol[os.path.relpath(carpeta, ruta).replace(os.sep, "/")] = tuple(sorted(
            f for f in os.listdir(carpeta) if f.endswith(".parquet")))
    return arbol


def comparar_arboles(antes, despues):
    """(intactas, reescritas, perdidas, nuevas) entre dos resumenes de arbol."""
    intactas = [k for k in antes if k in despues and antes[k] == despues[k]]
    reescritas = [k for k in antes if k in despues and antes[k] != despues[k]]
    perdidas = [k for k in antes if k not in despues]
    nuevas = [k for k in despues if k not in antes]
    return len(intactas), len(reescritas), len(perdidas), len(nuevas)


# --------------------------------------------------------------------- firma
def firma_por_grupo(df, col_micro):
    """Firma por (fecha, sector, calidad): filas, suma de consumo, suma de presion y marca
    temporal minima y maxima como entero de microsegundos. Se calcula con Spark y se devuelve a
    pandas (1 440 filas en la base)."""
    g = (df.groupBy(F.col("fecha").cast("string").alias("fecha"), "sector", "quality_flag")
           .agg(F.count("*").alias("n"), F.sum("consumo_m3").alias("suma_consumo"),
                F.sum("presion_bar").alias("suma_presion"),
                F.min(col_micro).alias("ts_min_us"), F.max(col_micro).alias("ts_max_us")))
    return g.toPandas().sort_values(["fecha", "sector", "quality_flag"]).reset_index(drop=True)


def firma_capa_nueva(spark, ruta):
    """Firma de la capa escrita, releida desde disco."""
    df = spark.read.parquet(str(ruta))
    return firma_por_grupo(df.withColumn("ts_us", F.unix_micros("timestamp")), "ts_us")


def firma_heredada(spark, carpeta_refinada):
    """Firma de la refinada heredada. Su marca temporal viene en NANOSEGUNDOS como entero largo
    (nanosAsLong): se divide entre 1 000 para compararla con los microsegundos de la capa nueva y
    entre 10^9 para derivar la fecha."""
    df = spark.read.parquet(str(carpeta_refinada))
    df = (df.withColumn("fecha", F.to_date(F.timestamp_seconds(F.expr("`timestamp` DIV 1000000000"))))
            .withColumn("ts_us", F.expr("`timestamp` DIV 1000")))
    return firma_por_grupo(df, "ts_us")


def comparar_firmas(a, b, tolerancia=TOLERANCIA):
    """Compara dos firmas por grupo. Devuelve (ok, detalle, diferencia maxima de las sumas)."""
    claves = ["fecha", "sector", "quality_flag"]
    m = a.merge(b, on=claves, how="outer", suffixes=("_a", "_b"), indicator=True)
    solo = int((m["_merge"] != "both").sum())
    if solo:
        return False, f"{solo} grupos presentes en solo una de las dos firmas", None
    d_n = int((m["n_a"] != m["n_b"]).sum())
    d_ts = int(((m["ts_min_us_a"] != m["ts_min_us_b"]) | (m["ts_max_us_a"] != m["ts_max_us_b"])).sum())
    dif = float(max((m["suma_consumo_a"] - m["suma_consumo_b"]).abs().max(),
                    (m["suma_presion_a"] - m["suma_presion_b"]).abs().max()))
    if d_n or d_ts or dif > tolerancia:
        return False, f"{d_n} grupos con distinto numero de filas, {d_ts} con distinta marca temporal, diferencia maxima de sumas {dif:.2e}", dif
    return True, f"{len(m)} grupos iguales (tolerancia {tolerancia}); diferencia maxima de sumas {dif:.2e}", dif


def agregado_por_dia_sector(firma):
    """Filas y suma de consumo por (fecha, sector), sin filtro de calidad."""
    g = firma.groupby(["fecha", "sector"]).agg(n_lecturas=("n", "sum"), consumo_total_m3=("suma_consumo", "sum"))
    return g.reset_index()


def referencia_consolidada(carpeta_lago):
    """consolidada/ del lago base: un Parquet por dia con (sector, consumo_total_m3, n_lecturas)."""
    filas = []
    patron = str(carpeta_lago / "consolidada" / "acueducto" / "anio=*" / "mes=*" / "dia=*" / "consumo_diario.parquet")
    for archivo in sorted(glob.glob(patron)):
        partes = Path(archivo).parts
        fecha = f"{partes[-4][5:]}-{partes[-3][4:]}-{partes[-2][4:]}"
        for r in pd.read_parquet(archivo).itertuples():
            filas.append((fecha, r.sector, float(r.consumo_total_m3), int(r.n_lecturas)))
    return pd.DataFrame(filas, columns=["fecha", "sector", "consumo_total_m3", "n_lecturas"])


def referencia_totales_control(carpeta_equipo):
    t = pd.read_csv(carpeta_equipo / "totales_control.csv")
    return t[["fecha", "sector", "consumo_total_m3", "n_lecturas"]]


def referencia_consolidada_spark(carpeta_lago):
    ruta = carpeta_lago / "consolidada_spark"
    if not ruta.exists() or not list(ruta.glob("*.parquet")):
        return None
    t = pd.read_parquet(str(ruta))
    t["fecha"] = t["fecha"].astype(str)
    return t[["fecha", "sector", "consumo_total_m3", "n_lecturas"]]


def reconciliar_agregado(firma, referencia, tolerancia=TOLERANCIA):
    """Compara el agregado por (fecha, sector) de la capa con una referencia de agregados."""
    if referencia is None:
        return None, "sin referencia"
    calc = agregado_por_dia_sector(firma).set_index(["fecha", "sector"])
    ref = referencia.set_index(["fecha", "sector"])
    if set(calc.index) != set(ref.index):
        return False, f"claves distintas: {len(calc)} calculadas frente a {len(ref)} de referencia"
    ref = ref.loc[calc.index]
    malas_n = int((calc["n_lecturas"] != ref["n_lecturas"]).sum())
    malas_c = int(((calc["consumo_total_m3"] - ref["consumo_total_m3"]).abs() > tolerancia).sum())
    if malas_n or malas_c:
        return False, f"{malas_n} grupos con distinto numero de lecturas y {malas_c} con distinta suma"
    return True, f"{len(ref)} grupos iguales a la referencia (tolerancia {tolerancia})"


def hash_firma(firma):
    """Hash del CONTENIDO de la firma: sumas redondeadas a 6 decimales, marcas como enteros."""
    texto = "".join(
        f"{r.fecha},{r.sector},{r.quality_flag},{r.n},{r.suma_consumo:.6f},{r.suma_presion:.6f},{r.ts_min_us},{r.ts_max_us}\n"
        for r in firma.itertuples())
    return hashlib.sha256(texto.encode("utf-8")).hexdigest()


# ------------------------------------------------------------ interfaz Spark
def _rest(spark, ruta):
    puerto = spark.sparkContext.uiWebUrl.rsplit(":", 1)[-1]
    url = f"http://localhost:{puerto}/api/v1/applications/{spark.sparkContext.applicationId}/{ruta}"
    with urllib.request.urlopen(url) as respuesta:
        return json.load(respuesta)


def marca_interfaz(spark):
    """(siguiente trabajo, siguiente etapa): sirve para medir solo lo que ocurre despues."""
    trabajos = max([j["jobId"] for j in _rest(spark, "jobs")], default=-1) + 1
    etapas = max([e["stageId"] for e in _rest(spark, "stages")], default=-1) + 1
    return trabajos, etapas


def metricas_interfaz(spark, marca):
    """Trabajos, etapas, tareas y MiB leidos desde la marca, de la misma API que alimenta la interfaz."""
    j0, e0 = marca
    trabajos = [j for j in _rest(spark, "jobs") if j["jobId"] >= j0]
    etapas = [e for e in _rest(spark, "stages") if e["stageId"] >= e0 and e["status"] == "COMPLETE"]
    return {
        "trabajos": len(trabajos),
        "etapas": len(etapas),
        "tareas": sum(e["numCompleteTasks"] for e in etapas),
        "mib_leidos": round(sum(e["inputBytes"] for e in etapas) / (1024 * 1024), 2),
    }


# ----------------------------------------------------- comparacion de variantes
def nodo_del_plan(df):
    """Nombre del nodo de evaluacion en Python que muestra explain(), o 'ninguno'."""
    texto = io.StringIO()
    with contextlib.redirect_stdout(texto):
        df.explain()
    plan = texto.getvalue()
    for nodo in ("BatchEvalPython", "ArrowEvalPython"):
        if nodo in plan:
            return nodo
    return "ninguno"


def consulta_clase(base, variante):
    """Consulta que CONSUME la clase: cuenta las lecturas por nivel_presion. Se construye de nuevo en
    cada corrida: con la ejecucion adaptativa activa, volver a ejecutar el MISMO objeto reutiliza la
    mezcla ya materializada y la segunda corrida no hace el trabajo (los tiempos saldrian en cero)."""
    return (base.withColumn("nivel_presion", columna_nivel_presion(variante))
                .groupBy("nivel_presion").count())


def medir_variante(spark, base, variante, corridas=3):
    """Protocolo: 1 corrida de calentamiento (numero 0, se descarta) y `corridas` medidas. La accion
    cronometrada consume la clase: un count() sobre una columna que no se usa haria que el optimizador
    podara la funcion y no se ejecutara."""
    nodo = nodo_del_plan(consulta_clase(base, variante))
    resultados = []
    for numero in range(corridas + 1):
        consulta = consulta_clase(base, variante)
        marca = marca_interfaz(spark)
        t0 = time.perf_counter()
        filas = consulta.collect()
        t = round(time.perf_counter() - t0, 3)
        conteo = {str(r["nivel_presion"]): int(r["count"]) for r in filas}
        resultados.append({"variante": variante, "numero_corrida": numero, "tiempo_s": t,
                           "nodo_plan": nodo, "conteo_por_clase": conteo,
                           **metricas_interfaz(spark, marca)})
    medidas = [r["tiempo_s"] for r in resultados if r["numero_corrida"] > 0]
    resumen = {"mediana_s": round(statistics.median(medidas), 3), "min_s": min(medidas), "max_s": max(medidas)}
    return resultados, resumen


# ------------------------------------------------------------------- bitacora
def registrar_bitacora(ruta_bitacora, registro):
    """Anexa una linea JSON por corrida: la bitacora es un historial y no forma parte de la
    'misma salida' que se compara entre corridas."""
    with open(ruta_bitacora, "a", encoding="utf-8") as f:
        f.write(json.dumps(registro, ensure_ascii=False, default=str) + "\n")


def contar_archivos(ruta):
    return sum(1 for _ in Path(ruta).rglob("*.parquet"))


# --------------------------------------------------------------------- principal
def main(argv=None):
    a = leer_parametros(argv)
    carpeta = carpeta_volumen(a.raiz, a.volumen, a.carpeta)
    salida = Path(a.salida) if a.salida else carpeta / "refinada_spark"
    ruta_bitacora = Path(a.raiz) / a.bitacora
    base_registro = {"marca_tiempo": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                     "equipo": a.equipo, "volumen": a.volumen, "esquema": "declarado", "modo_lectura": "PERMISSIVE",
                     "master": a.master, "driver_memory": a.driver_memory}

    spark = crear_sesion(a.master, a.driver_memory)
    try:
        esquema = definir_esquema()
        crudo = leer_cruda(spark, carpeta / "cruda" / "acueducto", esquema)
        particiones_entrada = crudo.rdd.getNumPartitions()

        if a.comparar_variantes:
            base = columnas_derivadas(crudo)
            for variante in VARIANTES:
                corridas, resumen = medir_variante(spark, base, variante, a.corridas)
                for c in corridas:
                    registrar_bitacora(ruta_bitacora, {**base_registro, "particiones_entrada": particiones_entrada, **c})
                print(f"{variante}: mediana {resumen['mediana_s']} s, min {resumen['min_s']} s, max {resumen['max_s']} s; "
                      f"nodo {corridas[0]['nodo_plan']}; clases {corridas[-1]['conteo_por_clase']}")
            return 0

        marca = marca_interfaz(spark)
        t0 = time.time()
        refinada = transformar(crudo, a.variante)
        if a.desde:
            refinada = refinada.filter(F.col("fecha") >= F.lit(a.desde).cast("date"))
        if a.hasta:
            refinada = refinada.filter(F.col("fecha") <= F.lit(a.hasta).cast("date"))
        calidad = resumen_calidad(refinada)    # primera lectura del CSV: validar antes de escribir
        escribir_capa(refinada, salida, a.sobrescritura)   # segunda lectura: escribir
        tiempo = round(time.time() - t0, 3)
        interfaz = metricas_interfaz(spark, marca)

        firma_nueva = firma_capa_nueva(spark, salida)
        ok_h, det_h, dif_h = comparar_firmas(firma_nueva, firma_heredada(spark, carpeta / "refinada" / "acueducto"))
        control = referencia_consolidada(carpeta) if a.volumen == "base" else referencia_totales_control(carpeta)
        ok_c, det_c = reconciliar_agregado(firma_nueva, control)
        ok_s, det_s = reconciliar_agregado(firma_nueva, referencia_consolidada_spark(carpeta))
        registro = {
            **base_registro,
            "particiones_entrada": particiones_entrada, "variante": a.variante,
            **interfaz, "tiempo_s": tiempo,
            "filas_leidas": calidad["filas"], "filas_no_conformes": calidad["no_conformes"], "nulos_por_columna": calidad["nulos"],
            "filas_escritas": int(firma_nueva["n"].sum()), "archivos_escritos": contar_archivos(salida),
            "sobrescritura": a.sobrescritura, "ventana": [a.desde, a.hasta],
            "hash_salida": hash_firma(firma_nueva),
            "reconciliacion_heredada": "OK" if ok_h else "FALLA", "detalle_heredada": det_h,
            "reconciliacion_control": "OK" if ok_c else "FALLA", "detalle_control": det_c,
            "reconciliacion_consolidada_spark": {None: "SIN_REFERENCIA", True: "OK", False: "FALLA"}[ok_s],
            "detalle_consolidada_spark": det_s,
        }
        registrar_bitacora(ruta_bitacora, registro)
        print(json.dumps(registro, ensure_ascii=False, indent=2, default=str))
        return 0 if (ok_h and ok_c and ok_s is not False) else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
