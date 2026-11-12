"""Trabajo heredado del tablero financiero: extrae la cruda, transforma fuera y carga solo el resultado.

Que hace
    Es el trabajo nocturno que alimenta hoy el tablero del cierre diario. Se escribio antes de que
    existiera la capa de preparacion (refinada_spark/): lee los CSV de la capa cruda, DESCARTA lo que
    la regla de confiabilidad considera dudoso, agrega y guarda solo el resultado en tablero_etl/.
    No deja rastro de lo descartado y no verifica ningun contrato.

      consumo   lecturas confiables y consumo total por (fecha, sector)             480 filas en la base
      presion   lecturas confiables por (fecha, sector, nivel_presion)

    Es una linea base para refactorizar: hace lo que hacia, con el mismo motor (Spark) y los mismos
    datos que el resto del proyecto. Tiene cuatro modos para medir con justicia que cuesta transformar
    antes de guardar:

      ETL              un trabajo por producto: cada ejecucion relee TODA la cruda
      ETL-una-pasada   --producto ambos: lee la cruda UNA vez y deriva de ella los dos productos
      ETL-ventana      --modo ventana: lee solo las carpetas de unos dias y reemplaza solo esos dias
      ETL-parquet      --formato parquet: el mismo trabajo, leyendo refinada_spark/ en lugar de la cruda

Reglas duplicadas
    La clasificacion de presion usa su PROPIA copia de los umbrales (2,5 y 3,5 bar), escrita aqui, y
    la regla de confiabilidad (quality_flag = 'OK') tambien. La capa de preparacion tiene la suya.
    Si una cambia y la otra no, las cifras divergen sin que nada falle: es un riesgo de este diseno.

Como usarlo (desde la carpeta raiz del paquete, dentro del contenedor del servicio spark)
    python pipeline/etl_tablero.py --producto consumo
    python pipeline/etl_tablero.py --producto presion
    python pipeline/etl_tablero.py --producto ambos
    python pipeline/etl_tablero.py --producto consumo --modo ventana --desde 2024-02-10 --hasta 2024-02-15
    python pipeline/etl_tablero.py --producto consumo --formato parquet
    python pipeline/etl_tablero.py --volumen ampliacion --equipo equipo-3 --producto ambos

Ejercicio didactico y produccion
    Aqui la cruda pesa unos 30 MB y cada ejecucion tarda segundos. En produccion el costo de releer la
    fuente se mide en bytes escaneados o en horas de cluster, y una fuente que no debe aterrizar en el
    almacen (por ejemplo, con datos personales) puede obligar a transformar antes de cargar.
"""
import argparse
import datetime as dt
import json
import sys
from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent))
from refinada_spark import carpeta_volumen, crear_sesion, definir_esquema  # noqa: E402
import metricas_capas as M  # noqa: E402

UMBRAL_BAJA = 2.5      # bar: copia propia de este trabajo
UMBRAL_ALTA = 3.5      # bar: copia propia de este trabajo
PRODUCTOS = ["consumo", "presion"]
CLAVES = {"consumo": ["fecha", "sector"], "presion": ["fecha", "sector", "nivel_presion"]}
COLUMNAS_SALIDA = {"consumo": ["fecha", "sector", "n_lecturas", "consumo_total_m3"],
                   "presion": ["fecha", "sector", "nivel_presion", "n_lecturas"]}


# ---------------------------------------------------------------- parametros
def leer_parametros(argv=None):
    ap = argparse.ArgumentParser(description="Trabajo heredado del tablero: transforma antes de guardar.")
    ap.add_argument("--equipo", default="sin-equipo")
    ap.add_argument("--volumen", choices=["base", "ampliacion"], default="base")
    ap.add_argument("--carpeta", default=None, help="Carpeta del lago a leer (por omision datalake/ o datalake_equipo/)")
    ap.add_argument("--raiz", default=".")
    ap.add_argument("--producto", choices=PRODUCTOS + ["ambos"], default="consumo")
    ap.add_argument("--modo", choices=["completo", "ventana"], default="completo")
    ap.add_argument("--desde", default=None, help="Primer dia AAAA-MM-DD (modo ventana)")
    ap.add_argument("--hasta", default=None, help="Ultimo dia AAAA-MM-DD (modo ventana)")
    ap.add_argument("--formato", choices=["csv", "parquet"], default="csv", help="csv: capa cruda · parquet: refinada_spark/")
    ap.add_argument("--estados-confiables", default="OK", help="Regla de confiabilidad: estados de quality_flag que cuentan como confiables, separados por coma")
    ap.add_argument("--salida", default=None, help="Carpeta del resultado (por omision <lago>/tablero_etl)")
    ap.add_argument("--bitacora", default="bitacora_capas.jsonl")
    ap.add_argument("--master", default="local[4]")
    ap.add_argument("--driver-memory", default="1g")
    return ap.parse_args(argv)


def configurar(spark):
    """Misma configuracion que la ruta ELT, para que la comparacion use el mismo motor y los mismos ajustes."""
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


# -------------------------------------------------------------------- lectura
def carpetas_de_dias(capa, desde=None, hasta=None):
    """Carpetas anio=AAAA/mes=MM/dia=DD de una capa, limitadas a la ventana [desde, hasta] por su RUTA.
    Filtrar por la ruta evita abrir los demas archivos; filtrar por la columna fecha los abre todos."""
    salida = []
    for c in sorted(Path(capa).glob("anio=*/mes=*/dia=*")):
        dia = f"{c.parent.parent.name[5:]}-{c.parent.name[4:]}-{c.name[4:]}"
        if (desde is None or dia >= desde) and (hasta is None or dia <= hasta):
            salida.append(c)
    return salida


def leer_fuente(spark, lago, formato, carpetas=None):
    """CSV de la cruda con esquema declarado (modo permisivo, encabezado verificado) o Parquet de
    refinada_spark/. Con `carpetas` solo se leen esas carpetas de dia."""
    lago = Path(lago)
    if formato == "csv":
        origen = [str(c) for c in carpetas] if carpetas else [str(lago / "cruda" / "acueducto")]
        df = (spark.read.csv(origen, header=True, schema=definir_esquema(), mode="PERMISSIVE",
                             timestampFormat="yyyy-MM-dd HH:mm:ss", enforceSchema=False))
        return df.drop("anio", "mes", "dia").withColumn("fecha", F.to_date("timestamp"))
    capa = lago / "refinada_spark"
    lector = spark.read.option("basePath", str(capa))
    df = lector.parquet(*[str(c) for c in carpetas]) if carpetas else lector.parquet(str(capa))
    return df.select("fecha", "sector", "consumo_m3", "presion_bar", "quality_flag")


# -------------------------------------------------------------- transformacion
def nivel_presion(presion):
    return (F.when(presion < UMBRAL_BAJA, "baja").when(presion > UMBRAL_ALTA, "alta")
             .when(presion.isNotNull(), "normal").otherwise("sin_dato"))


def filtro_confiables(estados):
    """Regla de confiabilidad de este trabajo: una lectura es confiable si su bandera esta entre los estados indicados."""
    return F.col("quality_flag") == estados[0] if len(estados) == 1 else F.col("quality_flag").isin(*estados)


def agregar(df, producto, estados=("OK",)):
    """Filtra lo confiable (se DESCARTA el resto) y agrega."""
    confiables = df.filter(filtro_confiables(estados))
    if producto == "consumo":
        return confiables.groupBy("fecha", "sector").agg(F.count("*").alias("n_lecturas"), F.sum("consumo_m3").alias("consumo_total_m3"))
    return (confiables.withColumn("nivel_presion", nivel_presion(F.col("presion_bar")))
                      .groupBy("fecha", "sector", "nivel_presion").agg(F.count("*").alias("n_lecturas")))


def agregar_una_pasada(df, estados=("OK",)):
    """Un solo agregado al grano mas fino (fecha, sector, nivel_presion); de el salen los dos productos."""
    confiables = df.filter(filtro_confiables(estados)).withColumn("nivel_presion", nivel_presion(F.col("presion_bar")))
    fino = (confiables.groupBy("fecha", "sector", "nivel_presion")
                      .agg(F.count("*").alias("n_lecturas"), F.sum("consumo_m3").alias("consumo_total_m3")).cache())
    consumo = fino.groupBy("fecha", "sector").agg(F.sum("n_lecturas").alias("n_lecturas"), F.sum("consumo_total_m3").alias("consumo_total_m3"))
    presion = fino.select("fecha", "sector", "nivel_presion", "n_lecturas")
    return fino, {"consumo": consumo, "presion": presion}


# ------------------------------------------------------------------ escritura
def escribir(resultado, salida, producto, modo):
    """modo completo: un Parquet de salida, reemplazo total · modo ventana: Parquet particionado por dia,
    con sobrescritura dinamica (solo se reemplazan los dias que se escriben)."""
    if modo == "completo":
        ruta = Path(salida) / producto
        resultado.coalesce(1).write.mode("overwrite").parquet(str(ruta))
    else:
        ruta = Path(salida) / f"{producto}_part"
        # repartition por las columnas de la ruta: todas las filas de un dia caen en la misma tarea y el dia queda en UN archivo
        (resultado.withColumn("anio", F.date_format("fecha", "yyyy")).withColumn("mes", F.date_format("fecha", "MM"))
                  .withColumn("dia", F.date_format("fecha", "dd")).repartition("anio", "mes", "dia")
                  .write.mode("overwrite").partitionBy("anio", "mes", "dia").parquet(str(ruta)))
    return ruta


def releer_resultado(ruta, producto, modo, dias=None):
    """Lee lo escrito (480 filas en la base) para calcular el hash del resultado ALMACENADO."""
    if modo == "completo":
        pdf = pd.read_parquet(str(ruta))
    else:
        partes = [pd.read_parquet(str(c)) for c in carpetas_de_dias(ruta, dias[0], dias[1])] if dias else [pd.read_parquet(str(c)) for c in carpetas_de_dias(ruta)]
        pdf = pd.concat(partes, ignore_index=True)
    pdf = pdf[COLUMNAS_SALIDA[producto]].copy()
    pdf["fecha"] = pdf["fecha"].astype(str)
    return pdf


def registrar_bitacora(ruta, registro):
    with open(ruta, "a", encoding="utf-8") as f:
        f.write(json.dumps(registro, ensure_ascii=False, default=str) + "\n")


# --------------------------------------------------------------------- principal
def ejecutar(spark, a):
    """Ejecuta el trabajo con una sesion de Spark YA ABIERTA (por ejemplo la de un cuaderno) y devuelve la linea de bitacora.
    Usarla en lugar de main evita abrir una segunda sesion: cada JVM de Spark ocupa cientos de MiB."""
    lago = carpeta_volumen(a.raiz, a.volumen, a.carpeta)
    salida = Path(a.salida) if a.salida else lago / "tablero_etl"
    productos = PRODUCTOS if a.producto == "ambos" else [a.producto]
    una_pasada = a.producto == "ambos"
    estados = tuple(e.strip() for e in a.estados_confiables.split(","))
    if una_pasada:
        enfoque = "ETL-una-pasada"
    elif a.modo == "ventana":
        enfoque = "ETL-ventana"
    elif a.formato == "parquet":
        enfoque = "ETL-parquet"
    else:
        enfoque = "ETL"
    fuente_dir = lago / ("cruda/acueducto" if a.formato == "csv" else "refinada_spark")
    carpetas = carpetas_de_dias(fuente_dir, a.desde, a.hasta) if a.modo == "ventana" else None
    patron = "lecturas.csv" if a.formato == "csv" else "*.parquet"
    if carpetas is None:
        archivos_fuente = M.archivos_de(fuente_dir, patron)
    else:
        archivos_fuente = [f for c in carpetas for f in sorted(c.glob(patron))]

    configurar(spark)
    df = leer_fuente(spark, lago, a.formato, carpetas)
    desde = M.marca(spark)
    t0 = dt.datetime.now(dt.timezone.utc)
    fino = None
    if una_pasada:
        fino, resultados = agregar_una_pasada(df, estados)
    else:
        resultados = {productos[0]: agregar(df, productos[0], estados)}
    planes = {p: M.capturar_plan(r) for p, r in resultados.items()}
    rutas = {p: escribir(r, salida, p, a.modo) for p, r in resultados.items()}
    if fino is not None:
        fino.unpersist()
    tiempo = round((dt.datetime.now(dt.timezone.utc) - t0).total_seconds(), 3)
    met = M.medir_desde(spark, desde)

    dias = (a.desde, a.hasta) if a.modo == "ventana" else None
    hashes, filas, bytes_escritos, archivos_escritos = {}, {}, 0, 0
    for p in productos:
        pdf = releer_resultado(rutas[p], p, a.modo, dias)
        hashes[p] = M.hash_resultado_ordenado(pdf, CLAVES[p])
        filas[p] = int(len(pdf))
        if a.modo == "completo":
            escritos = M.archivos_de(rutas[p], "*.parquet")
        else:
            escritos = [f for c in carpetas_de_dias(rutas[p], a.desde, a.hasta) for f in sorted(c.glob("*.parquet"))]
        bytes_escritos += M.tamano_de(escritos)
        archivos_escritos += len(escritos)
    registro = {
        "marca_medicion": t0.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), "equipo": a.equipo, "volumen": a.volumen,
        "capa": "tablero_etl", "accion": "construir", "producto": a.producto, "enfoque": enfoque, "modo": a.modo, "formato": a.formato, "estados_confiables": list(estados),
        "spark_version": spark.version, "aqe": False, "shuffle_partitions": 8,
        "ventana": [a.desde, a.hasta] if a.modo == "ventana" else None,
        "archivos_leidos": met["archivos_leidos"], "archivos_fuente": len(archivos_fuente),
        "bytes_archivos_seleccionados": M.tamano_de(archivos_fuente),
        "bytes_entrada_etapas": met["bytes_entrada_etapas"], "metrica_bytes": M.METRICA_BYTES,
        "bytes_leidos": M.bytes_leidos(a.formato, archivos_fuente, "\n".join(planes.values())),
        "columnas_leidas": sorted({c for t in planes.values() for c in M.columnas_leidas(t)}),
        "filas_salida_escaneo": met["filas_salida_escaneo"], "escaneos": met["escaneos"],
        "filas_resultado": filas, "bytes_escritos": bytes_escritos, "archivos_escritos": archivos_escritos,
        "etapas": met["etapas"], "tareas": met["tareas"], "bytes_mezcla": met["bytes_mezcla"], "derrame_b": met["derrame_b"],
        "hash_resultado": hashes if una_pasada else hashes[productos[0]],
        "tiempo_s": tiempo,
    }
    registrar_bitacora(Path(a.raiz) / a.bitacora, registro)
    return registro


def main(argv=None):
    a = leer_parametros(argv)
    spark = crear_sesion(a.master, a.driver_memory)
    try:
        print(json.dumps(ejecutar(spark, a), ensure_ascii=False, indent=2, default=str))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
