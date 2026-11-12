"""Planes de ejecucion de Spark como evidencia: consultas, captura, normalizacion y metricas.

Este archivo viene del paquete base con cuatro funciones pendientes: dia_de_a, consulta_a,
consulta_b y consulta_c, que ustedes escriben en el Nivel 2 (las marcan los comentarios que piden
su trabajo). La sesion de Spark, la lectura de la capa y de la referencia, la captura y la
normalizacion de los planes, las metricas, el hash, el calculo independiente y la bitacora ya
funcionan; el resto del texto de este encabezado describe el programa terminado.

Que hace
    Lee la capa refinada_spark/ del lago y ejecuta cinco consultas sobre ella, mas una variante de
    contraste, para estudiar QUE LEYO el motor en cada una y POR QUE. Para cada consulta captura el
    plan de ejecucion (explain en modo "formatted"), lo normaliza para que dos corridas se puedan
    comparar, lee las metricas del escaneo y de las mezclas desde la interfaz de Spark, comprueba que
    el resultado es el mismo entre variantes y contra un calculo independiente, y anexa una linea a
    bitacora_planes.jsonl.

      A          consumo por sector de UN dia, filtrando por las columnas de la ruta (anio, mes, dia)
      A-fecha    el mismo dia filtrado por la columna fecha, que esta DENTRO del archivo
      B          lecturas con presion alta (mas de 3,5 bar) por sector
      C          consumo por estrato de las lecturas confiables, uniendo con la referencia de medidores
      C-forzada  la misma consulta con la difusion desactivada (umbral -1): obliga a mezclar y ordenar
      C-aqe      la misma consulta C con ejecucion adaptativa activa: plan inicial frente a plan final

Por que se normaliza el plan
    El texto de explain() trae identificadores que cambian en cada ejecucion (la parte numerica de
    los atributos, plan_id de cada Exchange, el numero de etapa compilada) y rutas absolutas. Se
    quitan para que el hash del plan sea estable. Se comparan nodos y campos, no el texto literal:
    entre versiones de Spark los nodos y su formato cambian.

Configuracion fija de la sesion (se anota en la bitacora)
    ejecucion adaptativa apagada, spark.sql.shuffle.partitions = 8, umbral de difusion por omision,
    columnas de particion leidas como TEXTO (anio='2024', mes='02', dia='15': la inferencia de tipo
    se desactiva). Solo la consulta C-aqe enciende la ejecucion adaptativa.

Como usarlo (desde la carpeta raiz del paquete, dentro del contenedor del servicio spark)
    python pipeline/planes_s17.py --equipo equipo-3 --volumen base
    python pipeline/planes_s17.py --equipo equipo-3 --volumen ampliacion --corridas 3

Ejercicio didactico y produccion
    Aqui la capa pesa unos MB y casi todo cabe en una sola tarea; por eso la evidencia son archivos,
    filas, mezclas y resultado identico, y no los tiempos. En produccion los planes de las consultas
    caras se guardan y se vigilan como se vigila un contrato; las estadisticas de las tablas permiten
    al optimizador decidir con costos; y los archivos son de cientos de MB, no de 24 KiB.
"""
import argparse
import contextlib
import datetime as dt
import hashlib
import io
import json
import re
import statistics
import sys
import time
import urllib.request
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent))
from refinada_spark import crear_sesion  # noqa: E402  (la sesion del proyecto: UTC y memoria fijadas)

MODO_EXPLAIN = "formatted"
SQL_LIGERO = "sql?details=false&planDescription=false&length=100000"   # sin nodos ni texto de plan; length: por omision la API entrega solo las primeras 20 ejecuciones
CONSULTAS = ["A", "A-fecha", "B", "C", "C-forzada", "C-aqe"]
UMBRAL_PRESION = 3.5
PARTICIONES_MEZCLA = 8
TOLERANCIA = 1e-6
CAMPOS_BITACORA = [
    "equipo", "volumen", "consulta", "corrida", "modo_explain", "hipotesis_texto", "hipotesis_archivos_leidos",
    "hipotesis_estrategia_union", "marca_hipotesis", "marca_medicion", "spark_version", "aqe", "shuffle_partitions",
    "umbral_difusion", "pista", "inferencia_tipos_particion", "partition_filters", "pushed_filters", "read_schema",
    "archivos_leidos", "tamano_archivos_leidos_texto", "tamano_archivos_leidos_b", "filas_salida_escaneo",
    "filas_resultado", "estrategia_union", "n_exchange", "n_sort", "etapas", "tareas", "bytes_mezcla", "derrame_b",
    "hash_resultado", "hash_plan_normalizado", "diferencia_max_independiente", "tiempo_s"]


# ---------------------------------------------------------------- sesion y lectura
def abrir_sesion(master="local[4]", driver_memory="1g", aqe=False, particiones_mezcla=PARTICIONES_MEZCLA):
    """Sesion local con la configuracion fija del analisis de planes."""
    spark = crear_sesion(master, driver_memory)
    configurar(spark, aqe=aqe, particiones_mezcla=particiones_mezcla)
    return spark


def configurar(spark, aqe=False, particiones_mezcla=PARTICIONES_MEZCLA, umbral_difusion=None, inferir_tipos=False):
    """Fija la configuracion que cambia el plan. umbral_difusion=None deja el valor por omision de Spark."""
    spark.conf.set("spark.sql.adaptive.enabled", str(aqe).lower())
    spark.conf.set("spark.sql.shuffle.partitions", str(particiones_mezcla))
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", str(inferir_tipos).lower())
    if umbral_difusion is None:
        spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
    else:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(umbral_difusion))


def umbral_actual(spark):
    """Umbral de difusion en bytes (-1 si esta desactivado)."""
    valor = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    m = re.match(r"^(-?\d+)\s*([a-zA-Z]*)$", valor.strip())
    n, unidad = int(m.group(1)), m.group(2).lower()
    return n * {"": 1, "b": 1, "k": 1024, "kb": 1024, "m": 1024 ** 2, "mb": 1024 ** 2}[unidad] if n > 0 else -1


def leer_capa(spark, ruta, base_path=None):
    """Lee refinada_spark/. La inferencia de tipo de las columnas de particion queda desactivada en
    configurar(): anio, mes y dia se leen como texto con ceros, tal como se escribieron en la ruta."""
    lector = spark.read
    if base_path:
        lector = lector.option("basePath", str(base_path))
    return lector.parquet(str(ruta))


def leer_referencia(spark, carpeta, con_historia=False):
    nombre = "medidores_con_historia.parquet" if con_historia else "medidores.parquet"
    return spark.read.parquet(str(Path(carpeta) / "referencia" / nombre))


# ---------------------------------------------------------------------- consultas
def dia_de_a(totales_control):
    """Dia de la consulta A.

    Encargo (resuelto por equipo-1): totales_control es un DataFrame de pandas con las columnas fecha (texto
    'AAAA-MM-DD'), sector, n_lecturas y consumo_total_m3: una fila por dia y sector, sin filtro de
    calidad. Devuelvan la fecha, como texto, del dia de MAYOR consumo total (la suma de los cuatro
    sectores). Si dos dias empatan, la fecha mas temprana.
    Anoten en su registro de decisiones por que esta regla da un dia distinto a cada equipo y que
    harian si no existiera un archivo de totales de control.

    Decision (equipo-1): cada ampliacion tiene su propio volumen y sector dominante, asi que el dia
    de mayor consumo total cambia de un equipo a otro. Sin totales de control, lo calculariamos con
    un groupBy(fecha) sobre la propia capa (una pasada completa) y lo fijariamos con --dia."""
    por_dia = totales_control.groupby("fecha", as_index=False)["consumo_total_m3"].sum()
    por_dia = por_dia.sort_values(["consumo_total_m3", "fecha"], ascending=[False, True], kind="mergesort")
    return str(por_dia.iloc[0]["fecha"])


def consulta_a(capa, dia):
    """Consumo por sector de UN dia, filtrando por las columnas de la ruta.

    Encargo (resuelto por equipo-1): capa es el DataFrame de refinada_spark/ y dia es un texto 'AAAA-MM-DD'.
      - Filtren por anio, mes y dia. Esas tres columnas se leen como TEXTO con ceros ('2024',
        '02', '15'): el literal debe coincidir en tipo y en ceros con la columna.
      - No usen la columna fecha: esta guardada DENTRO de los archivos y no permite podar.
      - Agrupen por sector y devuelvan dos columnas: n_lecturas (conteo) y consumo_total_m3
        (suma de consumo_m3)."""
    anio, mes, d = dia.split("-")
    return (capa.filter((F.col("anio") == F.lit(anio)) & (F.col("mes") == F.lit(mes)) & (F.col("dia") == F.lit(d)))
                .groupBy("sector").agg(F.count("*").alias("n_lecturas"), F.sum("consumo_m3").alias("consumo_total_m3")))


def consulta_a_fecha(capa, dia):
    """El mismo dia, filtrado por la columna fecha (guardada dentro del archivo)."""
    return (capa.filter(F.col("fecha") == F.lit(dia).cast("date"))
                .groupBy("sector").agg(F.count("*").alias("n_lecturas"), F.sum("consumo_m3").alias("consumo_total_m3")))


def consulta_b(capa, umbral=UMBRAL_PRESION):
    """Lecturas con presion alta por sector.

    Encargo (resuelto por equipo-1): devuelvan, por sector, n_lecturas y consumo_total_m3 de las lecturas cuya
    presion_bar es MAYOR que `umbral` (3,5 bar por omision). La presion es una columna sin
    particion ni orden: anticipen en su hipotesis cuantos archivos se leeran y por que."""
    return (capa.filter(F.col("presion_bar") > F.lit(umbral))
                .groupBy("sector").agg(F.count("*").alias("n_lecturas"), F.sum("consumo_m3").alias("consumo_total_m3")))


def consulta_c(capa, medidores):
    """Consumo por estrato de las lecturas confiables: union con la referencia de medidores.

    Encargo (resuelto por equipo-1): capa trae las lecturas y medidores la referencia (meter_id, sector, estrato,
    fecha_inicio, fecha_fin, es_actual). Se quiere, por estrato, n_lecturas y consumo_total_m3 de
    las lecturas con es_confiable verdadero.
      - Lleven a la union solo las columnas que hacen falta de cada lado.
      - Unan por meter_id. No fijen ninguna pista de union ni cambien el umbral aqui: la
        estrategia la fija la configuracion de la sesion (ver configurar y ejecutar_consulta).
      - Antes de ejecutar, escriban cual esperan que sea la estrategia de union."""
    lecturas = capa.filter(F.col("es_confiable")).select("meter_id", "consumo_m3")
    ref = medidores.select("meter_id", "estrato")
    return (lecturas.join(ref, on="meter_id", how="inner")
                    .groupBy("estrato").agg(F.count("*").alias("n_lecturas"), F.sum("consumo_m3").alias("consumo_total_m3")))


# ------------------------------------------------------------------------- planes
def capturar_plan(df, modo=MODO_EXPLAIN):
    """Texto de explain(). En PySpark explain() IMPRIME; se captura la salida estandar."""
    salida = io.StringIO()
    with contextlib.redirect_stdout(salida):
        df.explain(mode=modo)
    return salida.getvalue()


def normalizar_plan(texto):
    """Quita lo que cambia entre ejecuciones sin cambiar el plan: la parte numerica de los
    atributos (sector#123), plan_id de cada Exchange, el numero de etapa compilada y las rutas."""
    t = re.sub(r"#\d+L?", "", texto)                                   # identificadores de expresion
    t = re.sub(r",?\s*\[?plan_id=\d+\]?", "", t)                       # identificador de la mezcla
    t = re.sub(r"\[codegen id : \d+\]", "[codegen]", t)                # numero de etapa compilada
    t = re.sub(r"(?:file|hdfs|s3a?):/[^\s,\]\)]*", "<ruta>", t)        # rutas absolutas
    t = re.sub(r"[A-Za-z]:[\\/][^\s,\]\)]*", "<ruta>", t)               # rutas de Windows
    return t


def hash_plan(texto_normalizado):
    return hashlib.sha256(texto_normalizado.encode("utf-8")).hexdigest()


def campo_del_plan(plan, nombre):
    """Valor del campo (PartitionFilters, PushedFilters, ReadSchema) del primer Scan parquet."""
    m = re.search(rf"^{nombre}: (.*)$", plan, re.M)
    return m.group(1).strip() if m else ""


def arbol_del_plan(plan):
    """Parte del plan que dibuja el arbol de nodos. Con ejecucion adaptativa el texto trae un plan
    final y un plan inicial: el arbol que se cuenta es el que se ejecuto (el final)."""
    if "== Final Plan ==" in plan:
        return plan.split("== Final Plan ==", 1)[1].split("== Initial Plan ==", 1)[0]
    return plan.split("\n\n", 1)[0]


def nodos_del_plan(plan):
    """Nodos del arbol del plan formatted: Exchange (sin BroadcastExchange), Sort y estrategia de union."""
    nodos = []
    for linea in arbol_del_plan(plan).splitlines():
        m = re.match(r"^[\s:+\-|*]*([A-Za-z]\w*)", linea)
        if m and not linea.lstrip().startswith("=="):
            nodos.append(m.group(1))
    estrategia = next((n for n in nodos if n in ("BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin",
                                                  "BroadcastNestedLoopJoin", "CartesianProduct")), None)
    return {"n_exchange": nodos.count("Exchange"), "n_sort": nodos.count("Sort"), "estrategia_union": estrategia, "nodos": nodos}


# ---------------------------------------------------------------------- metricas
def _rest(spark, ruta):
    puerto = spark.sparkContext.uiWebUrl.rsplit(":", 1)[-1]
    url = f"http://localhost:{puerto}/api/v1/applications/{spark.sparkContext.applicationId}/{ruta}"
    with urllib.request.urlopen(url) as r:
        return json.load(r)


def marca_interfaz(spark):
    """(siguiente trabajo, siguiente etapa, siguiente ejecucion SQL): lo que ocurra despues es lo que se mide."""
    trabajos = max([j["jobId"] for j in _rest(spark, "jobs")], default=-1) + 1
    etapas = max([e["stageId"] for e in _rest(spark, "stages")], default=-1) + 1
    ejecuciones = max([x["id"] for x in _rest(spark, SQL_LIGERO)], default=-1) + 1
    return trabajos, etapas, ejecuciones


def metricas_etapas(spark, marca, espera=10.0):
    """Etapas completadas, tareas, bytes de mezcla y derrame desde la marca. La interfaz se actualiza de
    forma asincrona: se sondea hasta que la ejecucion SQL figure COMPLETED (con un tope de espera)."""
    j0, e0, x0 = marca
    limite = time.time() + espera
    while True:
        ejec = [x for x in _rest(spark, SQL_LIGERO) if x["id"] >= x0]
        if ejec and all(x["status"] == "COMPLETED" for x in ejec) or time.time() > limite:
            break
        time.sleep(0.1)
    etapas = [e for e in _rest(spark, "stages") if e["stageId"] >= e0]
    hechas = [e for e in etapas if e["status"] == "COMPLETE"]
    return {
        "trabajos": len([j for j in _rest(spark, "jobs") if j["jobId"] >= j0]),
        "etapas": len(hechas),
        "etapas_omitidas": len([e for e in etapas if e["status"] == "SKIPPED"]),
        "tareas": sum(e["numCompleteTasks"] for e in hechas),
        "bytes_mezcla": sum(e["shuffleWriteBytes"] for e in hechas),
        "derrame_b": sum(e["diskBytesSpilled"] for e in hechas),
    }


def _nodos_ejecutados(plan):
    """Recorre el plan fisico ejecutado (por py4j), entrando en las etapas de consulta de la ejecucion adaptativa."""
    nombre = plan.getClass().getSimpleName()
    if nombre == "AdaptiveSparkPlanExec":
        yield from _nodos_ejecutados(plan.executedPlan())
        return
    yield plan
    if nombre.endswith("QueryStageExec"):
        yield from _nodos_ejecutados(plan.plan())
        return
    hijos = plan.children()
    for i in range(hijos.length()):
        yield from _nodos_ejecutados(hijos.apply(i))


def metricas_escaneo(df):
    """Metricas EXACTAS (por py4j) del nodo de lectura de la capa ya ejecutado: archivos leidos, bytes de los
    archivos seleccionados, filas de salida del escaneo y carpetas de particion leidas; y, si hay difusion,
    el tamano de la tabla difundida. df debe haberse ejecutado ya con collect() o toPandas()."""
    r = {"archivos_leidos": 0, "tamano_archivos_leidos_b": 0, "filas_salida_escaneo": 0, "carpetas_leidas": 0,
         "difusion_bytes": None}
    for nodo in _nodos_ejecutados(df._jdf.queryExecution().executedPlan()):
        clase = nodo.getClass().getSimpleName()
        m = nodo.metrics()
        if clase == "FileSourceScanExec":
            def valor(clave):
                return int(m.apply(clave).value()) if m.contains(clave) else None
            parcial = {"archivos_leidos": valor("numFiles"), "tamano_archivos_leidos_b": valor("filesSize"),
                       "filas_salida_escaneo": valor("numOutputRows"), "carpetas_leidas": valor("numPartitions")}
            # la referencia de medidores tambien es un escaneo: se informa la lectura de la capa (la de mas archivos)
            if parcial["archivos_leidos"] >= r["archivos_leidos"]:
                r.update(parcial)
        elif clase == "BroadcastExchangeExec":
            r["difusion_bytes"] = int(m.apply("dataSize").value()) if m.contains("dataSize") else None
    return r


def tamanos_estimados(df):
    """(tamano estimado del lado izquierdo, del derecho), en bytes, de la primera union del plan optimizado.
    Es la estimacion con la que Catalyst elige la estrategia: se basa en el tamano de los archivos en disco,
    no en la memoria que ocupara cada lado."""
    pila = [df._jdf.queryExecution().optimizedPlan()]
    while pila:
        nodo = pila.pop()
        if nodo.getClass().getSimpleName() == "Join":
            return int(str(nodo.left().stats().sizeInBytes())), int(str(nodo.right().stats().sizeInBytes()))
        hijos = nodo.children()
        pila += [hijos.apply(i) for i in range(hijos.length())]
    return None, None


# --------------------------------------------------------------------- resultados
def resultado_a_pandas(df):
    return df.toPandas()


def hash_resultado(pdf):
    """Hash del CONTENIDO: filas ordenadas, sumas redondeadas a 6 decimales."""
    cols = list(pdf.columns)
    filas = pdf.sort_values(cols[0]).itertuples(index=False)
    texto = "".join(",".join(f"{v:.6f}" if isinstance(v, float) else str(v) for v in f) + "\n" for f in filas)
    return hashlib.sha256(texto.encode("utf-8")).hexdigest()


def _lectura_pyarrow(capa, columnas, filtro_ruta=None):
    """Lee la capa con pyarrow, sin Spark. filtro_ruta: dia 'AAAA-MM-DD' para leer solo esa carpeta."""
    capa = Path(capa)
    if filtro_ruta:
        a, m, d = filtro_ruta.split("-")
        archivos = sorted((capa / f"anio={a}" / f"mes={m}" / f"dia={d}").glob("*.parquet"))
    else:
        archivos = sorted(capa.rglob("*.parquet"))
    return pd.concat([pq.read_table(str(f), columns=columnas).to_pandas() for f in archivos], ignore_index=True)


def calculo_independiente(consulta, capa, referencia, dia):
    """Resultado de la consulta calculado con pandas y pyarrow, sin Spark, para contrastarlo."""
    if consulta in ("A", "A-fecha"):
        d = _lectura_pyarrow(capa, ["sector", "consumo_m3"], filtro_ruta=dia)
        return d.groupby("sector").agg(n_lecturas=("consumo_m3", "size"), consumo_total_m3=("consumo_m3", "sum")).reset_index()
    if consulta == "B":
        d = _lectura_pyarrow(capa, ["sector", "consumo_m3", "presion_bar"])
        d = d[d["presion_bar"] > UMBRAL_PRESION]
        return d.groupby("sector").agg(n_lecturas=("consumo_m3", "size"), consumo_total_m3=("consumo_m3", "sum")).reset_index()
    d = _lectura_pyarrow(capa, ["meter_id", "consumo_m3", "es_confiable"])
    d = d[d["es_confiable"]].merge(pd.read_parquet(Path(referencia) / "referencia" / "medidores.parquet")[["meter_id", "estrato"]], on="meter_id")
    return d.groupby("estrato").agg(n_lecturas=("consumo_m3", "size"), consumo_total_m3=("consumo_m3", "sum")).reset_index()


def diferencia_maxima(a, b):
    """Diferencia maxima entre dos resultados (claves iguales; conteos exactos y sumas con tolerancia)."""
    clave = a.columns[0]
    m = a.merge(b, on=clave, how="outer", suffixes=("_a", "_b"), indicator=True)
    if (m["_merge"] != "both").any():
        return float("inf")
    return float(max((m["n_lecturas_a"] - m["n_lecturas_b"]).abs().max(),
                     (m["consumo_total_m3_a"] - m["consumo_total_m3_b"]).abs().max()))


# --------------------------------------------------------------------- hipotesis
def registrar_hipotesis(ruta, hipotesis):
    """Escribe las hipotesis con la marca de tiempo del momento: DEBEN existir antes de medir."""
    for h in hipotesis.values():
        h["marca_hipotesis"] = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    Path(ruta).write_text(json.dumps(hipotesis, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------------------------------------------- una consulta, una corrida
def preparar_datos(spark, lago, dia=None):
    """Lee la capa y la referencia UNA sola vez por sesion: listar 120 carpetas y leer el esquema cuesta
    segundos, y no hace falta repetirlo para cada consulta (la consulta si se construye de nuevo cada vez)."""
    lago = Path(lago)
    return {"capa": lago / "refinada_spark", "lago": lago, "dia": dia or dia_por_omision(lago),
            "capa_df": leer_capa(spark, lago / "refinada_spark"), "referencia_df": leer_referencia(spark, lago)}


def medir_df(spark, df):
    """Ejecuta un DataFrame y devuelve (registro, plan, plan_inicial, resultado). El registro trae lo que se
    observa: filtros del plan, archivos, filas de salida del escaneo, mezclas, etapas, tareas y hashes.
    El plan que se informa es el que se ejecuto: con ejecucion adaptativa activa, el final."""
    adaptativa = spark.conf.get("spark.sql.adaptive.enabled") == "true"
    plan_inicial = normalizar_plan(capturar_plan(df))
    marca = marca_interfaz(spark)
    t0 = time.perf_counter()
    pdf = df.toPandas()
    tiempo = round(time.perf_counter() - t0, 3)
    marca_medicion = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    plan_final = normalizar_plan(capturar_plan(df))
    plan = plan_final if adaptativa else plan_inicial
    nodos = nodos_del_plan(plan)
    registro = {
        "marca_medicion": marca_medicion, "spark_version": spark.version, "aqe": adaptativa,
        "shuffle_partitions": int(spark.conf.get("spark.sql.shuffle.partitions")),
        "umbral_difusion": umbral_actual(spark), "pista": None,
        "inferencia_tipos_particion": spark.conf.get("spark.sql.sources.partitionColumnTypeInference.enabled") == "true",
        "partition_filters": campo_del_plan(plan, "PartitionFilters"), "pushed_filters": campo_del_plan(plan, "PushedFilters"),
        "read_schema": campo_del_plan(plan, "ReadSchema"),
        **metricas_escaneo(df), "filas_resultado": int(len(pdf)),
        "estrategia_union": nodos["estrategia_union"], "n_exchange": nodos["n_exchange"], "n_sort": nodos["n_sort"],
        **metricas_etapas(spark, marca),
        "hash_resultado": hash_resultado(pdf), "hash_plan_normalizado": hash_plan(plan), "tiempo_s": tiempo}
    if adaptativa:
        registro["hash_plan_inicial"] = hash_plan(plan_inicial)
        registro["hash_plan_final"] = hash_plan(plan_final)
    registro["tamano_archivos_leidos_texto"] = f"{registro['tamano_archivos_leidos_b'] / 1024:.1f} KiB"
    return registro, plan, plan_inicial, pdf


def _constructor(nombre, constructores):
    """Funcion (capa, medidores, dia) que construye la consulta `nombre`: la que se pasa en `constructores`
    o, por omision, la del modulo (las funciones consulta_a, consulta_b y consulta_c)."""
    if constructores and nombre in constructores:
        return constructores[nombre]
    por_omision = {"A": lambda c, m, d: consulta_a(c, d), "A-fecha": lambda c, m, d: consulta_a_fecha(c, d),
                   "B": lambda c, m, d: consulta_b(c), "C": lambda c, m, d: consulta_c(c, m),
                   "C-forzada": lambda c, m, d: consulta_c(c, m), "C-aqe": lambda c, m, d: consulta_c(c, m)}
    return por_omision[nombre]


def ejecutar_consulta(spark, nombre, datos, hipotesis, corrida, equipo, volumen, constructores=None):
    """Ejecuta UNA consulta con la configuracion que le corresponde (C-forzada: umbral -1; C-aqe: ejecucion
    adaptativa activa) y devuelve (linea de bitacora, plan normalizado, resultado). datos viene de
    preparar_datos(); constructores es opcional: {nombre: funcion(capa, medidores, dia)}."""
    configurar(spark, aqe=(nombre == "C-aqe"), umbral_difusion=-1 if nombre == "C-forzada" else None)
    capa = datos["capa_df"] if "capa_df" in datos else leer_capa(spark, datos["capa"])
    medidores = datos["referencia_df"] if "referencia_df" in datos else leer_referencia(spark, datos["lago"])
    df = _constructor(nombre, constructores)(capa, medidores, datos["dia"])
    medicion, plan, _, pdf = medir_df(spark, df)
    h = hipotesis.get(nombre, {})
    registro = {
        "equipo": equipo, "volumen": volumen, "consulta": nombre, "corrida": corrida, "modo_explain": MODO_EXPLAIN,
        "hipotesis_texto": h.get("texto"), "hipotesis_archivos_leidos": h.get("archivos_leidos"),
        "hipotesis_estrategia_union": h.get("estrategia_union"), "marca_hipotesis": h.get("marca_hipotesis"),
        **medicion}
    return registro, plan, pdf


def hash_arbol(raiz_lago, excluir=("refinada_spark", "consolidada_spark")):
    """Hash del arbol de archivos del lago (nombre relativo y tamano), sin las capas que se reconstruyen."""
    h = hashlib.sha256()
    raiz = Path(raiz_lago)
    for f in sorted(raiz.rglob("*")):
        rel = f.relative_to(raiz)
        if f.is_file() and rel.parts[0] not in excluir and not f.name.endswith(".crc"):
            h.update(f"{rel.as_posix()}|{f.stat().st_size}\n".encode("utf-8"))
    return h.hexdigest()


# --------------------------------------------------- lectura independiente de Parquet
def estadisticas_parquet(capa, columnas=("fecha", "presion_bar")):
    """Minimo y maximo de cada columna en cada archivo (pie del archivo Parquet, sin Spark) y bytes
    comprimidos por columna de toda la capa. Explica por que un filtro empujado salta o no salta grupos."""
    filas, bytes_col = [], {}
    for f in sorted(Path(capa).rglob("*.parquet")):
        md = pq.ParquetFile(f).metadata
        fila = {"archivo": f.parent.name, "grupos_de_filas": md.num_row_groups, "filas": md.num_rows}
        for g in range(md.num_row_groups):
            rg = md.row_group(g)
            for c in range(rg.num_columns):
                cc = rg.column(c)
                bytes_col[cc.path_in_schema] = bytes_col.get(cc.path_in_schema, 0) + cc.total_compressed_size
                if cc.path_in_schema in columnas and cc.statistics is not None:
                    fila[cc.path_in_schema + "_min"], fila[cc.path_in_schema + "_max"] = cc.statistics.min, cc.statistics.max
        filas.append(fila)
    total = sum(bytes_col.values())
    por_columna = pd.DataFrame({"columna": list(bytes_col), "bytes_comprimidos": list(bytes_col.values())})
    por_columna["fraccion"] = (por_columna["bytes_comprimidos"] / total).round(4)
    return pd.DataFrame(filas), por_columna.sort_values("bytes_comprimidos", ascending=False).reset_index(drop=True)


# ---------------------------------------------------------- reparto de tareas (union sesgada)
def consulta_sesgo(capa, sectores, estrategia="catalyst", sal=8):
    """Consumo por estrato uniendo lecturas y sectores por la clave sector. estrategia: catalyst (lo que
    decida el optimizador), difundir (pista BROADCAST) o sal (sufijo aleatorio en la clave)."""
    lect = capa.select("sector", "consumo_m3")
    if estrategia == "difundir":
        union = lect.join(F.broadcast(sectores), "sector")
    elif estrategia == "sal":
        lect_s = lect.withColumn("sal", (F.rand(42) * sal).cast("int"))
        sec_s = sectores.crossJoin(capa.sparkSession.range(sal).select(F.col("id").cast("int").alias("sal")))
        union = lect_s.join(sec_s, ["sector", "sal"])
    else:
        union = lect.join(sectores, "sector")
    return union.groupBy("estrato").agg(F.count("*").alias("n_lecturas"), F.sum("consumo_m3").alias("consumo_total_m3"))


def distribucion_de_tareas(spark, marca):
    """Por tarea de la etapa que lee la mezcla de la union (la de 8 tareas con mas bytes leidos de mezcla):
    bytes y registros leidos. Devuelve None si no hubo mezcla (por ejemplo, con difusion)."""
    _, e0, _ = marca
    mejor = None
    for e in _rest(spark, "stages"):
        if e["stageId"] < e0 or e["status"] != "COMPLETE" or e["numTasks"] != PARTICIONES_MEZCLA:
            continue
        tl = _rest(spark, f"stages/{e['stageId']}/{e['attemptId']}/taskList?length=100")
        t = sorted([(x["index"], x["taskMetrics"]["shuffleReadMetrics"]["remoteBytesRead"] + x["taskMetrics"]["shuffleReadMetrics"]["localBytesRead"],
                     x["taskMetrics"]["shuffleReadMetrics"]["recordsRead"]) for x in tl if x.get("taskMetrics")])
        total = sum(x[1] for x in t)
        if mejor is None or total > mejor[0]:
            mejor = (total, e["stageId"], t)
    if mejor is None:
        return None
    bytes_t = [x[1] for x in mejor[2]]
    con_datos = [b for b in bytes_t if b > 0]
    return {"etapa": mejor[1], "bytes_por_tarea": bytes_t, "registros_por_tarea": [x[2] for x in mejor[2]],
            "tareas_con_datos": len(con_datos), "max_bytes": max(bytes_t), "fraccion_de_la_mas_pesada": round(max(bytes_t) / sum(bytes_t), 4),
            "fraccion_filas_pesada": round(max(x[2] for x in mejor[2]) / sum(x[2] for x in mejor[2]), 4),
            "mediana_con_datos": statistics.median(con_datos) if con_datos else 0}


def particion_de_mezcla(spark, claves, particiones=PARTICIONES_MEZCLA):
    """Particion de mezcla (de 0 a particiones-1) que Spark asigna a cada clave de texto."""
    d = spark.createDataFrame([(k,) for k in claves], ["clave"]).select("clave", F.expr(f"pmod(hash(clave), {particiones})").alias("particion"))
    return {r["clave"]: int(r["particion"]) for r in d.collect()}


# --------------------------------------------------------------- las consultas completas
def ejecutar_todo(spark, datos, hipotesis, equipo, volumen, corridas, ruta_bitacora, carpeta_planes):
    """Las seis consultas: 1 corrida de calentamiento (se descarta) y `corridas` medidas por consulta, con
    calculo independiente, bitacora y plan normalizado. Devuelve la ultima linea de bitacora de cada consulta."""
    hash_antes = hash_arbol(datos["lago"])
    carpeta_planes = Path(carpeta_planes)
    carpeta_planes.mkdir(parents=True, exist_ok=True)
    ultimas = {}
    for nombre in CONSULTAS:
        ejecutar_consulta(spark, nombre, datos, hipotesis, 0, equipo, volumen)
        for corrida in range(1, corridas + 1):
            reg, plan, pdf = ejecutar_consulta(spark, nombre, datos, hipotesis, corrida, equipo, volumen)
            reg["diferencia_max_independiente"] = diferencia_maxima(pdf, calculo_independiente(nombre, datos["capa"], datos["lago"], datos["dia"]))
            if nombre == "A":
                reg["hash_datalake_antes"], reg["hash_datalake_despues"] = hash_antes, hash_arbol(datos["lago"])
            with open(ruta_bitacora, "a", encoding="utf-8") as f:
                f.write(json.dumps(reg, ensure_ascii=False, default=str) + "\n")
            if corrida == 1:
                (carpeta_planes / f"plan_{volumen}_{nombre}.txt").write_text(plan, encoding="utf-8")
            ultimas[nombre] = reg
    return ultimas


# ------------------------------------------------------------------------ principal
def leer_parametros(argv=None):
    ap = argparse.ArgumentParser(description="Planes de ejecucion de las consultas A, B y C sobre refinada_spark/.")
    ap.add_argument("--equipo", default="sin-equipo")
    ap.add_argument("--volumen", choices=["base", "ampliacion"], default="base")
    ap.add_argument("--carpeta", default=None, help="Carpeta del lago (por omision datalake/ o datalake_equipo/)")
    ap.add_argument("--raiz", default=".")
    ap.add_argument("--corridas", type=int, default=3, help="Corridas medidas por consulta (mas 1 de calentamiento)")
    ap.add_argument("--dia", default=None, help="Dia de la consulta A (por omision, el de mayor consumo)")
    ap.add_argument("--hipotesis", default=None, help="Archivo JSON con las hipotesis (escrito antes de medir)")
    ap.add_argument("--planes", default="planes", help="Carpeta de los planes normalizados")
    ap.add_argument("--bitacora", default="bitacora_planes.jsonl")
    ap.add_argument("--master", default="local[4]")
    ap.add_argument("--driver-memory", default="1g")
    return ap.parse_args(argv)


def dia_por_omision(lago):
    """Dia de A: el de mayor consumo si hay totales_control.csv (ampliacion); el 2024-02-15 en el lago base."""
    if (Path(lago) / "totales_control.csv").exists():
        return dia_de_a(pd.read_csv(Path(lago) / "totales_control.csv"))
    return "2024-02-15"


def main(argv=None):
    a = leer_parametros(argv)
    raiz = Path(a.raiz)
    lago = Path(a.carpeta) if a.carpeta else raiz / ("datalake" if a.volumen == "base" else "datalake_equipo")
    dia = a.dia or dia_por_omision(lago)
    hipotesis = json.loads(Path(a.hipotesis).read_text(encoding="utf-8")) if a.hipotesis else {}
    spark = abrir_sesion(a.master, a.driver_memory)
    datos = preparar_datos(spark, lago, dia)
    try:
        ultimas = ejecutar_todo(spark, datos, hipotesis, a.equipo, a.volumen, a.corridas, raiz / a.bitacora, raiz / a.planes)
        for nombre, reg in ultimas.items():
            print(f"{nombre}: archivos {reg['archivos_leidos']}, filas del escaneo {reg['filas_salida_escaneo']}, "
                  f"union {reg['estrategia_union']}, exchange {reg['n_exchange']}, etapas {reg['etapas']}, tareas {reg['tareas']}")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
