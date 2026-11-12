"""Ejecutor de capas: transformaciones en SQL sobre la preparacion, contratos por frontera y reproceso.

Que hace
    Es la ruta "cargar primero, transformar adentro" del proyecto. La capa de preparacion
    (refinada_spark/) ya conserva todas las lecturas con su bandera de calidad; este modulo:

      1. abre una sesion con la configuracion fija del analisis (ver abrir_sesion);
      2. registra la vista temporal prep_lecturas sobre la preparacion, limitada a una ventana de dias
         por las columnas de la RUTA (anio, mes, dia), de modo que el SQL no sabe nada de la ventana;
      3. ejecuta los archivos sql/consolidada/*.sql y escribe consolidada_elt/ particionada por dia,
         con sobrescritura dinamica: reprocesar unos dias reemplaza solo esos dias;
      4. verifica los CONTRATOS de cada frontera (esquema, grano, invariantes de reconciliacion) con
         pandas y pyarrow, sin abrir Spark;
      5. reprocesa una ventana sobre una COPIA de la capa de salida y comprueba que las demas
         particiones quedaron intactas y que dos corridas dan el mismo resultado;
      6. anexa una linea a bitacora_capas.jsonl por ejecucion.

Contrato entre capas
    Un contrato es un acuerdo verificable entre quien produce una capa y quien la consume. Este modulo
    verifica contratos ESTRUCTURALES: columnas, grano con clave unica e invariantes de reconciliacion
    (conteos y sumas que deben coincidir entre capas). Rangos, completitud y acciones ante el
    incumplimiento son contratos de calidad, que van mas adelante.

Como usarlo (desde la carpeta raiz del paquete, dentro del contenedor del servicio spark)
    python pipeline/elt_s18.py construir
    python pipeline/elt_s18.py construir --volumen ampliacion --equipo equipo-3
    python pipeline/elt_s18.py contratos
    python pipeline/elt_s18.py reprocesar --desde 2024-02-10 --hasta 2024-02-15 --sobrescritura dinamica

Ejercicio didactico y produccion
    Aqui el almacen es el propio lago consultado con Spark SQL y los contratos son archivos JSON que
    interpreta una funcion. En produccion el almacen es un sistema con esquemas, permisos y duenos; los
    contratos son artefactos versionados con pruebas automaticas que bloquean la publicacion, y el
    reproceso lo lanza un orquestador por ventana.
"""
import argparse
import datetime as dt
import hashlib
import json
import re
import shutil
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
import planes_s17 as P  # noqa: E402
import metricas_capas as M  # noqa: E402
from refinada_spark import carpeta_volumen, comparar_arboles, resumen_arbol  # noqa: E402

TOLERANCIA = 1e-6
PRODUCTOS = {"consumo": ("consumo_dia_sector", ["fecha", "sector"]),
             "presion": ("presion_dia_sector", ["fecha", "sector", "nivel_presion"])}


class ContratoIncumplido(Exception):
    """Un invariante de un contrato no se cumple en la frontera indicada."""

    def __init__(self, frontera, invariante, detalle=""):
        super().__init__(f"Contrato incumplido en la frontera '{frontera}', invariante '{invariante}'. {detalle}")
        self.frontera, self.invariante = frontera, invariante


def ahora():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


# ---------------------------------------------------------------------- sesion
def abrir_sesion(master="local[4]", driver_memory="1g"):
    """Sesion local con la configuracion fija: UTC, ejecucion adaptativa apagada, 8 particiones de mezcla,
    columnas de ruta como TEXTO, marca temporal en microsegundos y sobrescritura dinamica de particiones."""
    spark = P.abrir_sesion(master, driver_memory)
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def registrar_prep(spark, lago, ventana=None, nombre="prep_lecturas"):
    """Vista temporal sobre refinada_spark/. Si hay ventana (desde, hasta) como 'AAAA-MM-DD', se limita por
    las columnas de la ruta: concat(anio, mes, dia) poda carpetas, mientras que filtrar por fecha abre todo."""
    capa = Path(lago) / "refinada_spark"
    df = spark.read.option("basePath", str(capa)).parquet(str(capa))
    if ventana and ventana[0]:
        d, h = ventana[0].replace("-", ""), ventana[1].replace("-", "")
        df = df.filter(f"concat(anio, mes, dia) BETWEEN '{d}' AND '{h}'")
    df.createOrReplaceTempView(nombre)
    return df


def ventana_de_equipo(totales_control, dias=6):
    """Ventana de reproceso del equipo: los `dias` dias que terminan en el dia de mayor consumo del equipo
    (P.dia_de_a, el mismo dia de la consulta A de la lectura de planes), recortados al primer dia de la historia
    (el menor valor de la columna fecha de totales_control). Devuelve (desde, hasta) como texto 'AAAA-MM-DD'."""
    hasta = dt.date.fromisoformat(P.dia_de_a(totales_control))
    primero = dt.date.fromisoformat(str(totales_control["fecha"].min()))
    desde = max(primero, hasta - dt.timedelta(days=dias - 1))
    return desde.isoformat(), hasta.isoformat()


def sin_pendientes(rutas):
    """Marcas de trabajo sin terminar en una lista de archivos: {archivo: [marcas]}. Busca las dos marcas del esqueleto
    (el error de funcion sin implementar y la palabra de pendiente); se arman por partes para que este modulo no se delate a si mismo."""
    pendientes_por_archivo = {}
    marcas_buscadas = ("NotImplemented" + "Error", "TO" + "DO")
    for r in map(Path, rutas):
        texto = r.read_text(encoding="utf-8") if r.exists() else ""
        marcas = [m for m in marcas_buscadas if m in texto]
        if marcas or not r.exists():
            pendientes_por_archivo[r.name] = marcas or ["no existe"]
    return pendientes_por_archivo


def registrar_fila_indice(ruta, fila, etapa="t18"):
    """Deja la fila de la etapa DENTRO de la tabla del indice de evidencias (justo despues de su ultima fila).

    Una fila escrita al final del archivo, despues del texto, no forma parte de la tabla: se ve como una linea suelta y
    nadie la nota, pero el indice queda sin esa etapa. La funcion distingue tres casos y devuelve cual ocurrio:
    'ya estaba' (dentro de la tabla), 'agregada' (no existia) o 'movida' (existia fuera de la tabla)."""
    lineas = Path(ruta).read_text(encoding="utf-8").splitlines()
    marca = re.compile(r"^\|\s*" + etapa + r"\b", re.I)
    inicio = next(i for i, l in enumerate(lineas) if l.startswith("| Etapa"))
    fin = inicio
    while fin + 1 < len(lineas) and lineas[fin + 1].startswith("|"):
        fin += 1
    dentro = any(marca.match(l) for l in lineas[inicio:fin + 1])
    fuera = [i for i, l in enumerate(lineas) if marca.match(l) and not inicio <= i <= fin]
    if dentro and not fuera:
        return "ya estaba"
    for i in reversed(fuera):
        del lineas[i]
    if not dentro:
        lineas.insert(fin + 1, fila)
    Path(ruta).write_text("\n".join(lineas) + "\n", encoding="utf-8")
    return "movida" if fuera else "agregada"


def archivos_seleccionados(capa, ventana=None, patron="*.parquet"):
    """Archivos de la capa que el plan abre: todos, o los de las carpetas de la ventana (por la ruta)."""
    salida = []
    for c in sorted(Path(capa).glob("anio=*/mes=*/dia=*")):
        dia = f"{c.parent.parent.name[5:]}-{c.parent.name[4:]}-{c.name[4:]}"
        if not ventana or not ventana[0] or ventana[0] <= dia <= ventana[1]:
            salida += sorted(c.glob(patron))
    return salida


# ------------------------------------------------------------------ producto SQL
def leer_sql(ruta_sql):
    return Path(ruta_sql).read_text(encoding="utf-8")


def hash_sql(ruta_sql):
    """Version de la regla: hash del texto del SQL."""
    return hashlib.sha256(leer_sql(ruta_sql).encode("utf-8")).hexdigest()


def plan_producto(spark, ruta_sql):
    """Plan fisico (modo formatted) del producto, normalizado, y su hash."""
    plan = P.normalizar_plan(P.capturar_plan(spark.sql(leer_sql(ruta_sql))))
    return plan, P.hash_plan(plan)


def construir_producto(spark, ruta_sql, salida, sobrescritura="dinamica"):
    """Ejecuta el SQL y escribe la capa de salida particionada por dia (columnas anio, mes, dia).
    dinamica: reemplaza solo las particiones que trae el resultado · estatica: borra la tabla completa."""
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic" if sobrescritura == "dinamica" else "static")
    try:
        # repartition por las columnas de la ruta: todas las filas de un dia caen en la misma tarea y el dia queda en UN archivo
        (spark.sql(leer_sql(ruta_sql)).repartition("anio", "mes", "dia")
              .write.mode("overwrite").partitionBy("anio", "mes", "dia").parquet(str(salida)))
    finally:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


def leer_producto_pandas(carpeta, columnas=None):
    """Lee la capa de salida con pyarrow, sin Spark (las columnas de particion no se guardan en el archivo)."""
    partes = [pq.read_table(str(f), columns=columnas).to_pandas() for f in sorted(Path(carpeta).rglob("*.parquet"))]
    pdf = pd.concat(partes, ignore_index=True)
    if "fecha" in pdf:
        pdf["fecha"] = pdf["fecha"].astype(str)
    return pdf


def hash_salida(carpeta, claves):
    return M.hash_resultado_ordenado(leer_producto_pandas(carpeta), claves)


def hash_entrada(archivos):
    """Hash del conjunto de archivos de entrada (ruta relativa al nombre de carpeta de dia y tamano)."""
    h = hashlib.sha256()
    for f in archivos:
        h.update(f"{f.parent.name}|{f.parent.parent.name}|{f.parent.parent.parent.name}|{f.stat().st_size}\n".encode("utf-8"))
    return h.hexdigest()


def construir_y_registrar(spark, raiz, lago, producto, equipo, volumen, bitacora="bitacora_capas.jsonl", sobrescritura="dinamica",
                          ventana=None, hipotesis=None, guardar_plan=True, carpeta_sql=None):
    """Construye un producto ELT, mide lo que leyo y escribio, y anexa la linea de bitacora. Devuelve la linea.
    carpeta_sql: donde estan los archivos .sql (por omision sql/consolidada/ del paquete)."""
    raiz, lago = Path(raiz), Path(lago)
    tabla, claves = PRODUCTOS[producto]
    ruta_sql = Path(carpeta_sql or raiz / "sql" / "consolidada") / f"{tabla}.sql"
    salida = lago / "consolidada_elt" / tabla
    registrar_prep(spark, lago, ventana)
    plan, h_plan = plan_producto(spark, ruta_sql)
    if guardar_plan:
        (raiz / "planes_s18").mkdir(exist_ok=True)
        (raiz / "planes_s18" / f"plan_{volumen}_{producto}.txt").write_text(plan, encoding="utf-8")
    desde = M.marca(spark)
    t0 = dt.datetime.now(dt.timezone.utc)
    construir_producto(spark, ruta_sql, salida, sobrescritura)
    tiempo = round((dt.datetime.now(dt.timezone.utc) - t0).total_seconds(), 3)
    met = M.medir_desde(spark, desde)
    sel = archivos_seleccionados(lago / "refinada_spark", ventana)
    escritos = archivos_seleccionados(salida, ventana)
    pdf = leer_producto_pandas(salida)
    h = hipotesis or {}
    linea = {
        "marca_medicion": t0.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), "equipo": equipo, "volumen": volumen, "capa": "consolidada_elt",
        "accion": "construir", "producto": producto, "tabla": tabla, "enfoque": "ELT", "spark_version": spark.version, "aqe": False,
        "shuffle_partitions": 8, "ventana": list(ventana) if ventana else None, "sobrescritura": sobrescritura,
        "archivos_leidos": met["archivos_leidos"], "archivos_fuente": len(sel),
        "bytes_archivos_seleccionados": M.tamano_de(sel), "bytes_entrada_etapas": met["bytes_entrada_etapas"],
        "metrica_bytes": M.METRICA_BYTES, "bytes_leidos": M.bytes_leidos("parquet", sel, plan), "columnas_leidas": M.columnas_leidas(plan),
        "filas_salida_escaneo": met["filas_salida_escaneo"], "filas_resultado": int(len(pdf)),
        "bytes_escritos": M.tamano_de(escritos), "archivos_escritos": len(escritos),
        "etapas": met["etapas"], "tareas": met["tareas"], "bytes_mezcla": met["bytes_mezcla"], "derrame_b": met["derrame_b"],
        "hash_entrada": hash_entrada(sel), "hash_sql": hash_sql(ruta_sql), "hash_plan_normalizado": h_plan,
        "hash_resultado": hash_salida(salida, claves), "tiempo_s": tiempo,
        "hipotesis_clave": h.get("clave"), "hipotesis_texto": h.get("texto"), "hipotesis_valor": h.get("valor"), "marca_hipotesis": h.get("marca_hipotesis")}
    registrar_bitacora(raiz / bitacora, linea)
    return linea


# ---------------------------------------------------------------------- contratos
def _medida(datos, especificacion):
    t = datos[especificacion["tabla"]]
    if especificacion["medida"] == "filas":
        return int(len(t))
    return float(t[especificacion["columna"]].sum())


def evaluar_invariante(inv, datos):
    """(observado, esperado, ok) de un invariante."""
    tipo = inv["tipo"]
    if tipo == "columnas_requeridas":
        orden = datos[inv["tabla"]].attrs.get("orden_columnas", list(datos[inv["tabla"]].columns))
        observado = list(orden)[:len(inv["columnas"])]
        return observado, inv["columnas"], observado == inv["columnas"]
    if tipo == "sin_nulos":
        t = datos[inv["tabla"]]
        nulos = {c: int(t[c].isna().sum()) for c in inv["columnas"]}
        return nulos, {c: 0 for c in inv["columnas"]}, all(v == 0 for v in nulos.values())
    if tipo == "unico":
        duplicadas = int(datos[inv["tabla"]].duplicated(inv["columnas"]).sum())
        return duplicadas, 0, duplicadas == 0
    if tipo == "valores_en":
        extra = sorted(set(datos[inv["tabla"]][inv["columna"]].dropna().unique()) - set(inv["permitidos"]))
        return extra, [], not extra
    if tipo == "conteo_igual":
        a, b = _medida(datos, inv["a"]), _medida(datos, inv["b"])
        return a, b, a == b
    if tipo == "suma_igual":
        a, b = _medida(datos, inv["a"]), _medida(datos, inv["b"])
        return round(a, 6), round(b, 6), abs(a - b) <= inv.get("tolerancia", TOLERANCIA)
    if tipo == "compuesto":
        malas = int((~datos[inv["tabla"]].eval(inv["expresion"])).sum())
        return malas, 0, malas == 0
    if tipo == "igual_por_grupo":
        m = datos[inv["a"]].merge(datos[inv["b"]], on=inv["clave"], how="outer", suffixes=("_a", "_b"), indicator=True)
        solo = int((m["_merge"] != "both").sum())
        malas, dif = 0, 0.0
        for c, tol in inv["columnas"].items():
            d = (m[c + "_a"] - m[c + "_b"]).abs()
            malas += int((d > tol).sum())
            dif = max(dif, float(d.max()))
        return {"grupos_sin_par": solo, "grupos_distintos": malas, "dif_max": float(f"{dif:.3e}")}, {"grupos_sin_par": 0, "grupos_distintos": 0}, solo == 0 and malas == 0
    raise ValueError(f"tipo de invariante desconocido: {tipo}")


def verificar_contrato(ruta_contrato, datos, lanzar=True):
    """Aplica un contrato JSON a las tablas `datos` (dict de DataFrames). Devuelve una lista de resultados
    (invariante, observado, esperado, ok) y, si `lanzar`, falla en el PRIMER invariante que no se cumple."""
    contrato = json.loads(Path(ruta_contrato).read_text(encoding="utf-8"))
    resultados = []
    for inv in contrato["invariantes"]:
        observado, esperado, ok = evaluar_invariante(inv, datos)
        resultados.append({"frontera": contrato["frontera"], "invariante": inv["id"], "tipo": inv["tipo"],
                           "observado": observado, "esperado": esperado, "ok": bool(ok)})
    if lanzar:
        for r in resultados:
            if not r["ok"]:
                raise ContratoIncumplido(r["frontera"], r["invariante"], f"observado {r['observado']} · esperado {r['esperado']}")
    return resultados


def control_desde_cruda(carpeta_cruda):
    """Filas y suma de consumo por (fecha, sector) leyendo los CSV con pandas: es el control con el que se
    reconcilia la preparacion. Un valor ilegible queda nulo y NO suma."""
    partes = []
    for f in sorted(Path(carpeta_cruda).rglob("lecturas.csv")):
        d = pd.read_csv(f, usecols=["timestamp", "sector", "consumo_m3"], dtype={"timestamp": "string", "sector": "string"})
        d["consumo_m3"] = pd.to_numeric(d["consumo_m3"], errors="coerce")
        d["fecha"] = d["timestamp"].str.slice(0, 10)
        partes.append(d.groupby(["fecha", "sector"]).agg(n_lecturas=("sector", "size"), consumo_total_m3=("consumo_m3", "sum")).reset_index())
    return pd.concat(partes, ignore_index=True)


def leer_preparacion_pandas(capa, columnas=("sector", "consumo_m3", "quality_flag", "fecha")):
    """Lee la preparacion con pyarrow (solo las columnas pedidas). El orden real de las columnas del archivo
    se guarda en pdf.attrs para el invariante de esquema."""
    archivos = sorted(Path(capa).rglob("*.parquet"))
    orden = pq.ParquetFile(archivos[0]).schema_arrow.names
    pdf = pd.concat([pq.read_table(str(f), columns=list(columnas)).to_pandas() for f in archivos], ignore_index=True)
    pdf["fecha"] = pdf["fecha"].astype(str)
    pdf.attrs["orden_columnas"] = orden
    return pdf


def tablas_cruda_a_preparacion(capa_prep, carpeta_cruda_control):
    """Tablas del contrato cruda_a_preparacion: la preparacion a evaluar y el control tomado de la cruda ORIGINAL."""
    prep = leer_preparacion_pandas(capa_prep, ("meter_id", "timestamp", "sector", "consumo_m3", "presion_bar", "quality_flag", "fecha"))
    por_grupo = prep.groupby(["fecha", "sector"]).agg(n_lecturas=("sector", "size"), consumo_total_m3=("consumo_m3", "sum")).reset_index()
    return {"preparacion": prep, "preparacion_por_grupo": por_grupo, "control": control_desde_cruda(carpeta_cruda_control)}


def tablas_preparacion_a_consolidada(capa_prep, carpeta_producto):
    return {"preparacion": leer_preparacion_pandas(capa_prep), "producto": leer_producto_pandas(carpeta_producto)}


def evaluar_y_registrar_contrato(raiz, ruta_contrato, datos, origen, equipo, volumen, bitacora="bitacora_capas.jsonl"):
    """Evalua un contrato, anexa la linea a la bitacora y devuelve (resultados, linea)."""
    t0 = ahora()
    res = verificar_contrato(ruta_contrato, datos, lanzar=False)
    fallos = [r for r in res if not r["ok"]]
    linea = {"marca_medicion": t0, "equipo": equipo, "volumen": volumen, "capa": "contrato", "accion": "contrato", "producto": "-", "enfoque": "ELT",
             "spark_version": None, "aqe": False, "shuffle_partitions": 8, "hash_resultado": hashlib.sha256(json.dumps(res, sort_keys=True, default=str).encode()).hexdigest(),
             "frontera": res[0]["frontera"], "origen": origen, "aprobado": not fallos,
             "invariante_que_falla": fallos[0]["invariante"] if fallos else None,
             "invariantes": [{"id": r["invariante"], "ok": r["ok"], "observado": r["observado"], "esperado": r["esperado"]} for r in res]}
    registrar_bitacora(Path(raiz) / bitacora, linea)
    return res, linea


# ---------------------------------------------------------------------- reproceso
def copiar_salida(origen, destino):
    if Path(destino).exists():
        shutil.rmtree(destino)
    shutil.copytree(origen, destino)


def reprocesar_y_registrar(spark, raiz, lago, producto, ventana, equipo, volumen, copia, sobrescritura="dinamica", bitacora="bitacora_capas.jsonl", hipotesis=None, carpeta_sql=None):
    """Reprocesa la ventana escribiendo en `copia` (copia de la capa de salida) y compara el arbol de archivos antes
    y despues. Anexa la linea de bitacora y la devuelve."""
    raiz, lago = Path(raiz), Path(lago)
    tabla, claves = PRODUCTOS[producto]
    ruta_sql = Path(carpeta_sql or raiz / "sql" / "consolidada") / f"{tabla}.sql"
    antes = resumen_arbol(copia)
    registrar_prep(spark, lago, ventana)
    plan_ventana, _ = plan_producto(spark, ruta_sql)
    desde = M.marca(spark)
    t0 = dt.datetime.now(dt.timezone.utc)
    construir_producto(spark, ruta_sql, copia, sobrescritura)
    tiempo = round((dt.datetime.now(dt.timezone.utc) - t0).total_seconds(), 3)
    met = M.medir_desde(spark, desde)
    despues = resumen_arbol(copia)
    intactas, reescritas, perdidas, nuevas = comparar_arboles(antes, despues)
    sel = archivos_seleccionados(lago / "refinada_spark", ventana)
    escritos = archivos_seleccionados(copia, ventana)
    h = hipotesis or {}
    linea = {
        "marca_medicion": t0.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), "equipo": equipo, "volumen": volumen, "capa": "consolidada_elt", "accion": "reproceso",
        "producto": producto, "tabla": tabla, "enfoque": "ELT", "spark_version": spark.version, "aqe": False, "shuffle_partitions": 8,
        "ventana": list(ventana), "sobrescritura": sobrescritura, "particiones_antes": len(antes), "particiones_intactas": intactas,
        "particiones_reescritas": reescritas, "particiones_perdidas": perdidas, "particiones_nuevas": nuevas,
        "archivos_leidos": met["archivos_leidos"], "bytes_archivos_seleccionados": M.tamano_de(sel), "bytes_entrada_etapas": met["bytes_entrada_etapas"],
"metrica_bytes": M.METRICA_BYTES, "bytes_leidos": M.bytes_leidos("parquet", sel, plan_ventana), "columnas_leidas": M.columnas_leidas(plan_ventana),
        "filas_salida_escaneo": met["filas_salida_escaneo"], "bytes_escritos": M.tamano_de(escritos), "archivos_escritos": len(escritos),
        "etapas": met["etapas"], "tareas": met["tareas"], "bytes_mezcla": met["bytes_mezcla"],
        "hash_sql": hash_sql(ruta_sql), "hash_entrada": hash_entrada(sel), "hash_resultado": hash_salida(copia, claves), "tiempo_s": tiempo,
        "hipotesis_clave": h.get("clave"), "hipotesis_texto": h.get("texto"), "hipotesis_valor": h.get("valor"), "marca_hipotesis": h.get("marca_hipotesis")}
    registrar_bitacora(raiz / bitacora, linea)
    return linea


def leer_bitacora(raiz, volumen=None, bitacora="bitacora_capas.jsonl"):
    """Lineas de la bitacora de capas, opcionalmente de un solo volumen (base o ampliacion)."""
    ruta = Path(raiz) / bitacora
    lineas = [json.loads(l) for l in ruta.read_text(encoding="utf-8").splitlines() if l.strip()] if ruta.exists() else []
    return [b for b in lineas if volumen is None or b.get("volumen") == volumen]


def escribir_comparacion(raiz, lago, volumen, ruta_salida, bitacora="bitacora_capas.jsonl"):
    """comparacion.csv: una fila por (producto, enfoque) con la ultima medicion de cada uno. Devuelve la tabla (DataFrame)."""
    ult = {}
    for b in leer_bitacora(raiz, volumen, bitacora):
        if b.get("accion") == "construir":
            ult[(b["producto"], b["enfoque"])] = b
    no_conf = int(leer_producto_pandas(Path(lago) / "consolidada_elt" / "consumo_dia_sector")["n_no_confiables"].sum())
    filas = []
    for (producto, enfoque), b in sorted(ult.items()):
        fr = b["filas_resultado"]
        filas.append({"producto": producto, "enfoque": enfoque, "metrica_bytes": b["metrica_bytes"], "archivos_leidos": b["archivos_leidos"],
                      "bytes_leidos": b["bytes_leidos"], "filas_leidas": b["filas_salida_escaneo"],
                      "filas_resultado": sum(fr.values()) if isinstance(fr, dict) else fr, "bytes_escritos": b["bytes_escritos"],
                      "etapas": b["etapas"], "tareas": b["tareas"], "bytes_mezcla": b["bytes_mezcla"],
                      "filas_descartadas_visibles": no_conf if enfoque == "ELT" else 0})
    tabla = pd.DataFrame(filas)
    tabla.to_csv(ruta_salida, index=False)
    return tabla


def registrar_integridad(raiz, lago, volumen, antes, despues, equipo, bitacora="bitacora_capas.jsonl"):
    """Anexa a la bitacora la prueba de que el lago heredado y la preparacion no cambiaron.
    antes y despues: dicts {'lago': hash del lago, 'preparacion': hash del arbol de refinada_spark/}."""
    linea = {"marca_medicion": ahora(), "equipo": equipo, "volumen": volumen, "capa": "lago", "accion": "integridad", "producto": "-", "enfoque": "-",
             "spark_version": None, "aqe": False, "shuffle_partitions": 8, "hash_resultado": "-", "carpeta": Path(lago).name,
             "hash_datalake_antes": antes["lago"], "hash_datalake_despues": despues["lago"],
             "hash_refinada_spark_antes": antes["preparacion"], "hash_refinada_spark_despues": despues["preparacion"]}
    registrar_bitacora(Path(raiz) / bitacora, linea)
    return linea


def registrar_hipotesis(ruta, hipotesis):
    """Escribe las hipotesis con la marca de tiempo del momento: deben existir antes de medir."""
    for h in hipotesis.values():
        h["marca_hipotesis"] = ahora()
    Path(ruta).write_text(json.dumps(hipotesis, ensure_ascii=False, indent=2), encoding="utf-8")
    return hipotesis


def registrar_bitacora(ruta, registro):
    with open(ruta, "a", encoding="utf-8") as f:
        f.write(json.dumps(registro, ensure_ascii=False, default=str) + "\n")


# ---------------------------------------------------------------------- principal
def leer_parametros(argv=None):
    ap = argparse.ArgumentParser(description="Ejecutor de capas: SQL sobre la preparacion, contratos y reproceso.")
    ap.add_argument("accion", choices=["construir", "contratos", "reprocesar"])
    ap.add_argument("--equipo", default="sin-equipo")
    ap.add_argument("--volumen", choices=["base", "ampliacion"], default="base")
    ap.add_argument("--carpeta", default=None)
    ap.add_argument("--raiz", default=".")
    ap.add_argument("--producto", choices=["consumo", "presion", "ambos"], default="ambos")
    ap.add_argument("--desde", default=None)
    ap.add_argument("--hasta", default=None)
    ap.add_argument("--sobrescritura", choices=["dinamica", "estatica"], default="dinamica")
    ap.add_argument("--bitacora", default="bitacora_capas.jsonl")
    ap.add_argument("--master", default="local[4]")
    ap.add_argument("--driver-memory", default="1g")
    return ap.parse_args(argv)


def main(argv=None):
    a = leer_parametros(argv)
    raiz = Path(a.raiz)
    lago = carpeta_volumen(a.raiz, a.volumen, a.carpeta)
    productos = list(PRODUCTOS) if a.producto == "ambos" else [a.producto]
    ventana = (a.desde, a.hasta) if a.desde else None
    if a.accion == "contratos":
        for ruta, datos in ((raiz / "contratos" / "preparacion_a_consolidada.json", tablas_preparacion_a_consolidada(lago / "refinada_spark", lago / "consolidada_elt" / "consumo_dia_sector")),
                            (raiz / "contratos" / "cruda_a_preparacion.json", tablas_cruda_a_preparacion(lago / "refinada_spark", lago / "cruda" / "acueducto"))):
            res, _ = evaluar_y_registrar_contrato(raiz, ruta, datos, str(lago.name) + "/refinada_spark", a.equipo, a.volumen, a.bitacora)
            for r in res:
                print(r["frontera"], r["invariante"], "APROBADO" if r["ok"] else "NO APROBADO", r["observado"], r["esperado"])
        return 0
    spark = abrir_sesion(a.master, a.driver_memory)
    try:
        for p in productos:
            if a.accion == "construir":
                l = construir_y_registrar(spark, raiz, lago, p, a.equipo, a.volumen, a.bitacora, a.sobrescritura, ventana)
            else:
                copia = raiz / "_copias_reproceso" / PRODUCTOS[p][0]
                if not copia.exists():
                    copiar_salida(lago / "consolidada_elt" / PRODUCTOS[p][0], copia)
                l = reprocesar_y_registrar(spark, raiz, lago, p, ventana, a.equipo, a.volumen, copia, a.sobrescritura, a.bitacora)
            print(json.dumps(l, ensure_ascii=False, default=str, indent=1))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
