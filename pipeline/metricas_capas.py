"""Medir lo que un trabajo de capas LEYO y ESCRIBIO, desde la interfaz de Spark.

Que hace
    Cuando un trabajo termina en una ESCRITURA (por ejemplo, agregar la cruda y guardar el
    resultado), el plan que ejecuto Spark no se puede consultar desde el DataFrame de origen:
    las metricas del escaneo (archivos, filas) quedan en el plan de la escritura. Este modulo las
    recupera de la misma interfaz que muestra la pagina de SQL de Spark, por su API REST:

      - del nodo "Scan csv" o "Scan parquet": archivos leidos y filas de salida del escaneo;
      - de las etapas: bytes de entrada que pidio el lector (inputBytes), tareas, bytes de mezcla;
      - del disco: tamano exacto de los archivos seleccionados y de los archivos escritos.

Tres formas de contar "bytes leidos" (hay que declarar cual se usa)
    (a) bytes de los archivos seleccionados: la suma de tamanos de los archivos que el plan abre.
    (b) bytes de entrada de las etapas: lo que el lector pidio al sistema de archivos.
    (c) bytes comprimidos de las columnas leidas (solo Parquet), calculados con pyarrow.
    Con archivos Parquet de unos 25 KB y un solo grupo de filas, ni (a) ni (b) cambian al leer menos
    columnas (el lector pide el grupo de filas completo): la metrica que distingue la poda de columnas
    es (c). Para un CSV no hay poda posible y los bytes leidos son los del archivo. Por eso esta
    practica declara METRICA_BYTES = "bytes_columnas_leidas": CSV, bytes del archivo; Parquet, bytes
    comprimidos de las columnas que el plan lee (ReadSchema). Es una cota inferior de lo que pide el lector.

Ejercicio didactico y produccion
    Aqui se lee una interfaz local y se suman archivos de decenas de KB. En produccion estas
    metricas llegan de la factura del almacen (bytes escaneados) o de un servicio de metricas, y se
    guardan junto con la version del codigo que las produjo.
"""
import contextlib
import hashlib
import io
import json
import re
import time
import urllib.request
from pathlib import Path

import pyarrow.parquet as pq

SQL_NODOS = "sql?details=true&planDescription=false&length=100000"
METRICA_BYTES = "bytes_columnas_leidas"


def _rest(spark, ruta):
    puerto = spark.sparkContext.uiWebUrl.rsplit(":", 1)[-1]
    url = f"http://localhost:{puerto}/api/v1/applications/{spark.sparkContext.applicationId}/{ruta}"
    with urllib.request.urlopen(url) as r:
        return json.load(r)


def marca(spark):
    """(siguiente trabajo, siguiente etapa, siguiente ejecucion SQL): lo que ocurra despues se mide."""
    trabajos = max([j["jobId"] for j in _rest(spark, "jobs")], default=-1) + 1
    etapas = max([e["stageId"] for e in _rest(spark, "stages")], default=-1) + 1
    ejecuciones = max([x["id"] for x in _rest(spark, "sql?details=false&planDescription=false&length=100000")], default=-1) + 1
    return trabajos, etapas, ejecuciones


def _entero(texto):
    """'541,368' -> 541368. Las metricas del escaneo llegan como texto con formato."""
    return int(re.sub(r"[^\d]", "", texto))


def medir_desde(spark, desde, espera=10.0):
    """Metricas de todo lo ejecutado despues de `desde` (la tupla que devolvio marca()).

    La interfaz se actualiza de forma asincrona: se sondea hasta que todas las ejecuciones SQL figuren
    como COMPLETED (con un tope de espera)."""
    j0, e0, x0 = desde
    limite = time.time() + espera
    while True:
        ejec = [x for x in _rest(spark, SQL_NODOS) if x["id"] >= x0]
        if (ejec and all(x["status"] == "COMPLETED" for x in ejec)) or time.time() > limite:
            break
        time.sleep(0.1)
    escaneos = []
    for x in ejec:
        for n in x.get("nodes", []):
            if n["nodeName"].startswith("Scan "):
                m = {k["name"]: k["value"] for k in n.get("metrics", [])}
                escaneos.append({"formato": n["nodeName"].split()[1],
                                 "archivos": _entero(m.get("number of files read", "0")),
                                 "filas_salida": _entero(m.get("number of output rows", "0")),
                                 "carpetas": _entero(m.get("number of partitions read", "0"))})
    etapas = [e for e in _rest(spark, "stages") if e["stageId"] >= e0]
    hechas = [e for e in etapas if e["status"] == "COMPLETE"]
    return {
        "ejecuciones_sql": len(ejec),
        "escaneos": len(escaneos),
        "archivos_leidos": sum(s["archivos"] for s in escaneos),
        "filas_salida_escaneo": sum(s["filas_salida"] for s in escaneos),
        "formato_escaneo": sorted({s["formato"] for s in escaneos}),
        "trabajos": len([j for j in _rest(spark, "jobs") if j["jobId"] >= j0]),
        "etapas": len(hechas),
        "tareas": sum(e["numCompleteTasks"] for e in hechas),
        "bytes_entrada_etapas": sum(e["inputBytes"] for e in hechas),
        "bytes_mezcla": sum(e["shuffleWriteBytes"] for e in hechas),
        "derrame_b": sum(e["diskBytesSpilled"] for e in hechas),
    }


def tamano_de(archivos):
    """Suma exacta de tamanos (bytes) de una lista de archivos."""
    return sum(Path(a).stat().st_size for a in archivos)


def archivos_de(carpeta, patron):
    return sorted(Path(carpeta).rglob(patron))


def bytes_columnas_parquet(archivos, columnas):
    """(c) bytes comprimidos de las columnas indicadas, sumados en el pie de cada archivo Parquet."""
    total = 0
    for f in archivos:
        md = pq.ParquetFile(f).metadata
        for g in range(md.num_row_groups):
            rg = md.row_group(g)
            for c in range(rg.num_columns):
                cc = rg.column(c)
                if cc.path_in_schema in columnas:
                    total += cc.total_compressed_size
    return total


def hash_resultado_ordenado(pdf, claves):
    """Hash del CONTENIDO: filas ordenadas por TODAS las columnas de la clave, sumas a 6 decimales.
    Con claves compuestas (fecha, sector) no basta ordenar por la primera columna."""
    filas = pdf.sort_values(list(claves)).itertuples(index=False)
    texto = "".join(",".join(f"{v:.6f}" if isinstance(v, float) else str(v) for v in f) + "\n" for f in filas)
    return hashlib.sha256(texto.encode("utf-8")).hexdigest()


def capturar_plan(df, modo="formatted"):
    """Texto de explain() (en PySpark imprime: se captura la salida estandar)."""
    salida = io.StringIO()
    with contextlib.redirect_stdout(salida):
        df.explain(mode=modo)
    return salida.getvalue()


def columnas_leidas(plan_texto):
    """Columnas que el plan lee, tomadas del campo ReadSchema del primer nodo de escaneo."""
    m = re.search(r"^ReadSchema: struct<(.*)>$", plan_texto, re.M)
    return [c.split(":")[0] for c in m.group(1).split(",")] if m else []


def bytes_leidos(formato, archivos, plan_texto):
    """Metrica declarada (bytes_columnas_leidas): CSV, tamano de los archivos; Parquet, bytes comprimidos de las
    columnas del ReadSchema. Las columnas de particion (anio, mes, dia) no estan dentro del archivo."""
    if formato == "csv":
        return tamano_de(archivos)
    return bytes_columnas_parquet(archivos, set(columnas_leidas(plan_texto)))
