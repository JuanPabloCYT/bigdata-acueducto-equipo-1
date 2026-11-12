"""Verificacion por etapa del pipeline del acueducto: un comando, un veredicto, valores observados.

Que hace
    Comprueba que cada etapa del proyecto integrador esta en el estado esperado y lo dice con
    el valor observado frente al esperado:
      t12  ingesta incremental idempotente (PostgreSQL: stg_lecturas y estado_ingesta)
      t13  conector de la API (PostgreSQL: stg_estado_medidor y bitacora_ingesta_api)
      t14  captura de cambios por marca temporal (PostgreSQL: maestro_suscriptores,
           stg_cambios_suscriptor y estado_cdc_suscriptores)
      t15  carga con Spark (consolidada_spark/ y bitacora_carga_spark.jsonl)
      t16  capa refinada_spark/ (esquema, tipo fisico de la marca temporal, particiones,
           reconciliacion contra consolidada/ y bitacora_refinada_spark.jsonl)
      t17  planes de ejecucion (bitacora_planes.jsonl, planes/ normalizados y entregables de texto)
      t18  ETL frente a ELT (bitacora_capas.jsonl, consolidada_elt/ y tablero_etl/, contratos por frontera,
           reproceso por ventana y entregables de texto)
    Imprime una linea por comprobacion (APROBADO o NO APROBADO) y termina con codigo 0 solo si
    todas pasan. Pegue la salida en su indice de evidencias.

Requisitos
    t12 a t14: el servicio postgres (perfil s12) en marcha. El script usa psycopg2 si esta
    instalado en su Python y, si no, ejecuta psql dentro del contenedor con el cliente de Docker.
    No levanten el perfil s12 junto al perfil s16 o s17: apaguen uno antes de levantar el otro.
    t15 a t18: no necesitan Spark ni PostgreSQL; leen los archivos con pyarrow y pandas, de modo
    que corren dentro del contenedor del servicio spark o en un Python con pyarrow.

Como usarlo (desde la carpeta raiz del paquete)
    python pipeline/verificar_etapa.py --etapa t16
    python pipeline/verificar_etapa.py --etapa t17                      # ampliacion del equipo
    python pipeline/verificar_etapa.py --etapa t17 --volumen base        # estado de referencia sobre la base
    python pipeline/verificar_etapa.py --etapa t18                      # ampliacion del equipo
    python pipeline/verificar_etapa.py --etapa t12 --nombre-contenedor acueducto_postgres_s12
    docker compose --profile s18 exec spark python pipeline/verificar_etapa.py --etapa t18

Ejercicio didactico y produccion
    Aqui la verificacion compara contra el estado de referencia del lago base (valores fijos).
    En produccion esas comprobaciones viven en pruebas automaticas de datos que se ejecutan en
    cada despliegue y bloquean la publicacion cuando fallan, con umbrales por tabla y alertas.
"""
import argparse
import glob
import hashlib
import json
import re
import subprocess
import time
from pathlib import Path

TOLERANCIA = 1e-6
HASH_CONSOLIDADA_SPARK = "e3e31f0e1f7837259597f405dba79dd9823680e272621dca227152016af82cd6"


# ----------------------------------------------------------------- utilidades
class Informe:
    def __init__(self, etapa):
        self.etapa = etapa
        self.filas = []

    def comprobar(self, nombre, observado, esperado, ok=None):
        ok = (observado == esperado) if ok is None else ok
        self.filas.append((bool(ok), nombre, observado, esperado))
        marca = "APROBADO   " if ok else "NO APROBADO"
        print(f"[{marca}] {nombre}: observado {observado} · esperado {esperado}")
        return ok

    def cerrar(self, inicio):
        aprobadas = sum(1 for f in self.filas if f[0])
        total = len(self.filas)
        veredicto = "APROBADA" if aprobadas == total and total > 0 else "NO APROBADA"
        print(f"\nEtapa {self.etapa}: {veredicto} ({aprobadas} de {total} comprobaciones, {time.time() - inicio:.1f} s)")
        return 0 if veredicto == "APROBADA" else 1


def consultar(sql, contenedor, puerto, host):
    """Ejecuta una consulta y devuelve una lista de filas (listas de texto). Usa psycopg2 si
    esta disponible; si no, psql dentro del contenedor."""
    try:
        import psycopg2
        con = psycopg2.connect(host=host, port=puerto, user="postgres", password="postgres", dbname="acueducto",
                               connect_timeout=5)
        try:
            with con.cursor() as cur:
                cur.execute(sql)
                return [[str(c) for c in fila] for fila in cur.fetchall()]
        finally:
            con.close()
    except ImportError:
        pass
    try:
        r = subprocess.run(["docker", "exec", contenedor, "psql", "-U", "postgres", "-d", "acueducto",
                            "-t", "-A", "-F", "|", "-c", sql], capture_output=True, text=True, timeout=60)
    except FileNotFoundError:
        raise SystemExit("No hay psycopg2 ni cliente de Docker: instalen psycopg2-binary o ejecuten desde un equipo con Docker.")
    if r.returncode != 0:
        raise SystemExit(f"No se pudo consultar PostgreSQL ({r.stderr.strip()}). Levanten el perfil s12.")
    return [linea.split("|") for linea in r.stdout.strip().splitlines() if linea]


def uno(sql, *a):
    return consultar(sql, *a)[0][0]


# ----------------------------------------------------------------------- etapas
def etapa_t12(a, inf):
    c = (a.nombre_contenedor, a.puerto, a.host)
    filas = int(uno("SELECT count(*) FROM stg_lecturas", *c))
    claves = int(uno('SELECT count(DISTINCT (meter_id, "timestamp")) FROM stg_lecturas', *c))
    inf.comprobar("stg_lecturas: filas", filas, 1200)
    inf.comprobar("stg_lecturas: filas sin repetir clave (meter_id, timestamp)", claves, filas)
    marcas = consultar("SELECT sector, ultima_marca_agua::text FROM estado_ingesta ORDER BY sector", *c)
    inf.comprobar("estado_ingesta: sectores con marca de agua", len(marcas), 4)
    inf.comprobar("estado_ingesta: marcas de agua (una por sector)", sorted({m[1] for m in marcas}), ["2024-06-02 23:00:00"])


def etapa_t13(a, inf):
    c = (a.nombre_contenedor, a.puerto, a.host)
    filas = int(uno("SELECT count(*) FROM stg_estado_medidor", *c))
    claves = int(uno("SELECT count(DISTINCT meter_id) FROM stg_estado_medidor", *c))
    inf.comprobar("stg_estado_medidor: filas", filas, 90)
    inf.comprobar("stg_estado_medidor: medidores distintos (UPSERT sin duplicar)", claves, filas)
    res = dict(consultar("SELECT resultado, count(*) FROM bitacora_ingesta_api GROUP BY resultado ORDER BY resultado", *c))
    inf.comprobar("bitacora_ingesta_api: intentos registrados", sum(int(v) for v in res.values()), 16)
    inf.comprobar("bitacora_ingesta_api: intentos por resultado", {k: int(v) for k, v in res.items()}, {"EXITO": 12, "REINTENTABLE": 4})
    inf.comprobar("bitacora_ingesta_api: corridas", int(uno("SELECT count(DISTINCT corrida_id) FROM bitacora_ingesta_api", *c)), 2)


def etapa_t14(a, inf):
    c = (a.nombre_contenedor, a.puerto, a.host)
    inf.comprobar("maestro_suscriptores: filas", int(uno("SELECT count(*) FROM maestro_suscriptores", *c)), 94)
    tipos = {k: int(v) for k, v in consultar("SELECT tipo_cambio, count(*) FROM stg_cambios_suscriptor GROUP BY tipo_cambio ORDER BY tipo_cambio", *c)}
    inf.comprobar("stg_cambios_suscriptor: cambios por tipo", tipos, {"alta": 99, "modificacion": 13})
    inf.comprobar("estado_cdc_suscriptores: checkpoint", uno("SELECT max(checkpoint)::text FROM estado_cdc_suscriptores", *c), "2024-06-02 09:00:00")


def leer_bitacora(ruta):
    if not ruta.exists():
        return []
    return [json.loads(l) for l in ruta.read_text(encoding="utf-8").splitlines() if l.strip()]


def etapa_t15(a, inf):
    import pandas as pd
    raiz = Path(a.raiz)
    cons = pd.concat([pd.read_parquet(f).assign(fecha=re.sub(r".*anio=(\d+).*mes=(\d+).*dia=(\d+).*", r"\1-\2-\3", f.replace("\\", "/")))
                      for f in glob.glob(str(raiz / "datalake/consolidada/acueducto/anio=*/mes=*/dia=*/consumo_diario.parquet"))])
    try:
        spark = pd.read_parquet(str(raiz / "datalake/consolidada_spark"))
    except Exception as e:
        inf.comprobar("consolidada_spark/: se puede leer", f"error: {type(e).__name__}", "480 filas")
        return
    spark["fecha"] = spark["fecha"].astype(str)
    inf.comprobar("consolidada_spark/: filas", len(spark), 480)
    m = spark.merge(cons, on=["fecha", "sector"], suffixes=("_s", "_c"))
    inf.comprobar("consolidada_spark/: grupos que coinciden con consolidada/", len(m), 480)
    dif = float((m["consumo_total_m3_s"] - m["consumo_total_m3_c"]).abs().max())
    inf.comprobar("consolidada_spark/: diferencia maxima de la suma de consumo", f"{dif:.1e}", f"<= {TOLERANCIA}", dif <= TOLERANCIA)
    inf.comprobar("consolidada_spark/: lecturas por sector y dia (todas)", sorted(spark["n_lecturas"].unique().tolist()), [1200])
    bit = leer_bitacora(raiz / "bitacora_carga_spark.jsonl")
    inf.comprobar("bitacora_carga_spark.jsonl: corridas registradas", len(bit) >= 1, True)
    inf.comprobar("bitacora_carga_spark.jsonl: corridas de la base con hash de referencia",
                  any(b.get("volumen") == "base" and b.get("hash_salida") == HASH_CONSOLIDADA_SPARK for b in bit), True)


def etapa_t16(a, inf):
    import pandas as pd
    import pyarrow.parquet as pq
    raiz = Path(a.raiz)
    capa = raiz / "datalake" / "refinada_spark"
    carpetas = sorted(glob.glob(str(capa / "anio=*" / "mes=*" / "dia=*")))
    inf.comprobar("refinada_spark/: carpetas de dia", len(carpetas), 120)
    rutas_ok = all(re.search(r"anio=\d{4}[/\\]mes=\d{2}[/\\]dia=\d{2}$", c) for c in carpetas)
    inf.comprobar("refinada_spark/: rutas con ceros (anio=AAAA/mes=MM/dia=DD)", rutas_ok, True)
    archivos = sorted(glob.glob(str(capa / "anio=*" / "mes=*" / "dia=*" / "*.parquet")))
    if not archivos:
        inf.comprobar("refinada_spark/: archivos Parquet", 0, "120 o mas")
        return
    esq = pq.ParquetFile(archivos[0]).schema
    col = esq.column(esq.names.index("timestamp"))
    inf.comprobar("marca temporal: tipo fisico", col.physical_type, "INT64")
    logico = str(col.logical_type)
    m = re.search(r"isAdjustedToUTC=(\w+), timeUnit=(\w+)", logico)
    observado = f"isAdjustedToUTC={m.group(1)}, unidad={m.group(2)}" if m else logico
    esperado = "isAdjustedToUTC=true, unidad=microseconds"
    inf.comprobar("marca temporal: tipo logico (zona y unidad)", observado, esperado)
    inf.comprobar("refinada_spark/: sin columnas de ruta dentro del archivo", [n for n in ("anio", "mes", "dia") if n in esq.names], [])
    frames = [pd.read_parquet(f, columns=["sector", "consumo_m3", "quality_flag", "fecha", "nivel_presion"]) for f in archivos]
    df = pd.concat(frames, ignore_index=True)
    inf.comprobar("refinada_spark/: filas (sin filtro de calidad)", len(df), 576000)
    inf.comprobar("refinada_spark/: clases de presion", sorted(df["nivel_presion"].dropna().unique().tolist()), ["alta", "baja", "normal"])
    df["fecha"] = df["fecha"].astype(str)
    g = df.groupby(["fecha", "sector"]).agg(n=("consumo_m3", "size"), s=("consumo_m3", "sum")).reset_index()
    cons = pd.concat([pd.read_parquet(f).assign(fecha=re.sub(r".*anio=(\d+).*mes=(\d+).*dia=(\d+).*", r"\1-\2-\3", f.replace("\\", "/")))
                      for f in glob.glob(str(raiz / "datalake/consolidada/acueducto/anio=*/mes=*/dia=*/consumo_diario.parquet"))])
    m = g.merge(cons, on=["fecha", "sector"])
    inf.comprobar("reconciliacion con consolidada/: grupos comparados", len(m), 480)
    inf.comprobar("reconciliacion con consolidada/: filas por grupo iguales", int((m["n"] != m["n_lecturas"]).sum()), 0)
    dif = float((m["s"] - m["consumo_total_m3"]).abs().max())
    inf.comprobar("reconciliacion con consolidada/: diferencia maxima de la suma", f"{dif:.1e}", f"<= {TOLERANCIA}", dif <= TOLERANCIA)
    # la refinada heredada no se modifica: sigue en nanosegundos
    h = glob.glob(str(raiz / "datalake/refinada/acueducto/anio=*/mes=*/dia=*/lecturas.parquet"))
    if h:
        eh = pq.ParquetFile(h[0]).schema
        lh = str(eh.column(eh.names.index("timestamp")).logical_type)
        inf.comprobar("refinada/ heredada intacta (nanosegundos)", "nanoseconds" in lh, True)
    bit = leer_bitacora(raiz / "bitacora_refinada_spark.jsonl")
    todas_base = [b for b in bit if b.get("volumen") == "base" and "reconciliacion_heredada" in b]
    base = todas_base[-2:]    # la bitacora es un historial: se juzgan las dos ultimas corridas de la base
    inf.comprobar("bitacora_refinada_spark.jsonl: corridas de la base", len(todas_base) >= 2, True)
    inf.comprobar("bitacora_refinada_spark.jsonl: reconciliacion de la base (dos ultimas corridas)", {b["reconciliacion_heredada"] for b in base} | {b["reconciliacion_control"] for b in base}, {"OK"})
    hashes = {b["hash_salida"] for b in base}
    inf.comprobar("bitacora_refinada_spark.jsonl: hash de la base entre las dos ultimas corridas", len(hashes), 1)
    comp = [b for b in bit if "numero_corrida" in b]
    inf.comprobar("bitacora_refinada_spark.jsonl: corridas de variantes (0 a 3 por variante)", sorted({b["numero_corrida"] for b in comp}), [0, 1, 2, 3])



CAMPOS_PLANES = ["equipo", "volumen", "consulta", "corrida", "modo_explain", "hipotesis_texto", "hipotesis_archivos_leidos",
                 "hipotesis_estrategia_union", "marca_hipotesis", "marca_medicion", "spark_version", "aqe", "shuffle_partitions",
                 "umbral_difusion", "pista", "inferencia_tipos_particion", "partition_filters", "pushed_filters", "read_schema",
                 "archivos_leidos", "tamano_archivos_leidos_texto", "filas_salida_escaneo", "filas_resultado", "estrategia_union",
                 "n_exchange", "n_sort", "etapas", "tareas", "bytes_mezcla", "derrame_b", "hash_resultado", "hash_plan_normalizado",
                 "diferencia_max_independiente", "tiempo_s"]
CINCO = ["A", "A-fecha", "B", "C", "C-forzada"]
PATRONES_PLAN = [r"#\d+", r"plan_id=\d+", r"codegen id", r"(?:file|hdfs|s3a?):/", r"[A-Za-z]:[\\/]"]


def hash_arbol_lago(raiz_lago, excluir=("refinada_spark", "consolidada_spark", "tablero_etl", "consolidada_elt", "_deriva", "_copias_reproceso")):
    """Hash del arbol de archivos del lago (ruta relativa y tamano), sin las capas que se reconstruyen o se derivan:
    las capas nuevas que se escriben dentro del lago no pueden cambiar el hash de las capas de origen."""
    h = hashlib.sha256()
    raiz = Path(raiz_lago)
    for f in sorted(raiz.rglob("*")):
        rel = f.relative_to(raiz)
        if f.is_file() and rel.parts[0] not in excluir and not f.name.endswith(".crc"):
            h.update(f"{rel.as_posix()}|{f.stat().st_size}\n".encode("utf-8"))
    return h.hexdigest()


def etapa_t17(a, inf):
    raiz = Path(a.raiz)
    lago = Path(a.carpeta) if a.carpeta else raiz / ("datalake" if a.volumen == "base" else "datalake_equipo")
    # 1 el modulo de planes esta completo
    fuente = raiz / "pipeline" / "planes_s17.py"
    texto = fuente.read_text(encoding="utf-8") if fuente.exists() else ""
    pendiente = [m for m in ("NotImplementedError", "TODO") if m in texto]
    inf.comprobar("pipeline/planes_s17.py: sin NotImplementedError ni marcas TODO", pendiente if texto else "no existe", [])
    bit = [b for b in leer_bitacora(raiz / "bitacora_planes.jsonl") if b.get("volumen") == a.volumen]
    por = {c: [b for b in bit if b.get("consulta") == c] for c in CINCO + ["C-aqe"]}
    ultima = {c: (por[c][-1] if por[c] else {}) for c in por}
    # 2 bitacora completa
    faltan = sorted({c for c in por if not por[c]} | {f"{c}:{k}" for c in por for k in CAMPOS_PLANES if por[c] and k not in ultima[c]})
    inf.comprobar(f"bitacora_planes.jsonl ({a.volumen}): seis consultas con todos los campos", faltan, [])
    # 3 hipotesis antes de medir
    ok3 = all(ultima[c] and ultima[c].get("marca_hipotesis") and ultima[c].get("marca_medicion")
              and ultima[c]["marca_hipotesis"] < ultima[c]["marca_medicion"]
              and isinstance(ultima[c].get("hipotesis_archivos_leidos"), int) for c in CINCO)
    inf.comprobar("hipotesis registrada antes de medir en las cinco consultas (con archivos esperados entero)", ok3, True)
    capa = lago / "refinada_spark"
    n_capa = len(list(capa.rglob("*.parquet")))
    A, AF, B, C, CF, CA = (ultima[c] for c in CINCO + ["C-aqe"])
    # 4 a 8
    inf.comprobar("A: PartitionFilters sobre anio, mes y dia", all(k in A.get("partition_filters", "") for k in ("anio", "mes", "dia")), True)
    inf.comprobar("A: archivos leidos", A.get("archivos_leidos"), 1)
    inf.comprobar("A-fecha: sin PartitionFilters y archivos leidos igual a los de la capa",
                  (AF.get("partition_filters", "x"), AF.get("archivos_leidos")), ("", n_capa))
    inf.comprobar("A-fecha: mismas filas de resultado que A", AF.get("filas_resultado"), A.get("filas_resultado"))
    inf.comprobar("B: predicado empujado, sin PartitionFilters y todos los archivos leidos",
                  ("presion_bar" in B.get("pushed_filters", ""), B.get("partition_filters", "x"), B.get("archivos_leidos")), (True, "", n_capa))
    inf.comprobar("C (configuracion por omision): estrategia de union", C.get("estrategia_union"), "BroadcastHashJoin")
    inf.comprobar("C-forzada: umbral -1, SortMergeJoin, al menos 2 Sort y 3 Exchange",
                  (CF.get("umbral_difusion"), CF.get("estrategia_union"), CF.get("n_sort", 0) >= 2, CF.get("n_exchange", 0) >= 3),
                  (-1, "SortMergeJoin", True, True))
    # 9
    ok9 = (all(ultima[c].get("aqe") is False for c in CINCO) and len({ultima[c].get("shuffle_partitions") for c in CINCO}) == 1
           and CA.get("aqe") is True and bool(CA.get("hash_plan_inicial")) and bool(CA.get("hash_plan_final")))
    inf.comprobar("aqe apagada y mismo shuffle_partitions en las cinco; C-aqe con aqe y con hash de plan inicial y final", ok9, True)
    # 10
    inf.comprobar("resultado identico: A con A-fecha, y C con C-forzada y C-aqe",
                  (A.get("hash_resultado") == AF.get("hash_resultado"), len({C.get("hash_resultado"), CF.get("hash_resultado"), CA.get("hash_resultado")}) == 1),
                  (True, True))
    # 11
    difs = {c: ultima[c].get("diferencia_max_independiente") for c in ("A", "B", "C")}
    inf.comprobar("diferencia maxima contra el calculo independiente en A, B y C", {k: f"{v:.1e}" if isinstance(v, float) else v for k, v in difs.items()},
                  f"<= {TOLERANCIA}", all(isinstance(v, (int, float)) and v <= TOLERANCIA for v in difs.values()))
    # 12
    malos = []
    for c in CINCO + ["C-aqe"]:
        ruta_plan = raiz / "planes" / f"plan_{a.volumen}_{c}.txt"
        if not ruta_plan.exists():
            malos.append(f"{c}: falta"); continue
        txt = ruta_plan.read_text(encoding="utf-8")
        if any(re.search(p, txt) for p in PATRONES_PLAN):
            malos.append(f"{c}: identificadores o rutas sin normalizar")
        elif hashlib.sha256(txt.encode("utf-8")).hexdigest() != ultima[c].get("hash_plan_normalizado"):
            malos.append(f"{c}: el hash no coincide con la bitacora")
    inf.comprobar("seis planes normalizados en planes/ con el hash de la bitacora", malos, [])
    # 13
    inestables = [c for c in CINCO if len({b.get("hash_plan_normalizado") for b in por[c]}) != 1 or len(por[c]) < 2]
    inf.comprobar("al menos dos corridas por consulta con el mismo hash del plan", inestables, [])
    # 14
    antes, despues = A.get("hash_datalake_antes"), A.get("hash_datalake_despues")
    inf.comprobar(f"lago heredado intacto ({lago.name}/): hash antes igual al de despues y al recalculado, y referencia/ presente",
                  (antes == despues, despues == hash_arbol_lago(lago), (lago / "referencia" / "medidores.parquet").exists()), (True, True, True))
    # 15
    ent = raiz / "entregables" / "t17"
    def secciones(nombre, patron):
        p = ent / nombre
        return len(re.findall(patron, p.read_text(encoding="utf-8"), re.M)) if p.exists() else -1
    informe = ent / "informe_planes.md"
    t_inf = informe.read_text(encoding="utf-8") if informe.exists() else ""
    dec = ent / "decisiones.md"
    t_dec = dec.read_text(encoding="utf-8") if dec.exists() else ""
    numeros = {int(n) for n in re.findall(r"^## Decisi[oó]n (\d)", t_dec, re.M)}
    costo = (ent / "costo.md").exists()
    indice = raiz / "indice_evidencias.md"
    en_indice = indice.exists() and re.search(r"^\|\s*t17\b", indice.read_text(encoding="utf-8"), re.M | re.I) is not None
    ok15 = (all(f"## Plan {x}" in t_inf for x in "ABC"), len(numeros) >= 3 and {1, 4} <= numeros, costo, en_indice)
    inf.comprobar("entregables: informe con Plan A, B y C; tres decisiones (1 y 4); costo.md; fila t17 en el indice", ok15, (True, True, True, True))
    # 16
    glos = ent / "glosario.md"
    filas = [l for l in glos.read_text(encoding="utf-8").splitlines() if l.startswith("|") and not re.match(r"^\|[\s:-]+\|", l)] if glos.exists() else []
    inf.comprobar("glosario.md: tabla de 8 terminos (mas el encabezado)", len(filas) - 1 if filas else 0, 8)


CAMPOS_COMUNES_T18 = ["marca_medicion", "equipo", "volumen", "capa", "accion", "producto", "enfoque", "spark_version", "aqe",
                      "shuffle_partitions", "hash_resultado"]
CAMPOS_LECTURA_T18 = ["archivos_leidos", "metrica_bytes", "bytes_leidos", "bytes_archivos_seleccionados", "bytes_entrada_etapas",
                      "filas_salida_escaneo", "bytes_escritos", "etapas", "tareas", "bytes_mezcla"]
CAMPOS_REPROCESO_T18 = ["ventana", "sobrescritura", "particiones_antes", "particiones_intactas", "particiones_reescritas", "particiones_perdidas",
                        "hash_sql", "hash_entrada", "hipotesis_valor", "marca_hipotesis"]
EXCLUIDAS_DEL_HASH = ("refinada_spark", "consolidada_spark", "tablero_etl", "consolidada_elt", "_deriva", "_copias_reproceso")
CLASES_PRESION = {"baja", "normal", "alta", "sin_dato"}


def hash_arbol_capa(capa):
    """Hash del arbol de archivos de una capa (ruta relativa y tamano): detecta cualquier archivo agregado, borrado o cambiado."""
    h = hashlib.sha256()
    raiz = Path(capa)
    for f in sorted(raiz.rglob("*")):
        if f.is_file() and not f.name.endswith(".crc"):
            h.update(f"{f.relative_to(raiz).as_posix()}|{f.stat().st_size}\n".encode("utf-8"))
    return h.hexdigest()


def fila_en_tabla(texto, etapa):
    """True si el indice de evidencias tiene la fila de la etapa DENTRO de su tabla (las lineas contiguas que empiezan en '| Etapa').
    Una fila suelta despues del texto no cuenta: no forma parte de la tabla."""
    lineas = texto.splitlines()
    inicio = next((i for i, l in enumerate(lineas) if l.startswith("| Etapa")), None)
    if inicio is None:
        return False
    fin = inicio
    while fin + 1 < len(lineas) and lineas[fin + 1].startswith("|"):
        fin += 1
    return any(re.match(r"^\|\s*" + etapa + r"\b", l, re.I) for l in lineas[inicio:fin + 1])


def _dias_de_ventana(ventana):
    import datetime as dt
    d0, d1 = (dt.date.fromisoformat(x) for x in ventana)
    return (d1 - d0).days + 1


def etapa_t18(a, inf):
    import pandas as pd
    import pyarrow.parquet as pq
    raiz = Path(a.raiz)
    lago = Path(a.carpeta) if a.carpeta else raiz / ("datalake" if a.volumen == "base" else "datalake_equipo")
    # 1 el ejecutor esta completo y existen los SQL y los contratos
    fuente = raiz / "pipeline" / "elt_s18.py"
    texto = fuente.read_text(encoding="utf-8") if fuente.exists() else ""
    pendiente = [m for m in ("NotImplementedError", "TODO") if m in texto]
    sql_c, json_c = sorted((raiz / "sql" / "consolidada").glob("*.sql")), sorted((raiz / "contratos").glob("*.json"))
    for f in sql_c + json_c:
        pendiente += [f"{f.name}: TODO" for m in ("TODO",) if m in f.read_text(encoding="utf-8")]
    inf.comprobar("pipeline/elt_s18.py, los SQL y los contratos sin NotImplementedError ni TODO; dos SQL en sql/consolidada y dos contratos",
                  (pendiente if texto else "no existe", len(sql_c), len(json_c)), ([], 2, 2))
    bit = [b for b in leer_bitacora(raiz / "bitacora_capas.jsonl") if b.get("volumen") == a.volumen]

    def ultima(**kw):
        sel = [b for b in bit if all(b.get(k) == v for k, v in kw.items())]
        return sel[-1] if sel else {}

    def todas(**kw):
        return [b for b in bit if all(b.get(k) == v for k, v in kw.items())]

    clases = {"ETL consumo": ultima(accion="construir", enfoque="ETL", producto="consumo"),
              "ETL presion": ultima(accion="construir", enfoque="ETL", producto="presion"),
              "ETL-una-pasada": ultima(accion="construir", enfoque="ETL-una-pasada"),
              "ETL-ventana": ultima(accion="construir", enfoque="ETL-ventana"),
              "ELT consumo": ultima(accion="construir", enfoque="ELT", producto="consumo"),
              "ELT presion": ultima(accion="construir", enfoque="ELT", producto="presion"),
              "reproceso": ultima(accion="reproceso", sobrescritura="dinamica"),
              "contrato": ultima(accion="contrato")}
    # 2 bitacora completa
    faltan = []
    for nombre, b in clases.items():
        if not b:
            faltan.append(f"{nombre}: sin lineas")
            continue
        req = CAMPOS_COMUNES_T18 + (CAMPOS_REPROCESO_T18 if nombre == "reproceso" else [] if nombre == "contrato" else CAMPOS_LECTURA_T18)
        faltan += [f"{nombre}:{k}" for k in req if k not in b]
    inf.comprobar("bitacora_capas.jsonl: ETL (consumo, presion, una pasada, ventana), ELT (consumo, presion), reproceso y contrato con todos los campos", faltan, [])
    # 3 hipotesis antes de medir
    tres = [clases["ELT consumo"], clases["ELT presion"], clases["reproceso"]]
    ok3 = all(b and b.get("marca_hipotesis") and b.get("marca_medicion") and b["marca_hipotesis"] < b["marca_medicion"]
              and isinstance(b.get("hipotesis_valor"), (int, float)) and not isinstance(b.get("hipotesis_valor"), bool) for b in tres)
    inf.comprobar("hipotesis numerica registrada antes de medir (bytes de columnas que leera el producto ELT de consumo, bytes de columnas que leera el producto ELT de presion, particiones que reescribira el reproceso)", ok3, True)
    # datos de referencia
    elt = lago / "consolidada_elt"
    consumo = pd.concat([pd.read_parquet(f) for f in sorted((elt / "consumo_dia_sector").rglob("*.parquet"))], ignore_index=True) if (elt / "consumo_dia_sector").exists() else pd.DataFrame()
    presion = pd.concat([pd.read_parquet(f) for f in sorted((elt / "presion_dia_sector").rglob("*.parquet"))], ignore_index=True) if (elt / "presion_dia_sector").exists() else pd.DataFrame()
    for t in (consumo, presion):
        if len(t):
            t["fecha"] = t["fecha"].astype(str)
    archivos_prep = sorted((lago / "refinada_spark").rglob("*.parquet"))
    filas_prep = sum(pq.ParquetFile(f).metadata.num_rows for f in archivos_prep)
    if a.volumen == "base":
        import re as _re
        partes = []
        for f in glob.glob(str(raiz / "datalake/consolidada/acueducto/anio=*/mes=*/dia=*/consumo_diario.parquet")):
            d = pd.read_parquet(f)
            d["fecha"] = _re.sub(r".*anio=(\d+).*mes=(\d+).*dia=(\d+).*", r"\1-\2-\3", f.replace("\\", "/"))
            partes.append(d)
        ref = pd.concat(partes, ignore_index=True)
    else:
        ref = pd.read_csv(lago / "totales_control.csv")
    ref = ref[["fecha", "sector", "consumo_total_m3", "n_lecturas"]]
    ref["fecha"] = ref["fecha"].astype(str)
    # 4 forma del producto de consumo
    rutas_ok = all(re.search(r"anio=\d{4}[/\\]mes=\d{2}[/\\]dia=\d{2}$", str(p)) for p in (elt / "consumo_dia_sector").glob("anio=*/mes=*/dia=*"))
    inf.comprobar("consolidada_elt/consumo_dia_sector: filas del grano de la referencia, clave (fecha, sector) unica y rutas con ceros",
                  (len(consumo), int(consumo.duplicated(["fecha", "sector"]).sum()) if len(consumo) else -1, rutas_ok), (len(ref), 0, True))
    # 5 sin filtro: reconcilia con la preparacion y con la referencia
    m = consumo.merge(ref, on=["fecha", "sector"], suffixes=("", "_ref")) if len(consumo) else pd.DataFrame()
    dif = float((m["consumo_total_m3"] - m["consumo_total_m3_ref"]).abs().max()) if len(m) else float("inf")
    inf.comprobar("sin filtro: suma de n_lecturas igual a las filas de refinada_spark/ y a la referencia; diferencia maxima de consumo",
                  (int(consumo["n_lecturas"].sum()) if len(consumo) else -1, int(len(m)), f"{dif:.1e}"),
                  (filas_prep, len(ref), f"<= {TOLERANCIA}"),
                  ok=len(consumo) > 0 and int(consumo["n_lecturas"].sum()) == filas_prep == int(ref["n_lecturas"].sum()) and len(m) == len(ref) and dif <= TOLERANCIA)
    # 6 filtrado: el ELT reproduce al ETL (consumo y presion) y la una pasada coincide con el ETL por producto
    etl_c = pd.concat([pd.read_parquet(f) for f in sorted((lago / "tablero_etl" / "consumo").glob("*.parquet"))], ignore_index=True) if (lago / "tablero_etl" / "consumo").exists() else pd.DataFrame()
    etl_p = pd.concat([pd.read_parquet(f) for f in sorted((lago / "tablero_etl" / "presion").glob("*.parquet"))], ignore_index=True) if (lago / "tablero_etl" / "presion").exists() else pd.DataFrame()
    ok6, det6 = False, "falta tablero_etl o consolidada_elt"
    if len(etl_c) and len(etl_p) and len(consumo) and len(presion):
        etl_c["fecha"] = etl_c["fecha"].astype(str)
        etl_p["fecha"] = etl_p["fecha"].astype(str)
        mc = etl_c.merge(consumo, on=["fecha", "sector"], how="outer", suffixes=("_etl", "_elt"), indicator=True)
        mp = etl_p.merge(presion, on=["fecha", "sector", "nivel_presion"], how="outer", suffixes=("_etl", "_elt"), indicator=True)
        solo_c, solo_p = int((mc["_merge"] != "both").sum()), int((mp["_merge"] != "both").sum())
        d_n = int((mc["n_lecturas_etl"] != mc["n_confiables"]).sum())
        d_s = float((mc["consumo_total_m3_etl"] - mc["consumo_confiable_m3"]).abs().max())
        d_p = int((mp["n_lecturas_etl"] != mp["n_confiables"]).sum())
        hu = clases["ETL-una-pasada"].get("hash_resultado") or {}
        h_ok = isinstance(hu, dict) and hu.get("consumo") == clases["ETL consumo"].get("hash_resultado") and hu.get("presion") == clases["ETL presion"].get("hash_resultado")
        ok6 = solo_c == 0 and solo_p == 0 and d_n == 0 and d_s <= TOLERANCIA and d_p == 0 and h_ok
        det6 = (f"consumo: {len(mc)} grupos, sin par {solo_c}, conteos distintos {d_n}, dif max {d_s:.1e} · presion: {len(mp)} grupos, sin par {solo_p}, "
                f"conteos distintos {d_p} · una pasada igual al ETL por producto: {h_ok}")
    inf.comprobar("ELT filtrado igual al ETL en consumo y presion; ETL de una pasada igual al ETL por producto", det6, "todo igual", ok6)
    # 7 invariante de los conteos y lector independiente
    if len(consumo):
        malas = int((consumo["n_confiables"] + consumo["n_no_confiables"] != consumo["n_lecturas"]).sum())
        no_conf = 0
        for f in archivos_prep:
            no_conf += int((~pq.read_table(str(f), columns=["es_confiable"]).column("es_confiable").to_pandas()).sum())
        inf.comprobar("n_confiables + n_no_confiables = n_lecturas en todas las filas; suma de no confiables igual a la contada con un lector independiente",
                      (malas, int(consumo["n_no_confiables"].sum())), (0, no_conf))
    else:
        inf.comprobar("consolidada_elt/consumo_dia_sector existe", "no existe", "existe")
    # 8 presion
    if len(presion):
        conf = 0
        for f in archivos_prep:
            conf += int(pq.read_table(str(f), columns=["es_confiable"]).column("es_confiable").to_pandas().sum())
        obs8 = (int(presion.duplicated(["fecha", "sector", "nivel_presion"]).sum()), sorted(set(presion["nivel_presion"]) - CLASES_PRESION),
                int(presion["n_lecturas"].sum()), int(presion["n_confiables"].sum()), int((presion["n_confiables"] + presion["n_no_confiables"] != presion["n_lecturas"]).sum()))
        inf.comprobar("presion_dia_sector: grano unico, clases validas, suma de lecturas = filas de la preparacion, suma de confiables = es_confiable verdadero, conteos consistentes",
                      obs8, (0, [], filas_prep, conf, 0))
    else:
        inf.comprobar("consolidada_elt/presion_dia_sector existe", "no existe", "existe")
    # 9 contratos: estructura minima y aprobacion sobre las capas del proyecto
    import sys as _sys
    _sys.path.insert(0, str(raiz / "pipeline"))
    import elt_s18 as E
    estructura = []
    for ruta in sorted((raiz / "contratos").glob("*.json")):
        c = json.loads(ruta.read_text(encoding="utf-8"))
        recon = [i for i in c.get("invariantes", []) if i.get("tipo") in ("conteo_igual", "suma_igual")]
        if not (c.get("columnas") and c.get("grano") and len(c.get("invariantes", [])) >= 3 and recon):
            estructura.append(ruta.name)
    aprob = []
    try:
        r1 = E.verificar_contrato(raiz / "contratos" / "preparacion_a_consolidada.json", E.tablas_preparacion_a_consolidada(lago / "refinada_spark", elt / "consumo_dia_sector"), lanzar=False)
        r2 = E.verificar_contrato(raiz / "contratos" / "cruda_a_preparacion.json", E.tablas_cruda_a_preparacion(lago / "refinada_spark", lago / "cruda" / "acueducto"), lanzar=False)
        aprob = [x["invariante"] for x in r1 + r2 if not x["ok"]]
    except Exception as e:
        aprob = [f"error: {type(e).__name__}"]
    inf.comprobar("contratos con columnas, grano, al menos tres invariantes y uno de reconciliacion; ambos se aprueban sobre las capas del proyecto",
                  (estructura, aprob), ([], []))
    # 10 el contrato de la frontera cruda_a_preparacion fallo ante la alteracion
    cf = [b for b in todas(accion="contrato") if b.get("frontera") == "cruda_a_preparacion" and b.get("aprobado") is False]
    c10 = cf[-1] if cf else {}
    inf.comprobar("bitacora: contrato cruda_a_preparacion que fallo, con el invariante, evaluado sobre la preparacion de la copia alterada",
                  (bool(c10), c10.get("invariante_que_falla"), c10.get("origen")), (True, "no nulo", "_deriva/refinada_spark"),
                  ok=bool(c10) and bool(c10.get("invariante_que_falla")) and str(c10.get("origen", "")).endswith("_deriva/refinada_spark"))
    # 11 reproceso
    r = clases["reproceso"]
    total_part = r.get("particiones_antes", -1)
    dias = _dias_de_ventana(r["ventana"]) if r.get("ventana") else -1
    inf.comprobar("reproceso en modo dinamico: particiones reescritas = dias de la ventana, perdidas = 0, intactas = total menos la ventana",
                  (r.get("particiones_reescritas"), r.get("particiones_perdidas"), r.get("particiones_intactas")), (dias, 0, total_part - dias))
    # 12 idempotencia
    rr = [b for b in todas(accion="reproceso", sobrescritura="dinamica") if r and b.get("ventana") == r.get("ventana")]
    hashes = {b.get("hash_resultado") for b in rr}
    inf.comprobar("idempotencia: al menos dos corridas del reproceso de la misma ventana con el mismo hash del resultado",
                  (len(rr), len(hashes)), ">= 2 corridas, 1 hash", ok=len(rr) >= 2 and len(hashes) == 1 and None not in hashes)
    # 13 planes de los productos ELT
    malos = []
    for prod in ("consumo", "presion"):
        ruta_plan = raiz / "planes_s18" / f"plan_{a.volumen}_{prod}.txt"
        lineas = todas(accion="construir", enfoque="ELT", producto=prod)
        if not ruta_plan.exists():
            malos.append(f"{prod}: falta el plan")
            continue
        txt = ruta_plan.read_text(encoding="utf-8")
        if any(re.search(p, txt) for p in PATRONES_PLAN):
            malos.append(f"{prod}: identificadores o rutas sin normalizar")
        h = hashlib.sha256(txt.encode("utf-8")).hexdigest()
        if len(lineas) < 2 or any(b.get("hash_plan_normalizado") != h for b in lineas[-2:]):
            malos.append(f"{prod}: el hash no coincide en dos corridas")
    inf.comprobar("planes de los productos ELT normalizados y con el hash de la bitacora en dos corridas", malos, [])
    # 14 comparacion.csv
    cmp_ = raiz / "comparacion.csv"
    fallas14 = []
    if cmp_.exists():
        c = pd.read_csv(cmp_)
        for col in ("producto", "enfoque", "metrica_bytes", "bytes_leidos", "bytes_escritos", "filas_descartadas_visibles"):
            if col not in c.columns:
                fallas14.append(f"falta {col}")
        if not fallas14:
            if c[["producto", "enfoque"]].duplicated().any():
                fallas14.append("filas repetidas")
            if c["metrica_bytes"].isna().any():
                fallas14.append("metrica_bytes sin declarar")
            if not pd.to_numeric(c["bytes_leidos"], errors="coerce").notna().all():
                fallas14.append("bytes_leidos no numerico")
    else:
        fallas14.append("falta comparacion.csv")
    inf.comprobar("comparacion.csv: una fila por (producto, enfoque), metrica de bytes declarada y valores numericos", fallas14, [])
    # 15 integridad
    it = ultima(accion="integridad")
    ok15 = (bool(it) and it.get("hash_datalake_antes") == it.get("hash_datalake_despues") == hash_arbol_lago(lago)
            and it.get("hash_refinada_spark_antes") == it.get("hash_refinada_spark_despues") == hash_arbol_capa(lago / "refinada_spark"))
    inf.comprobar(f"lago heredado intacto ({lago.name}/): hash antes = despues = recalculado con las exclusiones extendidas, y refinada_spark/ con el mismo arbol",
                  ok15, True)
    # 16 a 18 entregables
    ent = raiz / "entregables" / "t18"
    dec = ent / "decisiones.md"
    t_dec = dec.read_text(encoding="utf-8") if dec.exists() else ""
    numeros = {int(n) for n in re.findall(r"^## Decisi[oó]n (\d)", t_dec, re.M)}
    indice = raiz / "indice_evidencias.md"
    en_indice = indice.exists() and fila_en_tabla(indice.read_text(encoding="utf-8"), "t18")
    inf.comprobar("entregables: decisiones.md con al menos tres decisiones (1, 2 y 4); costo.md; fila t18 dentro de la tabla del indice",
                  (len(numeros) >= 3 and {1, 2, 4} <= numeros, (ent / "costo.md").exists(), en_indice), (True, True, True))
    glos = ent / "glosario.md"
    filas = [l for l in glos.read_text(encoding="utf-8").splitlines() if l.startswith("|") and not re.match(r"^\|[\s:-]+\|", l)] if glos.exists() else []
    inf.comprobar("glosario.md: tabla de 8 terminos (mas el encabezado)", len(filas) - 1 if filas else 0, 8)
    ficha = ent / "ficha_capa.md"
    t_f = ficha.read_text(encoding="utf-8") if ficha.exists() else ""
    palabras = len(re.findall(r"\w+", t_f))
    apartados = [s for s in ("Qué contiene", "De dónde viene", "Qué se garantiza", "Qué no se garantiza", "Por qué hay dos cifras", "Cómo pedir un recálculo") if s.lower() in t_f.lower()]
    inf.comprobar("ficha_capa.md: seis apartados y de 150 a 450 palabras", (len(apartados), palabras), "6 apartados, 150 a 450 palabras",
                  ok=len(apartados) == 6 and 150 <= palabras <= 450)


ETAPAS = {"t12": etapa_t12, "t13": etapa_t13, "t14": etapa_t14, "t15": etapa_t15, "t16": etapa_t16, "t17": etapa_t17, "t18": etapa_t18}


def main(argv=None):
    ap = argparse.ArgumentParser(description="Verificacion por etapa del pipeline del acueducto.")
    ap.add_argument("--etapa", required=True, choices=list(ETAPAS), help="Etapa a verificar (t12 a t18)")
    ap.add_argument("--raiz", default=".", help="Carpeta raiz del paquete (donde estan datalake/ y pipeline/)")
    ap.add_argument("--nombre-contenedor", default="acueducto_postgres_s12", help="Contenedor de PostgreSQL (t12 a t14)")
    ap.add_argument("--host", default="127.0.0.1", help="Host de PostgreSQL (t12 a t14)")
    ap.add_argument("--puerto", default="5432", help="Puerto de PostgreSQL (t12 a t14)")
    ap.add_argument("--volumen", choices=["base", "ampliacion"], default="ampliacion", help="Volumen cuyas lineas de bitacora se verifican (t17 y t18)")
    ap.add_argument("--carpeta", default=None, help="Carpeta del lago (t17 y t18; por omision datalake/ o datalake_equipo/ segun el volumen)")
    a = ap.parse_args(argv)
    inicio = time.time()
    inf = Informe(a.etapa)
    print(f"Verificando la etapa {a.etapa} en {Path(a.raiz).resolve()}\n")
    ETAPAS[a.etapa](a, inf)
    return inf.cerrar(inicio)


if __name__ == "__main__":
    raise SystemExit(main())
