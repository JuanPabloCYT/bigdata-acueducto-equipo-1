"""Tablas de referencia del lago del acueducto en Parquet: medidores y sectores.

Que hace
    Las dimensiones del modelo estrella (dim_sector y dim_medidor) viven en PostgreSQL, pero
    para estudiar uniones con Spark se necesitan como ARCHIVOS junto a las lecturas. Este script
    escribe, sin levantar PostgreSQL, una carpeta referencia/ con tres tablas pequenas:

      referencia/medidores.parquet            una fila por medidor (200 en el lago base)
          columnas: meter_id, sector, estrato, fecha_inicio, fecha_fin, es_actual
          Es la vista COHERENTE CON LAS LECTURAS: cada medidor figura con el sector con el que
          aparece en cruda/, vigente desde el inicio de la historia. Sirve para unir lecturas con
          medidores sin multiplicar filas.
      referencia/medidores_con_historia.parquet   una fila por VERSION de medidor (201 en el lago base)
          La dimension de medidores tal cual: un medidor que cambio de sector figura dos veces.
          Sirve para ver que ocurre al unir por una clave que no es unica.
      referencia/sectores.parquet             4 filas: sector, estrato, codigo_dimension, nombre_dimension

    El estrato va DENORMALIZADO en medidores.parquet (se copia desde sectores.parquet): asi una
    consulta que agrupa por estrato necesita una sola union. La copia es segura porque el script
    comprueba que cada estrato coincide con el de su sector.

Los nombres de sector no coinciden entre fuentes
    La dimension de sectores identifica el primer sector como SEC1 con el nombre S-NORTE-CENTRO,
    mientras que las lecturas y los medidores dicen S-NORTE. Una union por nombre no encontraria
    ese sector. El script lleva una tabla de traduccion explicita (codigo de la dimension a
    codigo de la lectura) y comprueba que los cuatro sectores traducidos son exactamente los
    cuatro que aparecen en las lecturas.

De donde salen los datos
    Lago base (datalake/): las dimensiones se leen de los datos que carga PostgreSQL en
    perfiles/s12/postgres/init/01_esquema_dimensional.sql (bloques COPY de dim_sector y
    dim_medidor), de modo que hay una sola fuente de verdad. Ampliacion del equipo
    (datalake_equipo/): no existe una dimension para sus medidores MEQ-xxxxxxx; la referencia se
    deduce de las lecturas y el estrato, del sector. En medidores_con_historia.parquet de la
    ampliacion, la segunda version del primer medidor es SINTETICA: replica el patron del lago
    base (cambia al sector siguiente el 2024-03-01) y se rotula asi en el metadato del archivo.

Como usarlo (desde la carpeta raiz del paquete, dentro del contenedor del servicio spark)
    python practica/generar_referencia_s17.py                         # lago base: datalake/referencia/
    python practica/generar_referencia_s17.py --equipo equipo-3       # datalake_equipo/referencia/

Reproducibilidad
    No hay aleatoriedad: dos corridas producen archivos identicos byte a byte con la misma version
    de pyarrow. La semilla del proyecto (42) se declara por convencion y no se usa.

Ejercicio didactico y produccion
    Aqui la referencia pesa unos kilobytes y se regenera en segundos. En produccion una dimension
    se carga desde su sistema de origen con claves subrogadas y control de versiones, y la union
    se disena para que la clave de la dimension sea unica en el momento de la lectura.
"""
import argparse
import hashlib
import json
import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

SEED = 42  # semilla del proyecto; este script es determinista y no la usa
INICIO_HISTORIA = date(2024, 1, 1)
CAMBIO_PRIMER_MEDIDOR = date(2024, 3, 1)
ORDEN_SECTORES = ["S-NORTE", "S-SUR", "S-ESTE", "S-OESTE"]
# Traduccion explicita: codigo de la dimension de sectores -> codigo con el que aparece en las lecturas
TRADUCCION_SECTORES = {"SEC1": "S-NORTE", "SEC2": "S-SUR", "SEC3": "S-ESTE", "SEC4": "S-OESTE"}
SQL_DIMENSIONES = Path("perfiles/s12/postgres/init/01_esquema_dimensional.sql")
NOTA_SINTETICA = "La segunda version del primer medidor es sintetica: replica el patron del lago base"


def leer_bloque_copy(ruta_sql, tabla):
    """Lee las filas del bloque COPY <tabla> ... FROM stdin; hasta la linea \\. del script SQL."""
    filas, dentro = [], False
    with open(ruta_sql, encoding="utf-8") as f:
        for linea in f:
            if not dentro:
                if linea.startswith(f"COPY {tabla} "):
                    dentro = True
                continue
            if linea.strip() == "\\.":
                break
            filas.append(linea.rstrip("\n").split("\t"))
    return filas


def leer_dimensiones(raiz):
    """(dim_sector, dim_medidor) como DataFrames, desde el SQL que carga PostgreSQL."""
    ruta = Path(raiz) / SQL_DIMENSIONES
    if not ruta.exists():
        raise SystemExit(f"No se encuentra {ruta}: ejecute el script desde la carpeta raiz del paquete.")
    sec = pd.DataFrame(leer_bloque_copy(ruta, "dim_sector"),
                       columns=["sk_sector", "codigo_sector", "nombre_sector", "estrato"])
    sec["estrato"] = sec["estrato"].astype(int)
    med = pd.DataFrame(leer_bloque_copy(ruta, "dim_medidor"),
                       columns=["sk_medidor", "codigo_medidor", "sector_asignado", "fecha_inicio", "fecha_fin", "es_actual"])
    return sec, med


def medidores_de_las_lecturas(carpeta_cruda):
    """meter_id -> sector, leido de TODOS los CSV de la capa cruda. Un medidor con dos sectores
    en las lecturas haria ambigua la referencia: se detiene."""
    archivos = sorted(Path(carpeta_cruda).glob("anio=*/mes=*/dia=*/lecturas.csv"))
    if not archivos:
        raise SystemExit(f"No hay archivos CSV en {carpeta_cruda}")
    partes = [pd.read_csv(a, usecols=["meter_id", "sector"]).drop_duplicates() for a in archivos]
    todo = pd.concat(partes, ignore_index=True).drop_duplicates()
    repetidos = todo[todo.duplicated("meter_id", keep=False)]
    assert repetidos.empty, f"medidores con mas de un sector en las lecturas: {sorted(repetidos['meter_id'].unique())[:5]}"
    return todo.sort_values("meter_id").reset_index(drop=True), len(archivos)


def tabla_sectores(dim_sector, sectores_lecturas):
    """sectores.parquet: traduce los codigos de la dimension y comprueba el conjunto contra las lecturas."""
    s = dim_sector.copy()
    s["sector"] = s["codigo_sector"].map(TRADUCCION_SECTORES)
    assert s["sector"].notna().all(), "un codigo de la dimension no tiene traduccion"
    assert set(s["sector"]) == set(sectores_lecturas), (
        f"los sectores traducidos {sorted(s['sector'])} no coinciden con los de las lecturas {sorted(sectores_lecturas)}")
    s = s.rename(columns={"codigo_sector": "codigo_dimension", "nombre_sector": "nombre_dimension"})
    return s[["sector", "estrato", "codigo_dimension", "nombre_dimension"]].sort_values("sector").reset_index(drop=True)


def tabla_medidores(lecturas_meter, sectores):
    """medidores.parquet: una fila por medidor, con el sector de las lecturas y el estrato de ese sector."""
    m = lecturas_meter.merge(sectores[["sector", "estrato"]], on="sector", how="left", validate="m:1")
    assert m["estrato"].notna().all(), "un medidor quedo sin estrato"
    m["fecha_inicio"] = INICIO_HISTORIA
    m["fecha_fin"] = pd.NaT
    m["es_actual"] = True
    return m[["meter_id", "sector", "estrato", "fecha_inicio", "fecha_fin", "es_actual"]]


def tabla_con_historia_base(medidores, dim_medidor, sectores):
    """Dimension tal cual: 201 filas, un medidor con dos versiones. Mismas columnas que medidores.parquet."""
    d = dim_medidor.rename(columns={"codigo_medidor": "meter_id", "sector_asignado": "sector"}).copy()
    d["fecha_inicio"] = pd.to_datetime(d["fecha_inicio"]).dt.date
    d["fecha_fin"] = pd.to_datetime(d["fecha_fin"].replace("\\N", None)).dt.date
    d["es_actual"] = d["es_actual"].map({"t": True, "f": False})
    d = d.merge(sectores[["sector", "estrato"]], on="sector", how="left", validate="m:1")
    d = d[["meter_id", "sector", "estrato", "fecha_inicio", "fecha_fin", "es_actual"]]
    # los medidores sin cambio coinciden con la tabla de una fila por medidor
    vigentes_unicos = d[~d["meter_id"].duplicated(keep=False)].set_index("meter_id")
    esperado = medidores.set_index("meter_id").loc[vigentes_unicos.index]
    assert (vigentes_unicos["sector"] == esperado["sector"]).all(), "la dimension y las lecturas discrepan en un medidor sin cambio"
    return d.sort_values(["meter_id", "fecha_inicio"]).reset_index(drop=True)


def tabla_con_historia_ampliacion(medidores):
    """Ampliacion: no existe dimension. Se agrega UNA version sintetica del primer medidor."""
    d = medidores.copy()
    primero = d["meter_id"].iloc[0]
    sector0 = d.loc[d["meter_id"] == primero, "sector"].iloc[0]
    siguiente = ORDEN_SECTORES[(ORDEN_SECTORES.index(sector0) + 1) % 4]
    estrato_sig = int(medidores.loc[medidores["sector"] == siguiente, "estrato"].iloc[0])
    d.loc[d["meter_id"] == primero, ["fecha_fin", "es_actual"]] = [CAMBIO_PRIMER_MEDIDOR, False]
    nueva = pd.DataFrame([{"meter_id": primero, "sector": siguiente, "estrato": estrato_sig,
                           "fecha_inicio": CAMBIO_PRIMER_MEDIDOR, "fecha_fin": pd.NaT, "es_actual": True}])
    return pd.concat([d, nueva], ignore_index=True).sort_values(["meter_id", "fecha_inicio"]).reset_index(drop=True)


def a_arrow(df, nota=None):
    """DataFrame -> tabla Arrow con tipos explicitos (fechas como date32, estrato como int32)."""
    esquema = pa.schema([
        ("meter_id", pa.string()), ("sector", pa.string()), ("estrato", pa.int32()),
        ("fecha_inicio", pa.date32()), ("fecha_fin", pa.date32()), ("es_actual", pa.bool_())])
    if "codigo_dimension" in df.columns:
        esquema = pa.schema([("sector", pa.string()), ("estrato", pa.int32()),
                             ("codigo_dimension", pa.string()), ("nombre_dimension", pa.string())])
    cols = {}
    for campo in esquema:
        serie = df[campo.name]
        if pa.types.is_date32(campo.type):
            cols[campo.name] = pa.array([None if pd.isna(v) else v for v in serie], type=pa.date32())
        else:
            cols[campo.name] = pa.array(serie.tolist(), type=campo.type)
    t = pa.table(cols, schema=esquema)
    if nota:
        t = t.replace_schema_metadata({"nota": nota})
    return t


def escribir(df, ruta, nota=None):
    pq.write_table(a_arrow(df, nota), ruta, compression="snappy")
    return Path(ruta).stat().st_size


def hash_archivo(ruta):
    return hashlib.sha256(Path(ruta).read_bytes()).hexdigest()


def main(argv=None):
    ap = argparse.ArgumentParser(description="Escribe referencia/ (medidores y sectores) en Parquet.")
    ap.add_argument("--equipo", default=None, help="Si se indica (equipo-1 a equipo-6), usa datalake_equipo/ (hay que generar antes la ampliacion)")
    ap.add_argument("--carpeta", default=None, help="Carpeta del lago a usar (por omision datalake/ o datalake_equipo/)")
    ap.add_argument("--raiz", default=".", help="Carpeta raiz del paquete")
    a = ap.parse_args(argv)
    inicio = time.time()
    raiz = Path(a.raiz)
    ampliacion = a.equipo is not None or a.carpeta is not None and "equipo" in a.carpeta
    lago = Path(a.carpeta) if a.carpeta else raiz / ("datalake_equipo" if a.equipo else "datalake")
    if not (lago / "cruda").exists():
        raise SystemExit(f"No existe {lago / 'cruda'}: genere antes la ampliacion de su equipo." if a.equipo else f"No existe {lago / 'cruda'}")

    dim_sector, dim_medidor = leer_dimensiones(raiz)
    lect, n_archivos = medidores_de_las_lecturas(lago / "cruda" / "acueducto")
    sectores = tabla_sectores(dim_sector, set(lect["sector"]))
    medidores = tabla_medidores(lect, sectores)
    con_historia = tabla_con_historia_ampliacion(medidores) if ampliacion else tabla_con_historia_base(medidores, dim_medidor, sectores)

    # aserciones de consistencia
    assert medidores["meter_id"].is_unique, "medidores.parquet tiene meter_id repetidos"
    est_sector = sectores.set_index("sector")["estrato"]
    assert (medidores["estrato"].values == est_sector.loc[medidores["sector"]].values).all(), "estrato de un medidor distinto al de su sector"
    capa = lago / "refinada_spark"
    if capa.exists() and any(capa.rglob("*.parquet")):
        ids_capa = set(pq.read_table(str(capa), columns=["meter_id"]).column("meter_id").to_pylist())
        assert ids_capa == set(medidores["meter_id"]), "los medidores de la referencia no coinciden con los de refinada_spark/"
    assert con_historia["meter_id"].nunique() == len(medidores) and len(con_historia) == len(medidores) + 1

    destino = lago / "referencia"
    destino.mkdir(parents=True, exist_ok=True)
    tam = {
        "medidores.parquet": escribir(medidores, destino / "medidores.parquet"),
        "medidores_con_historia.parquet": escribir(con_historia, destino / "medidores_con_historia.parquet",
                                                  NOTA_SINTETICA if ampliacion else None),
        "sectores.parquet": escribir(sectores, destino / "sectores.parquet"),
    }
    print(f"Referencia escrita en {destino} ({time.time() - inicio:.1f} s)")
    print(f"  fuente de los medidores: {n_archivos} archivos CSV de cruda/ · fuente de los sectores: dim_sector de {SQL_DIMENSIONES}")
    print(f"  medidores.parquet: {len(medidores)} filas · medidores_con_historia.parquet: {len(con_historia)} filas · sectores.parquet: {len(sectores)} filas")
    for k, v in tam.items():
        print(f"  {k}: {v} B")
    primero = con_historia[con_historia["meter_id"].duplicated(keep=False)]
    print("  medidor con dos versiones:")
    print(primero.to_string(index=False))
    if ampliacion:
        print(f"  NOTA: {NOTA_SINTETICA} (version sintetica rotulada en el metadato del archivo)")
    print(json.dumps({"hash_medidores": hash_archivo(destino / 'medidores.parquet')[:16],
                      "hash_con_historia": hash_archivo(destino / 'medidores_con_historia.parquet')[:16],
                      "hash_sectores": hash_archivo(destino / 'sectores.parquet')[:16]}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
