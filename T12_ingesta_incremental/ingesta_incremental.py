"""Ingesta incremental idempotente de lecturas del acueducto (tarea T12).

Carga un lote diario en stg_lecturas por sector, en una sola transaccion por
sector: filtra por la marca de agua de estado_ingesta, inserta escribiendo por
clave (ON CONFLICT (meter_id, "timestamp") DO NOTHING) y mueve la marca con
GREATEST para que nunca retroceda. Con --reprocesar reabre una ventana ya
cargada: inserta el lote sin el filtro de la marca y no toca estado_ingesta.

Uso tipico (desde la carpeta practica/, con el perfil s12 levantado):

    # 1) Generar la instancia del equipo
    python generar_datos_S12.py --equipo equipo-1 --dias 2 --inicio 2024-06-01 --salida lotes_equipo-1

    # 2) Prueba de doble ejecucion: estado en cero, misma corrida dos veces y comparacion
    python ingesta_incremental.py --equipo equipo-1 --dia 1 --reiniciar --doble

    # 3) Corridas sueltas: la misma linea dos veces deja el mismo conteo
    python ingesta_incremental.py --equipo equipo-1 --dia 1

La base de cada equipo es acueducto_<equipo> (por ejemplo acueducto_equipo_1),
la misma que usa el cuaderno de la practica. La contrasena postgres es
exclusivamente didactica.
"""
import argparse
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

from generar_datos_S12 import EQUIPOS_VALIDOS, SECTORES

URL_POSTGRES = "postgresql+psycopg2://postgres:postgres@localhost:5432/{bd}"  # credenciales didacticas
MARCA_CENTINELA = "2000-01-01 00:00:00"

DDL_STG = '''
CREATE TABLE IF NOT EXISTS stg_lecturas (
    meter_id VARCHAR,
    "timestamp" TIMESTAMP,
    sector VARCHAR,
    consumo_m3 DOUBLE PRECISION,
    presion_bar DOUBLE PRECISION,
    quality_flag VARCHAR,
    PRIMARY KEY (meter_id, "timestamp")
);
'''
DDL_ESTADO = '''
CREATE TABLE IF NOT EXISTS estado_ingesta (
    sector VARCHAR PRIMARY KEY,
    ultima_marca_agua TIMESTAMP,
    ultima_ejecucion TIMESTAMP,
    filas_cargadas INTEGER
);
'''


def construir_engine(equipo):
    """Engine de la base propia del equipo; la crea si no existe."""
    nombre_bd = f"acueducto_{equipo.replace('-', '_')}"
    admin = create_engine(URL_POSTGRES.format(bd="postgres"), isolation_level="AUTOCOMMIT")
    try:
        with admin.connect() as conn:
            existe = conn.execute(text("SELECT 1 FROM pg_database WHERE datname = :n"), {"n": nombre_bd}).scalar()
            if not existe:
                conn.execute(text(f'CREATE DATABASE "{nombre_bd}"'))
    except Exception as e:
        raise SystemExit(f"No se pudo conectar a PostgreSQL ({type(e).__name__}). "
                         "Levanten el perfil con 'docker compose --profile s12 up -d'.") from e
    finally:
        admin.dispose()
    return create_engine(URL_POSTGRES.format(bd=nombre_bd)), nombre_bd


def preparar_esquema(engine):
    """Crea las tablas si no existen y siembra la marca centinela de cada sector (nunca NULL)."""
    with engine.begin() as conn:
        conn.execute(text(DDL_STG))
        conn.execute(text(DDL_ESTADO))
        for sector in SECTORES:
            conn.execute(text(f'''
                INSERT INTO estado_ingesta (sector, ultima_marca_agua, ultima_ejecucion, filas_cargadas)
                VALUES (:sector, TIMESTAMP '{MARCA_CENTINELA}', NULL, 0)
                ON CONFLICT (sector) DO NOTHING
            '''), {"sector": sector})


def reiniciar_estado(engine):
    """DESTRUCTIVA: vacia stg_lecturas y devuelve las marcas al centinela. Solo para repetir la prueba."""
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM stg_lecturas"))
        conn.execute(text(f"UPDATE estado_ingesta SET ultima_marca_agua = TIMESTAMP '{MARCA_CENTINELA}', "
                          "ultima_ejecucion = NULL, filas_cargadas = 0"))


def ingest_lote(engine, df_lote, sector):
    """Inserta el lote de un sector y actualiza su marca de agua en una sola
    transaccion: o se cargan los datos y se actualiza el estado, o nada."""
    df_sector = df_lote[df_lote["sector"] == sector].copy()
    if df_sector.empty:
        return 0
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS lote_entrante"))
        df_sector.to_sql("lote_entrante", conn, if_exists="replace", index=False)
        antes = conn.execute(text("SELECT COUNT(*) FROM stg_lecturas WHERE sector = :s"), {"s": sector}).scalar()
        conn.execute(text('''
            INSERT INTO stg_lecturas (meter_id, "timestamp", sector, consumo_m3, presion_bar, quality_flag)
            SELECT meter_id, "timestamp", sector, consumo_m3, presion_bar, quality_flag
            FROM lote_entrante
            WHERE "timestamp" > (SELECT ultima_marca_agua FROM estado_ingesta WHERE sector = :s)
            ON CONFLICT (meter_id, "timestamp") DO NOTHING
        '''), {"s": sector})
        despues = conn.execute(text("SELECT COUNT(*) FROM stg_lecturas WHERE sector = :s"), {"s": sector}).scalar()
        # La marca nunca retrocede (GREATEST); filas_cargadas se asigna, no se acumula
        conn.execute(text('''
            UPDATE estado_ingesta
            SET ultima_marca_agua = GREATEST(ultima_marca_agua, (SELECT MAX("timestamp") FROM lote_entrante)),
                ultima_ejecucion   = now(),
                filas_cargadas     = (SELECT COUNT(*) FROM lote_entrante)
            WHERE sector = :s
        '''), {"s": sector})
        conn.execute(text("DROP TABLE IF EXISTS lote_entrante"))
    return despues - antes


def reprocesar_ventana(engine, df_lote, sector):
    """Reabre una ventana ya cargada: sin filtro de marca, solo ON CONFLICT DO
    NOTHING, y sin tocar estado_ingesta (rellena el pasado, no mueve el corte)."""
    df_sector = df_lote[df_lote["sector"] == sector].copy()
    if df_sector.empty:
        return 0
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS lote_reproceso"))
        df_sector.to_sql("lote_reproceso", conn, if_exists="replace", index=False)
        antes = conn.execute(text("SELECT COUNT(*) FROM stg_lecturas WHERE sector = :s"), {"s": sector}).scalar()
        conn.execute(text('''
            INSERT INTO stg_lecturas (meter_id, "timestamp", sector, consumo_m3, presion_bar, quality_flag)
            SELECT meter_id, "timestamp", sector, consumo_m3, presion_bar, quality_flag
            FROM lote_reproceso
            ON CONFLICT (meter_id, "timestamp") DO NOTHING
        '''))
        despues = conn.execute(text("SELECT COUNT(*) FROM stg_lecturas WHERE sector = :s"), {"s": sector}).scalar()
        conn.execute(text("DROP TABLE IF EXISTS lote_reproceso"))
    return despues - antes


def huella(engine):
    """Estado observable: conteo, suma de consumo y marca de agua por sector."""
    with engine.connect() as conn:
        n = conn.execute(text("SELECT COUNT(*) FROM stg_lecturas")).scalar()
        suma = round(conn.execute(text("SELECT SUM(consumo_m3) FROM stg_lecturas")).scalar() or 0.0, 6)
        marcas = {s: str(m) for s, m in conn.execute(
            text("SELECT sector, ultima_marca_agua FROM estado_ingesta ORDER BY sector"))}
    return n, suma, marcas


def corrida(engine, df_lote, reprocesar, etiqueta):
    funcion = reprocesar_ventana if reprocesar else ingest_lote
    print(f"\n== {etiqueta} ({'reproceso de ventana' if reprocesar else 'ingesta incremental'}) ==")
    for sector in SECTORES:
        print(f"  {sector:8s} {funcion(engine, df_lote, sector):>5} filas nuevas")
    n, suma, marcas = huella(engine)
    print(f"  stg_lecturas: {n} filas · suma consumo_m3: {suma}")
    for sector, marca in marcas.items():
        print(f"  marca de agua {sector:8s} {marca}")
    return n, suma, marcas


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--equipo", required=True, choices=EQUIPOS_VALIDOS, help="Identificador de equipo")
    ap.add_argument("--dia", type=int, default=1, help="Numero de lote a cargar (lote_diaNN.csv), por defecto 1")
    ap.add_argument("--lotes", default=None, help="Carpeta de lotes (por defecto lotes_<equipo>)")
    ap.add_argument("--reiniciar", action="store_true", help="DESTRUCTIVO: vacia stg_lecturas y marcas antes de cargar")
    ap.add_argument("--doble", action="store_true", help="Corre la carga dos veces y compara conteo, suma y marcas")
    ap.add_argument("--reprocesar", action="store_true", help="Reabre la ventana del lote sin filtro de marca")
    a = ap.parse_args()

    archivo = Path(a.lotes or f"lotes_{a.equipo}") / f"lote_dia{a.dia:02d}.csv"
    if not archivo.exists():
        raise SystemExit(f"No existe {archivo}. Generen la instancia con: python generar_datos_S12.py "
                         f"--equipo {a.equipo} --dias {max(a.dia, 2)} --inicio 2024-06-01 --salida lotes_{a.equipo}")
    df_lote = pd.read_csv(archivo, parse_dates=["timestamp"])

    engine, nombre_bd = construir_engine(a.equipo)
    preparar_esquema(engine)
    if a.reiniciar:
        reiniciar_estado(engine)
    n0, _, marcas0 = huella(engine)
    print(f"Base {nombre_bd} · lote {archivo} ({len(df_lote)} filas recibidas)")
    print(f"Estado inicial: {n0} filas en stg_lecturas · marcas {sorted(set(marcas0.values()))}")

    primera = corrida(engine, df_lote, a.reprocesar, "Corrida 1")
    if a.doble:
        segunda = corrida(engine, df_lote, a.reprocesar, "Corrida 2, mismo lote")
        iguales = primera == segunda
        print(f"\nPrueba de doble ejecucion: conteo {primera[0]} -> {segunda[0]}, "
              f"suma {primera[1]} -> {segunda[1]}, marcas iguales: {primera[2] == segunda[2]}")
        print("RESULTADO: IDEMPOTENTE" if iguales else "RESULTADO: FALLO DE IDEMPOTENCIA")
        sys.exit(0 if iguales else 1)


if __name__ == "__main__":
    main()
