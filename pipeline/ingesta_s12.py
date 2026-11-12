"""Ingesta incremental idempotente del acueducto: patron de referencia.

Que hace
    Carga lotes de lecturas en la tabla stg_lecturas de PostgreSQL de forma que
    repetir una carga no duplica filas. Combina tres mecanismos:
      1. Marca de agua: la tabla estado_ingesta guarda, por sector, el ultimo
         instante ya cargado; cada corrida solo mira lo posterior.
      2. Escritura por clave: INSERT ... ON CONFLICT (meter_id, "timestamp")
         DO NOTHING ignora las filas que ya existen (idempotencia).
      3. Transaccion atomica: los datos y la marca de agua se actualizan juntos
         o no se actualiza ninguno, asi una caida a mitad de camino no deja el
         estado inconsistente.

Funciones
    ingest_lote(engine, df_lote, sector)
        Ingesta incremental normal de un sector. Devuelve las filas nuevas.
    reprocesar_ventana(engine, df_lote, sector)
        Reabre una ventana ya cargada: ignora a proposito el filtro de marca
        de agua y confia solo en ON CONFLICT. No toca estado_ingesta, asi que
        la marca de agua no cambia.

Requisitos
    PostgreSQL del perfil s12 en marcha, con las tablas stg_lecturas y
    estado_ingesta; paquetes sqlalchemy, psycopg2-binary y pandas.

Como usarlo
    Importen las funciones desde su propio script de ingesta:
        from pipeline.ingesta_s12 import ingest_lote, reprocesar_ventana
    y llamenlas con un motor de SQLAlchemy y un DataFrame que tenga las
    columnas meter_id, timestamp, sector, consumo_m3, presion_bar y quality_flag. Este modulo no
    se ejecuta al arrancar el contenedor.

Ejercicio didactico y produccion
    Este patron es el nucleo de una ingesta real, pero un despliegue
    productivo agrega reintentos con espera creciente ante fallos de red,
    alertas cuando la marca de agua no avanza, credenciales en un gestor de
    secretos y tablas temporales por sesion en lugar de tablas de paso
    compartidas.
"""
from sqlalchemy import create_engine, text

SECTORES = ["S-NORTE", "S-SUR", "S-ESTE", "S-OESTE"]


def ingest_lote(engine, df_lote, sector):
    """Inserta el lote de un sector y actualiza su marca de agua en una sola
    transaccion: o se cargan los datos y se actualiza el estado, o ninguna de
    las dos cosas."""
    df_sector = df_lote[df_lote["sector"] == sector].copy()
    if df_sector.empty:
        return 0
    with engine.begin() as conn:
        # El lote entrante se deja en una tabla de paso para poder usar SQL.
        conn.execute(text("DROP TABLE IF EXISTS lote_entrante"))
        df_sector.to_sql("lote_entrante", conn, if_exists="replace", index=False)
        antes = conn.execute(text("SELECT COUNT(*) FROM stg_lecturas WHERE sector = :s"), {"s": sector}).scalar()
        # Solo lo posterior a la marca de agua, y sin duplicar por clave.
        conn.execute(text('''
            INSERT INTO stg_lecturas (meter_id, "timestamp", sector, consumo_m3, presion_bar, quality_flag)
            SELECT meter_id, "timestamp", sector, consumo_m3, presion_bar, quality_flag
            FROM lote_entrante
            WHERE "timestamp" > (SELECT ultima_marca_agua FROM estado_ingesta WHERE sector = :s)
            ON CONFLICT (meter_id, "timestamp") DO NOTHING
        '''), {"s": sector})
        despues = conn.execute(text("SELECT COUNT(*) FROM stg_lecturas WHERE sector = :s"), {"s": sector}).scalar()
        # La marca de agua nunca retrocede (GREATEST) y se mueve en la misma transaccion.
        conn.execute(text('''
            UPDATE estado_ingesta
            SET ultima_marca_agua = GREATEST(ultima_marca_agua, (SELECT MAX("timestamp") FROM lote_entrante)),
                ultima_ejecucion   = now(),
                filas_cargadas     = (SELECT COUNT(*) FROM lote_entrante)
            WHERE sector = :s
        '''), {"s": sector})
    return despues - antes


def reprocesar_ventana(engine, df_lote, sector):
    """Reabre una ventana ya cargada: ignora a proposito el filtro de marca de
    agua e inserta el lote completo, confiando en ON CONFLICT DO NOTHING para
    no duplicar. No usa GREATEST ni toca estado_ingesta: la marca de agua (y
    con ella el punto de reanudacion de la carga incremental) queda como estaba."""
    df_sector = df_lote[df_lote["sector"] == sector].copy()
    if df_sector.empty:
        return 0
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS lote_reproceso"))
        df_sector.to_sql("lote_reproceso", conn, if_exists="replace", index=False)
        antes = conn.execute(text("SELECT COUNT(*) FROM stg_lecturas WHERE sector = :s"), {"s": sector}).scalar()
        # Sin filtro de marca de agua: se intenta insertar toda la ventana.
        conn.execute(text('''
            INSERT INTO stg_lecturas (meter_id, "timestamp", sector, consumo_m3, presion_bar, quality_flag)
            SELECT meter_id, "timestamp", sector, consumo_m3, presion_bar, quality_flag
            FROM lote_reproceso
            ON CONFLICT (meter_id, "timestamp") DO NOTHING
        '''), {"s": sector})
        despues = conn.execute(text("SELECT COUNT(*) FROM stg_lecturas WHERE sector = :s"), {"s": sector}).scalar()
    return despues - antes


if __name__ == "__main__":
    # Ejemplo de uso, no se ejecuta automaticamente.
    engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/acueducto")
    print("Motor listo. Importen ingest_lote / reprocesar_ventana en su script de T12.")
