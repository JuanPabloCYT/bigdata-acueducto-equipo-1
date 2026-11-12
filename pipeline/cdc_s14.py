"""Prototipo de referencia de captura de cambios (CDC) por marca temporal.

Que hace
    Captura los cambios de maestro_suscriptores con el metodo mas simple: sondear la
    tabla con la marca temporal de ultima modificacion.
      1. Lee el checkpoint del equipo en estado_cdc_suscriptores. Si aun no hay ninguno
         usa un centinela antiguo (2000-01-01), nunca NULL: la primera corrida ve toda
         la tabla sin un caso especial.
      2. Consulta WHERE updated_at > checkpoint.
      3. Clasifica cada fila: created_at igual a updated_at es una ALTA (la fila nunca
         existio antes del checkpoint); created_at menor es una MODIFICACION.
      4. Agrega un registro por cambio a stg_cambios_suscriptor (solo inserciones) y
         avanza el checkpoint al mayor updated_at leido, en UNA sola transaccion: si el
         proceso cae en medio no queda un staging escrito con el checkpoint sin avanzar
         ni al reves.
      5. Devuelve el conteo por tipo de cambio. bajas_capturadas se MIDE con una consulta
         sobre stg_cambios_suscriptor por corrida_id; no es una constante.

El punto ciego
    Una fila eliminada fisicamente (DELETE) no cumple ninguna condicion WHERE: el metodo
    no puede verla. Cubrir las bajas exige otra tecnica (baja logica en el origen,
    reconciliacion periodica de llaves o captura por registro de transacciones).

Funciones
    leer_checkpoint(engine, equipo_etiqueta), consultar_cambios(engine, checkpoint),
    clasificar(df_cambios), escribir_cambios_y_checkpoint(...),
    correr_prototipo_cdc(engine, equipo_etiqueta)

Requisitos
    PostgreSQL del perfil s12 en marcha, con las tablas maestro_suscriptores,
    stg_cambios_suscriptor y estado_cdc_suscriptores; paquetes sqlalchemy, psycopg2-binary
    y pandas.

Como usarlo
    Importen la funcion principal desde su propio script de captura:
        from pipeline.cdc_s14 import correr_prototipo_cdc
    y llamenla con un motor de SQLAlchemy. Este modulo no se ejecuta al arrancar el
    contenedor.

Ejercicio didactico y produccion
    Este patron es el nucleo de una captura por consulta, pero un despliegue productivo
    agrega un indice sobre updated_at, un sondeo sobre una replica de lectura para no
    competir con las transacciones del negocio, un margen de solapamiento con un destino
    idempotente (para no perder cambios confirmados tarde), escritura por lotes en vez de
    fila a fila, metricas y alertas de retraso, y una estrategia explicita para las bajas
    fisicas.
"""
import datetime as dt
import uuid

import pandas as pd
from sqlalchemy import text


# Punto de control: lectura del checkpoint
CHECKPOINT_INICIAL = dt.datetime(2000, 1, 1)   # centinela antiguo: el checkpoint nunca es NULL

def leer_checkpoint(engine, equipo_etiqueta="sin-equipo"):
    """Devuelve el ultimo checkpoint guardado para este equipo. Si todavia no hay
    ninguno (primera corrida), devuelve el centinela antiguo y el sondeo ve toda la tabla."""
    with engine.connect() as conn:
        fila = conn.execute(
            text("SELECT checkpoint FROM estado_cdc_suscriptores WHERE equipo = :equipo"),
            {"equipo": equipo_etiqueta},
        ).fetchone()
    return fila[0] if fila else CHECKPOINT_INICIAL


def consultar_cambios(engine, checkpoint):
    """CDC por consulta: WHERE updated_at > checkpoint. No distingue por si sola una
    alta de una modificacion (eso lo hace clasificar) y tampoco puede ver ninguna fila
    que ya no exista: una fila eliminada no cumple ninguna condicion WHERE. Ese es el
    punto ciego del metodo."""
    with engine.connect() as conn:
        df = pd.read_sql(
            text("SELECT subscriber_id, sector, estrato, created_at, updated_at "
                 "FROM maestro_suscriptores WHERE updated_at > :checkpoint"),
            conn, params={"checkpoint": checkpoint},
        )
    return df


def clasificar(df_cambios):
    """created_at igual a updated_at implica ALTA (la fila nunca existio antes de este
    checkpoint); created_at menor que updated_at implica MODIFICACION (la fila ya
    existia y cambio un atributo). Devuelve el mismo DataFrame con la columna tipo_cambio."""
    if df_cambios.empty:
        return df_cambios.assign(tipo_cambio=pd.Series(dtype="object"))
    creado = pd.to_datetime(df_cambios["created_at"])
    actualizado = pd.to_datetime(df_cambios["updated_at"])
    tipo = pd.Series("modificacion", index=df_cambios.index)
    tipo[creado == actualizado] = "alta"
    return df_cambios.assign(tipo_cambio=tipo)


def escribir_cambios_y_checkpoint(engine, df_clasificado, corrida_id, nuevo_checkpoint, equipo_etiqueta):
    """Una sola transaccion: los cambios detectados se agregan a stg_cambios_suscriptor
    (solo inserciones) y el checkpoint del equipo avanza con un UPSERT."""
    filas = [
        dict(corrida_id=corrida_id, equipo=equipo_etiqueta, subscriber_id=f.subscriber_id,
             tipo_cambio=f.tipo_cambio, sector=f.sector, estrato=int(f.estrato))
        for f in df_clasificado.itertuples()
    ]
    with engine.begin() as conn:
        if filas:
            conn.execute(text('''
                INSERT INTO stg_cambios_suscriptor
                    (corrida_id, equipo, subscriber_id, tipo_cambio, sector, estrato, detectado_en)
                VALUES (:corrida_id, :equipo, :subscriber_id, :tipo_cambio, :sector, :estrato, now())
            '''), filas)
        conn.execute(text('''
            INSERT INTO estado_cdc_suscriptores (equipo, checkpoint, actualizado_en)
            VALUES (:equipo, :checkpoint, now())
            ON CONFLICT (equipo) DO UPDATE SET
                checkpoint = EXCLUDED.checkpoint,
                actualizado_en = EXCLUDED.actualizado_en
        '''), dict(equipo=equipo_etiqueta, checkpoint=nuevo_checkpoint))
    return len(filas)


def correr_prototipo_cdc(engine, equipo_etiqueta="sin-equipo"):
    """Una corrida completa: lee el checkpoint, consulta, clasifica, escribe los cambios
    y avanza el checkpoint en una sola transaccion. Devuelve el conteo por tipo de cambio.

    bajas_capturadas se MIDE con una consulta real sobre stg_cambios_suscriptor para esta
    corrida_id: no se asume como una constante. consultar_cambios solo ve filas que
    siguen existiendo, asi que clasificar solo produce altas y modificaciones; esta
    consulta lo confirma contra la base en cada corrida en lugar de darlo por sentado."""
    corrida_id = str(uuid.uuid4())
    checkpoint_anterior = leer_checkpoint(engine, equipo_etiqueta)
    df_clasificado = clasificar(consultar_cambios(engine, checkpoint_anterior))
    if df_clasificado.empty:
        nuevo_checkpoint = checkpoint_anterior       # sin cambios: el checkpoint no retrocede ni avanza
    else:
        nuevo_checkpoint = pd.to_datetime(df_clasificado["updated_at"]).max().to_pydatetime()
    n_escritas = escribir_cambios_y_checkpoint(engine, df_clasificado, corrida_id, nuevo_checkpoint, equipo_etiqueta)
    conteo = df_clasificado["tipo_cambio"].value_counts().to_dict() if not df_clasificado.empty else {}
    with engine.connect() as conn:
        bajas_capturadas = conn.execute(
            text("SELECT COUNT(*) FROM stg_cambios_suscriptor WHERE corrida_id = :corrida_id AND tipo_cambio = 'baja'"),
            {"corrida_id": corrida_id},
        ).scalar()
    return {
        "corrida_id": corrida_id,
        "checkpoint_anterior": checkpoint_anterior,
        "nuevo_checkpoint": nuevo_checkpoint,
        "altas_capturadas": conteo.get("alta", 0),
        "modificaciones_capturadas": conteo.get("modificacion", 0),
        "bajas_capturadas": bajas_capturadas,
        "filas_escritas": n_escritas,
    }


if __name__ == "__main__":
    # Ejemplo de uso, no se ejecuta automaticamente.
    from sqlalchemy import create_engine
    engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/acueducto")
    print("Motor listo. Importen correr_prototipo_cdc en su script de captura.")
