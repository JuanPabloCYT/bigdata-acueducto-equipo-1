"""Conector de ingesta desde una API paginada: patron de referencia.

Que hace
    Extrae el estado de facturacion de los medidores desde una API REST con
    paginacion, autenticacion por token y limite de tasa, y lo deja en las
    tablas del proyecto:
      1. Pagina siguiendo "hay_siguiente", sin calcular de antemano cuantas
         paginas hay.
      2. Reintenta solo los errores transitorios con retroceso exponencial
         mas jitter: 429, 5xx y la falta de respuesta (tiempo de espera
         agotado o conexion fallida, que quedan en la bitacora sin codigo
         HTTP). El encabezado Retry-After del proveedor prevalece sobre el
         calculo propio. Una pagina que agota sus intentos se registra como
         AGOTADO y la corrida continua.
      3. Un 4xx distinto de 429 y de 401 (por ejemplo 404) es permanente solo
         para esa pagina: se registra como FALLO_PERMANENTE y la corrida
         continua con las restantes. Un 401 (credencial invalida) es
         permanente para TODA la corrida: no se reintenta, se registra como
         FALLO_AUTENTICACION y la corrida se aborta sin pedir mas paginas
         (estado_corrida = "ABORTADA_AUTENTICACION"); lo cargado antes queda.
      4. Escribe el estado vigente de cada medidor en stg_estado_medidor con
         UPSERT por meter_id: repetir una corrida no duplica filas.
      5. Registra cada intento en bitacora_ingesta_api (solo inserciones),
         para poder reconstruir que paso en cada pagina.

Funciones
    extraer_todas_las_paginas(engine, base_url, token, equipo_etiqueta, ...)
        Recorre todas las paginas y devuelve un resumen de la corrida
        (corrida_id, estado_corrida, filas_nuevas, paginas_incompletas,
        ultima_pagina, duracion_s). estado_corrida es COMPLETA,
        CON_PAGINAS_INCOMPLETAS, ABORTADA_AUTENTICACION, PRESUPUESTO_AGOTADO
        o TOPE_PAGINAS. El presupuesto de tiempo es un tope blando (se evalua
        entre paginas; la pagina en curso termina primero) y el tope de
        paginas es un tope duro.
    extraer_pagina_con_reintentos(...), solicitar_pagina(...), espera_backoff(...),
    registrar_intento(...), upsert_estado_medidor(...)
        Los pasos que componen la extraccion de una pagina.

Requisitos
    PostgreSQL del perfil s12 en marcha, con las tablas stg_estado_medidor y
    bitacora_ingesta_api; el simulador de la API arrancado (practica/
    iniciar_api_simulada.py); paquetes sqlalchemy, psycopg2-binary, pandas,
    requests y tenacity.

Como usarlo
    Importen la funcion principal desde su propio script de ingesta:
        from pipeline.conector_s13 import extraer_todas_las_paginas, TOKEN
    y llamenla con un motor de SQLAlchemy y la direccion del simulador. Este
    modulo no se ejecuta al arrancar el contenedor.

Ejercicio didactico y produccion
    Este patron es el nucleo de un conector real, pero un despliegue
    productivo agrega un punto de reanudacion persistido para no recomenzar
    desde la pagina 1, metricas y alertas (tasa de errores por codigo, paginas
    agotadas por corrida), registros estructurados hacia un sistema de
    observabilidad, concurrencia controlada con un cupo compartido y el token
    en un gestor de secretos.
"""
import os
import random
import time
import uuid

import pandas as pd
import requests
from sqlalchemy import text

# Token didactico por defecto. En un despliegue real lo entrega un gestor de secretos.
TOKEN = os.environ.get("API_TOKEN", "acueducto-token-didactico")


# Clasificacion de errores y espera entre intentos
from tenacity import retry, retry_if_exception_type, stop_after_attempt

class ErrorTransitorio(Exception):
    """Error que amerita reintento: 429, 5xx, o una solicitud sin respuesta
    (tiempo de espera agotado o conexion fallida). En ese ultimo caso no hay
    codigo HTTP y codigo_http queda en None."""
    def __init__(self, mensaje, codigo_http=None, retry_after=None):
        super().__init__(mensaje)
        self.codigo_http = codigo_http
        self.retry_after = retry_after

class ErrorPermanente(Exception):
    """Error 4xx (distinto de 429 y de 401) que NO amerita reintento. Afecta
    solo a esa pagina: la corrida continua con las demas."""
    def __init__(self, mensaje, codigo_http=None):
        super().__init__(mensaje)
        self.codigo_http = codigo_http

class ErrorAutenticacion(Exception):
    """HTTP 401: la credencial es invalida. No depende de la pagina, asi que
    ningun reintento ni ninguna otra pagina lo corrige: aborta toda la corrida.
    No hereda de ErrorPermanente a proposito, para que nadie la capture por
    accidente como un error de una sola pagina."""
    def __init__(self, mensaje, codigo_http=401):
        super().__init__(mensaje)
        self.codigo_http = codigo_http

def espera_backoff(t0, t_max):
    """Funcion de espera para tenacity: t_n = min(t_max, t0 * 2^n) + jitter U(0, t0).
    Si el proveedor envio Retry-After, ese valor manda sobre el calculo propio."""
    def _wait(retry_state):
        excepcion = retry_state.outcome.exception()
        if isinstance(excepcion, ErrorTransitorio) and excepcion.retry_after is not None:
            return excepcion.retry_after
        n = retry_state.attempt_number - 1      # tras el intento 1, n = 0: la primera espera es t0
        return min(t_max, t0 * (2 ** n)) + random.uniform(0, t0)
    return _wait

# Nivel 1 (acelerado): esperas en fracciones de segundo, coherentes con --acelerar del simulador.
T0_ACELERADO, T_MAX_ACELERADO = 0.1, 1.0
# Nivel 2 y siguientes: valores realistas (t0 = 1 s, techo de 30 s).
T0_REALISTA, T_MAX_REALISTA = 1.0, 30.0
MAX_INTENTOS = 5
TIMEOUT_S = 5         # segundos que se espera la respuesta de UNA solicitud antes de darla por perdida
PRESUPUESTO_S = 180   # tope BLANDO por corrida: se evalua antes de pedir cada pagina, asi que la pagina en curso termina primero


def solicitar_pagina(session, base_url, token, pagina, page_size, timeout_s=TIMEOUT_S):
    """Una sola solicitud HTTP. 200 es exito. Son transitorios (reintentables):
    429, 5xx y la falta de respuesta (requests.exceptions.Timeout o
    ConnectionError, que se convierten en ErrorTransitorio sin codigo HTTP).
    401 lanza ErrorAutenticacion (aborta la corrida). Cualquier otro 4xx es
    permanente solo para esa pagina (no se reintenta)."""
    parametros = {"page": pagina}
    if page_size is not None:
        parametros["page_size"] = page_size
    try:
        resp = session.get(f"{base_url}/medidores/estado", params=parametros,
                           headers={"Authorization": f"Bearer {token}"}, timeout=timeout_s)
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        # Sin respuesta no hay codigo HTTP que clasificar, pero la falla puede desaparecer sola.
        raise ErrorTransitorio(f"Sin respuesta en pagina {pagina}: {type(e).__name__}", codigo_http=None) from e
    if resp.status_code == 200:
        return resp.json()
    if resp.status_code == 401:
        raise ErrorAutenticacion(f"HTTP 401 en pagina {pagina}: token invalido o ausente")
    if resp.status_code == 429 or 500 <= resp.status_code < 600:
        retry_after = resp.headers.get("Retry-After")
        raise ErrorTransitorio(f"HTTP {resp.status_code} en pagina {pagina}",
                               codigo_http=resp.status_code,
                               retry_after=float(retry_after) if retry_after else None)
    raise ErrorPermanente(f"HTTP {resp.status_code} en pagina {pagina}", codigo_http=resp.status_code)


def registrar_intento(engine, corrida_id, equipo, pagina, intento, codigo_http, duracion_ms, resultado):
    """bitacora_ingesta_api solo inserta: un INSERT por intento, nunca un UPSERT.
    A proposito distinto de stg_estado_medidor."""
    with engine.begin() as conn:
        conn.execute(text('''
            INSERT INTO bitacora_ingesta_api
                (corrida_id, equipo, pagina, intento, codigo_http, duracion_ms, resultado, timestamp_intento)
            VALUES (:corrida_id, :equipo, :pagina, :intento, :codigo_http, :duracion_ms, :resultado, now())
        '''), dict(corrida_id=corrida_id, equipo=equipo, pagina=pagina, intento=intento,
                   codigo_http=codigo_http, duracion_ms=duracion_ms, resultado=resultado))


def extraer_pagina_con_reintentos(engine, session, base_url, token, pagina, page_size,
                                  corrida_id, equipo, t0, t_max, max_intentos=MAX_INTENTOS,
                                  timeout_s=TIMEOUT_S):
    """Extrae una pagina con reintentos, registrando cada intento.
    Devuelve (respuesta, estado, codigo): la respuesta JSON (o None si la pagina
    no se pudo leer), el resultado final y el ultimo codigo HTTP (None si no
    hubo respuesta). Resultados posibles:
      EXITO                200.
      AGOTADO              error transitorio (429, 5xx, tiempo de espera agotado
                           o conexion fallida) en todos los intentos; el llamador
                           sigue con la pagina siguiente.
      FALLO_PERMANENTE     4xx distinto de 429 y de 401: NO se reintenta; el
                           llamador sigue con la pagina siguiente (fallo PARCIAL).
      FALLO_AUTENTICACION  401: NO se reintenta y el llamador debe abortar la
                           corrida, porque la credencial es la misma para todas
                           las paginas."""
    contador = {"n": 0, "t_intento": None}

    def _solicitar():
        contador["n"] += 1
        contador["t_intento"] = time.time()
        return solicitar_pagina(session, base_url, token, pagina, page_size, timeout_s)

    def _antes_de_dormir(retry_state):
        exc = retry_state.outcome.exception()
        duracion_ms = (time.time() - contador["t_intento"]) * 1000
        registrar_intento(engine, corrida_id, equipo, pagina, contador["n"],
                          getattr(exc, "codigo_http", None), duracion_ms, "REINTENTABLE")

    intentar = retry(
        retry=retry_if_exception_type(ErrorTransitorio),
        stop=stop_after_attempt(max_intentos),
        wait=espera_backoff(t0, t_max),
        before_sleep=_antes_de_dormir,
        reraise=True,
    )(_solicitar)

    try:
        resultado = intentar()
        duracion_ms = (time.time() - contador["t_intento"]) * 1000
        registrar_intento(engine, corrida_id, equipo, pagina, contador["n"], 200, duracion_ms, "EXITO")
        return resultado, "EXITO", 200
    except ErrorAutenticacion as e:
        duracion_ms = (time.time() - contador["t_intento"]) * 1000
        registrar_intento(engine, corrida_id, equipo, pagina, contador["n"], e.codigo_http, duracion_ms, "FALLO_AUTENTICACION")
        return None, "FALLO_AUTENTICACION", e.codigo_http
    except ErrorPermanente as e:
        duracion_ms = (time.time() - contador["t_intento"]) * 1000
        registrar_intento(engine, corrida_id, equipo, pagina, contador["n"], e.codigo_http, duracion_ms, "FALLO_PERMANENTE")
        return None, "FALLO_PERMANENTE", e.codigo_http
    except ErrorTransitorio as e:      # con reraise=True, asi llega el agotamiento de intentos
        duracion_ms = (time.time() - contador["t_intento"]) * 1000
        registrar_intento(engine, corrida_id, equipo, pagina, contador["n"], e.codigo_http, duracion_ms, "AGOTADO")
        return None, "AGOTADO", e.codigo_http


def upsert_estado_medidor(engine, registros):
    """Estado vigente por meter_id: UPSERT, no historico."""
    if not registros:
        return 0
    df = pd.DataFrame(registros)
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS lote_estado_entrante"))
        df.to_sql("lote_estado_entrante", conn, if_exists="replace", index=False)
        antes = conn.execute(text("SELECT COUNT(*) FROM stg_estado_medidor")).scalar()
        conn.execute(text('''
            INSERT INTO stg_estado_medidor (meter_id, sector, saldo, estado_servicio, actualizado_en)
            SELECT meter_id, sector, saldo, estado_servicio, CAST(actualizado_en AS TIMESTAMP)
            FROM lote_estado_entrante
            ON CONFLICT (meter_id) DO UPDATE SET
                sector = EXCLUDED.sector,
                saldo = EXCLUDED.saldo,
                estado_servicio = EXCLUDED.estado_servicio,
                actualizado_en = EXCLUDED.actualizado_en
        '''))
        despues = conn.execute(text("SELECT COUNT(*) FROM stg_estado_medidor")).scalar()
    return despues - antes   # filas NUEVAS: actualizar una fila existente no suma


def extraer_todas_las_paginas(engine, base_url, token, equipo_etiqueta, page_size=None,
                              t0=T0_REALISTA, t_max=T_MAX_REALISTA, max_intentos=MAX_INTENTOS,
                              presupuesto_s=PRESUPUESTO_S, tope_paginas=50, timeout_s=TIMEOUT_S):
    """Sigue 'hay_siguiente' pagina a pagina: NUNCA calcula de antemano cuantas
    paginas hay. Una pagina incompleta se anota y la corrida continua, salvo
    que la credencial sea invalida (401): entonces la corrida se aborta.

    Devuelve un resumen con 'estado_corrida':
      COMPLETA                 llego al final de los datos sin paginas incompletas.
      CON_PAGINAS_INCOMPLETAS  llego al final, pero alguna pagina agoto sus intentos
                               o fallo de forma permanente.
      ABORTADA_AUTENTICACION   un 401 detuvo la corrida; lo cargado antes del 401
                               queda cargado y la pagina del 401 figura como incompleta.
      PRESUPUESTO_AGOTADO      el tiempo transcurrido supero presupuesto_s (tope BLANDO:
                               se evalua antes de pedir cada pagina, asi que la pagina
                               en curso termina primero).
      TOPE_PAGINAS             se alcanzo tope_paginas (tope DURO contra bucles infinitos).
    'paginas_incompletas' lista solo paginas que se pidieron: para saber si la corrida
    cubrio todos los datos hay que mirar tambien 'estado_corrida'."""
    corrida_id = str(uuid.uuid4())
    session = requests.Session()
    pagina = 1
    ultima_pedida = 0
    filas_nuevas = 0
    paginas_incompletas = []
    estado_corrida = None
    pagina_previa_leida = True       # la pagina 1 no tiene pagina previa cuya senal haya que leer
    inicio = time.time()
    while True:
        if time.time() - inicio > presupuesto_s:
            print(f"Presupuesto de {presupuesto_s} s agotado: se detiene la corrida antes de pedir la pagina {pagina}.")
            estado_corrida = "PRESUPUESTO_AGOTADO"
            break
        ultima_pedida = pagina
        respuesta, estado, codigo = extraer_pagina_con_reintentos(
            engine, session, base_url, token, pagina, page_size, corrida_id, equipo_etiqueta, t0, t_max, max_intentos,
            timeout_s)
        if estado == "FALLO_AUTENTICACION":
            # Un 401 no depende de la pagina: seguir pidiendo solo gastaria solicitudes y cuota.
            # Lo cargado antes queda cargado; el intento ya esta en la bitacora.
            print(f"HTTP 401 en la pagina {pagina}: credencial invalida. Se aborta la corrida sin pedir mas paginas.")
            paginas_incompletas.append(pagina)
            estado_corrida = "ABORTADA_AUTENTICACION"
            break
        if respuesta is None:
            if codigo == 404 and not pagina_previa_leida:
                # La pagina anterior no se pudo leer, asi que se perdio su 'hay_siguiente'. Un 404
                # aqui significa que esa pagina era la ultima: no hay mas datos que pedir. El 404
                # queda en la bitacora, pero no se cuenta como pagina incompleta.
                break
            paginas_incompletas.append(pagina)
            pagina_previa_leida = False
            pagina += 1
            if pagina > tope_paginas:
                print(f"Tope duro de {tope_paginas} paginas alcanzado: se detiene la corrida.")
                estado_corrida = "TOPE_PAGINAS"
                break
            continue
        pagina_previa_leida = True
        filas_nuevas += upsert_estado_medidor(engine, respuesta["datos"])
        if not respuesta["hay_siguiente"]:
            break
        pagina += 1
    if estado_corrida is None:
        estado_corrida = "CON_PAGINAS_INCOMPLETAS" if paginas_incompletas else "COMPLETA"
    return {"corrida_id": corrida_id, "estado_corrida": estado_corrida, "filas_nuevas": filas_nuevas,
            "paginas_incompletas": paginas_incompletas, "ultima_pagina": ultima_pedida,
            "duracion_s": round(time.time() - inicio, 2)}


if __name__ == "__main__":
    # Ejemplo de uso, no se ejecuta automaticamente.
    from sqlalchemy import create_engine
    engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/acueducto")
    print("Motor listo. Importen extraer_todas_las_paginas en su script de ingesta.")
