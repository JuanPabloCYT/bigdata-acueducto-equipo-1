"""Conector de ingesta desde la API de estado de medidores (T13 · equipo-1).

Pagina siguiendo `hay_siguiente`, se autentica con un token Bearer, reintenta solo lo
transitorio con retroceso exponencial (con techo y jitter) y deja una bitacora con un
registro por intento.

Tablas (nombres exactos):
  stg_estado_medidor    estado vigente por meter_id, se escribe con UPSERT (no crece al repetir).
  bitacora_ingesta_api  auditoria, un INSERT por intento (crece con cada corrida, nunca se deduplica).

Clasificacion de codigos (decision del equipo, ver docs/T13_entrega_equipo-1.md, seccion 3):
  200                         exito
  408, 429, 5xx, sin respuesta  reintentable (transitorio)
  401                         aborta la corrida (la credencial es la misma para todas las paginas)
  otros 4xx                   permanente solo para esa pagina; la corrida continua

Parametros elegidos para el perfil de equipo-1 (p = 0.141 + 0.139 = 0.28, 7 paginas):
  t0 = 1 s, t_max = 30 s, max_intentos = 6  -> 31 s de espera maxima sin jitter por pagina.

Uso (con PostgreSQL del perfil s12 arriba y la API simulada del equipo corriendo):
  python iniciar_api_simulada.py --equipo equipo-1 --puerto 8014          # terminal 1
  python conector_api_medidores.py --equipo equipo-1 --puerto 8014 --reiniciar --doble   # terminal 2
"""
import argparse
import os
import random
import sys
import time
import uuid

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from tenacity import retry, retry_if_exception_type, stop_after_attempt

EQUIPOS_VALIDOS = [f"equipo-{i}" for i in range(1, 7)]
URL_POSTGRES = os.environ.get("URL_POSTGRES", "postgresql+psycopg2://postgres:postgres@localhost:5432/{bd}")
TOKEN = os.environ.get("API_TOKEN", "acueducto-token-didactico")  # didactico: en produccion, gestor de secretos

# Configuracion elegida en el Nivel 3 (registro de decision)
T0, T_MAX, MAX_INTENTOS = 1.0, 30.0, 6
TIMEOUT_S = 5
PRESUPUESTO_S = 300      # tope blando por corrida (7 paginas x 36 s de peor caso = 252 s)
TOPE_PAGINAS = 50        # tope duro contra bucles infinitos

DDL = [
    """CREATE TABLE IF NOT EXISTS stg_estado_medidor (
        meter_id VARCHAR PRIMARY KEY,
        sector VARCHAR,
        saldo DOUBLE PRECISION,
        estado_servicio VARCHAR,
        actualizado_en TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS bitacora_ingesta_api (
        corrida_id VARCHAR,
        equipo VARCHAR,
        pagina INTEGER,
        intento INTEGER,
        codigo_http INTEGER,
        duracion_ms DOUBLE PRECISION,
        resultado VARCHAR,
        timestamp_intento TIMESTAMP)""",
]


# --------------------------------------------------------------------------- errores
class ErrorTransitorio(Exception):
    def __init__(self, mensaje, codigo_http=None, retry_after=None):
        super().__init__(mensaje)
        self.codigo_http = codigo_http
        self.retry_after = retry_after


class ErrorPermanente(Exception):
    def __init__(self, mensaje, codigo_http=None):
        super().__init__(mensaje)
        self.codigo_http = codigo_http


class ErrorAutenticacion(Exception):
    def __init__(self, mensaje, codigo_http=401):
        super().__init__(mensaje)
        self.codigo_http = codigo_http


def clasificar(codigo):
    """'exito', 'reintentable' o 'permanente'. None = sin respuesta."""
    if codigo is None:
        return "reintentable"
    if 200 <= codigo < 300:
        return "exito"
    if codigo in (408, 429) or 500 <= codigo < 600:
        return "reintentable"
    return "permanente"


def espera_backoff(t0, t_max):
    """t_n = min(t_max, t0 * 2^n) + U(0, t0); Retry-After del proveedor manda si viene."""
    def _wait(retry_state):
        exc = retry_state.outcome.exception()
        if isinstance(exc, ErrorTransitorio) and exc.retry_after is not None:
            return exc.retry_after
        n = retry_state.attempt_number - 1
        return min(t_max, t0 * (2 ** n)) + random.uniform(0, t0)
    return _wait


# --------------------------------------------------------------------------- base de datos
def crear_bd(nombre):
    admin = create_engine(URL_POSTGRES.format(bd="postgres"), isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        if not conn.execute(text("SELECT 1 FROM pg_database WHERE datname = :n"), {"n": nombre}).scalar():
            conn.execute(text(f'CREATE DATABASE "{nombre}"'))
    admin.dispose()


def construir_engine(equipo):
    nombre = f"acueducto_{equipo.replace('-', '_')}"
    crear_bd(nombre)
    engine = create_engine(URL_POSTGRES.format(bd=nombre))
    with engine.begin() as conn:
        for ddl in DDL:
            conn.execute(text(ddl))
    return engine, nombre


def contar(engine, tabla):
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT COUNT(*) FROM {tabla}")).scalar()


def registrar_intento(engine, corrida_id, equipo, pagina, intento, codigo_http, duracion_ms, resultado):
    """Un INSERT por intento, en su propia transaccion (si el proceso cae, el rastro queda)."""
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO bitacora_ingesta_api (corrida_id, equipo, pagina, intento, codigo_http, "
            "duracion_ms, resultado, timestamp_intento) VALUES (:c, :e, :p, :i, :cod, :d, :r, now())"),
            {"c": corrida_id, "e": equipo, "p": pagina, "i": intento, "cod": codigo_http,
             "d": duracion_ms, "r": resultado})


def upsert_estado_medidor(engine, registros):
    """Estado vigente por meter_id. Devuelve cuantas filas NUEVAS agrego."""
    if not registros:
        return 0
    with engine.begin() as conn:
        antes = conn.execute(text("SELECT COUNT(*) FROM stg_estado_medidor")).scalar()
        conn.execute(text(
            "INSERT INTO stg_estado_medidor (meter_id, sector, saldo, estado_servicio, actualizado_en) "
            "VALUES (:meter_id, :sector, :saldo, :estado_servicio, CAST(:actualizado_en AS TIMESTAMP)) "
            "ON CONFLICT (meter_id) DO UPDATE SET sector = EXCLUDED.sector, saldo = EXCLUDED.saldo, "
            "estado_servicio = EXCLUDED.estado_servicio, actualizado_en = EXCLUDED.actualizado_en"),
            registros)
        despues = conn.execute(text("SELECT COUNT(*) FROM stg_estado_medidor")).scalar()
    return despues - antes


# --------------------------------------------------------------------------- HTTP
def solicitar_pagina(session, base_url, token, pagina, timeout_s=TIMEOUT_S):
    try:
        r = session.get(f"{base_url}/medidores/estado", params={"page": pagina},
                        headers={"Authorization": f"Bearer {token}"}, timeout=timeout_s)
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        raise ErrorTransitorio(f"sin respuesta: {type(e).__name__}") from e
    tipo = clasificar(r.status_code)
    if tipo == "exito":
        return r.json()
    if r.status_code == 401:
        raise ErrorAutenticacion("token invalido")
    if tipo == "reintentable":
        ra = r.headers.get("Retry-After")
        raise ErrorTransitorio(f"HTTP {r.status_code}", r.status_code, float(ra) if ra else None)
    raise ErrorPermanente(f"HTTP {r.status_code}", r.status_code)


def extraer_pagina(engine, session, base_url, token, pagina, corrida_id, equipo, t0, t_max, max_intentos):
    ctx = {"n": 0, "t": None}

    def _solicitar():
        ctx["n"] += 1
        ctx["t"] = time.time()
        return solicitar_pagina(session, base_url, token, pagina)

    def _antes_de_dormir(rs):
        exc = rs.outcome.exception()
        registrar_intento(engine, corrida_id, equipo, pagina, ctx["n"], getattr(exc, "codigo_http", None),
                          (time.time() - ctx["t"]) * 1000, "REINTENTABLE")

    intentar = retry(retry=retry_if_exception_type(ErrorTransitorio), stop=stop_after_attempt(max_intentos),
                     wait=espera_backoff(t0, t_max), before_sleep=_antes_de_dormir, reraise=True)(_solicitar)

    def _fin(codigo, resultado):
        registrar_intento(engine, corrida_id, equipo, pagina, ctx["n"], codigo, (time.time() - ctx["t"]) * 1000, resultado)

    try:
        datos = intentar()
        _fin(200, "EXITO")
        return datos, "EXITO", 200
    except ErrorAutenticacion as e:
        _fin(e.codigo_http, "FALLO_AUTENTICACION")
        return None, "FALLO_AUTENTICACION", e.codigo_http
    except ErrorPermanente as e:
        _fin(e.codigo_http, "FALLO_PERMANENTE")
        return None, "FALLO_PERMANENTE", e.codigo_http
    except ErrorTransitorio as e:
        _fin(e.codigo_http, "AGOTADO")
        return None, "AGOTADO", e.codigo_http


def extraer_todas_las_paginas(engine, base_url, token, equipo, t0=T0, t_max=T_MAX, max_intentos=MAX_INTENTOS,
                              presupuesto_s=PRESUPUESTO_S, tope_paginas=TOPE_PAGINAS):
    corrida_id = str(uuid.uuid4())
    session = requests.Session()
    pagina, ultima, filas_nuevas = 1, 0, 0
    incompletas, estado_corrida, previa_leida = [], None, True
    inicio = time.time()
    while True:
        if time.time() - inicio > presupuesto_s:
            estado_corrida = "PRESUPUESTO_AGOTADO"
            break
        ultima = pagina
        resp, estado, codigo = extraer_pagina(engine, session, base_url, token, pagina, corrida_id, equipo,
                                              t0, t_max, max_intentos)
        if estado == "FALLO_AUTENTICACION":
            incompletas.append(pagina)
            estado_corrida = "ABORTADA_AUTENTICACION"
            break
        if resp is None:
            if codigo == 404 and not previa_leida:
                break      # la pagina previa se perdio con su hay_siguiente: este 404 marca el final
            incompletas.append(pagina)
            previa_leida = False
            pagina += 1
            if pagina > tope_paginas:
                estado_corrida = "TOPE_PAGINAS"
                break
            continue
        previa_leida = True
        filas_nuevas += upsert_estado_medidor(engine, resp["datos"])
        if not resp["hay_siguiente"]:
            break
        pagina += 1
    if estado_corrida is None:
        estado_corrida = "CON_PAGINAS_INCOMPLETAS" if incompletas else "COMPLETA"
    return {"corrida_id": corrida_id, "estado_corrida": estado_corrida, "filas_nuevas": filas_nuevas,
            "paginas_incompletas": incompletas, "ultima_pagina": ultima, "duracion_s": round(time.time() - inicio, 2)}


def reporte_por_pagina(engine, corrida_id):
    with engine.connect() as conn:
        df = pd.read_sql(text("SELECT pagina, intento, codigo_http, resultado FROM bitacora_ingesta_api "
                              "WHERE corrida_id = :c ORDER BY pagina, intento"), conn, params={"c": corrida_id})
    ultimo = df.groupby("pagina").tail(1).set_index("pagina")
    return pd.DataFrame({"pagina": ultimo.index, "intentos": df.groupby("pagina")["intento"].count().values,
                         "codigo_final": ultimo["codigo_http"].values,
                         "resultado_final": ultimo["resultado"].values})


# --------------------------------------------------------------------------- CLI
def main():
    ap = argparse.ArgumentParser(description="Conector de la API de estado de medidores (T13)")
    ap.add_argument("--equipo", required=True, choices=EQUIPOS_VALIDOS)
    ap.add_argument("--puerto", type=int, default=8014, help="puerto de la API simulada")
    ap.add_argument("--token", default=TOKEN, help="token Bearer (use uno falso para probar el 401)")
    ap.add_argument("--reiniciar", action="store_true", help="DESTRUCTIVO: vacia las dos tablas antes de correr")
    ap.add_argument("--doble", action="store_true", help="corre dos veces seguidas y compara conteos")
    a = ap.parse_args()

    engine, bd = construir_engine(a.equipo)
    base_url = f"http://127.0.0.1:{a.puerto}"
    print(f"Base: {bd} · API: {base_url} · t0={T0} s, t_max={T_MAX} s, max_intentos={MAX_INTENTOS}")
    if a.reiniciar:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE stg_estado_medidor, bitacora_ingesta_api"))
    conteos = [(contar(engine, "stg_estado_medidor"), contar(engine, "bitacora_ingesta_api"))]
    print(f"Antes:          stg_estado_medidor={conteos[0][0]} · bitacora_ingesta_api={conteos[0][1]}")
    for n in range(1, 3 if a.doble else 2):
        res = extraer_todas_las_paginas(engine, base_url, a.token, a.equipo)
        conteos.append((contar(engine, "stg_estado_medidor"), contar(engine, "bitacora_ingesta_api")))
        print(f"Corrida {n}: {res['estado_corrida']} · paginas incompletas {res['paginas_incompletas']} · "
              f"{res['duracion_s']} s")
        print(reporte_por_pagina(engine, res["corrida_id"]).to_string(index=False))
        print(f"Tras corrida {n}: stg_estado_medidor={conteos[-1][0]} · bitacora_ingesta_api={conteos[-1][1]}")
    if a.doble:
        ok = conteos[2][0] == conteos[1][0] and conteos[2][1] > conteos[1][1]
        print(f"stg_estado_medidor {conteos[1][0]} -> {conteos[2][0]} (estado vigente, no crece) · "
              f"bitacora_ingesta_api {conteos[1][1]} -> {conteos[2][1]} (auditoria, crece)")
        print("RESULTADO: OK" if ok else "RESULTADO: FALLO")
        sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
