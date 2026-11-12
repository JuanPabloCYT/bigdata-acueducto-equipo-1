"""Generador del conjunto de estado de facturacion de medidores para la practica del conector de API.

Produce el conjunto (identificador de medidor, sector, saldo, estado del
servicio: activo, suspendido o en mora) que `iniciar_api_simulada.py` sirve
paginado. Tambien deriva, de forma reproducible, los parametros de
comportamiento de la API que varian por equipo (probabilidad de 429,
probabilidad de 5xx, latencia base, jitter, tamano de pagina): mismo
principio de diferenciacion que generar_datos_S12.py, extendido de "volumen y
tardios" a "latencia y tasa de fallos".

Semilla fija SEED = 42. Esquema de columnas del estado servido: meter_id,
sector, saldo, estado_servicio, actualizado_en.

Uso sin diferenciar por equipo (ejemplo resuelto del Nivel 1): produce
siempre 90 medidores, pagina de 15 en 15 (6 paginas), con los parametros de
fallo fijos que documenta la guia (prob_429=0.15, prob_5xx=0.05).
    python generar_estado_medidores.py --salida estado_medidores.csv

Uso diferenciado por equipo (Nivel 2, Nivel 3 y reto de diseno): --equipo fija
de forma reproducible el volumen de medidores, el tamano de pagina y los
parametros de fallo y latencia propios de cada equipo. Lista cerrada
(equipo-1 a equipo-6, igual que en el generador de lecturas).
    python generar_estado_medidores.py --equipo equipo-3 --salida estado_equipo-3.csv

--equipo es incompatible con --total/--page-size explicitos: son las mismas
cantidades que --equipo ya fija.

El mismo comando, con los mismos parametros, produce el mismo archivo byte a
byte (semilla fija).
"""
import argparse
import datetime as dt
import time
import zlib
from pathlib import Path

import numpy as np
import pandas as pd

SEED = 42
SECTORES = ["S-NORTE", "S-SUR", "S-ESTE", "S-OESTE"]
ESTADOS_SERVICIO = ["activo", "suspendido", "en_mora"]
PROB_ESTADOS = [0.80, 0.12, 0.08]
COLUMNAS = ["meter_id", "sector", "saldo", "estado_servicio", "actualizado_en"]

# Marca de actualizacion fija (no datetime.now()): reproducibilidad byte a
# byte exige que el archivo no dependa de cuando se ejecuta el generador.
ACTUALIZADO_EN = dt.datetime(2024, 6, 1, 6, 0, 0)

# Identificadores de equipo cerrados (la misma lista que en el generador de
# lecturas): los seis equipos del curso, ni uno mas.
EQUIPOS_VALIDOS = [f"equipo-{i}" for i in range(1, 7)]

# Valores fijos del ejemplo resuelto (sin --equipo): deterministas, sin
# depender de ninguna tirada aleatoria de parametros, para que la guia
# describa siempre el mismo escenario.
PARAMS_SIN_EQUIPO = {
    "total_registros": 90,
    "page_size": 15,
    "prob_429": 0.15,
    "prob_5xx": 0.05,
    "latencia_base_ms": 150.0,
    "jitter_ms": 60.0,
}


def validar_equipo(equipo):
    if equipo not in EQUIPOS_VALIDOS:
        raise SystemExit(
            f"Equipo '{equipo}' no reconocido. Identificadores validos: {', '.join(EQUIPOS_VALIDOS)}."
        )


def parametros_equipo(equipo, seed_base=SEED):
    """Deriva, de forma reproducible y determinista, los parametros que varian
    por equipo: volumen de medidores, tamano de pagina, probabilidad de 429,
    probabilidad de 5xx, latencia base y jitter. El mismo equipo, con la misma
    semilla base, produce siempre los mismos parametros y los mismos datos.

    Se etiqueta la semilla con b"s13" (ademas de --equipo) para que un mismo
    identificador de equipo NO derive los mismos numeros que
    generar_datos_S12.parametros_equipo: son dos flujos de aleatoriedad
    independientes, aunque compartan --equipo y SEED.

    Dos flujos independientes (numpy.random.SeedSequence.spawn): uno para
    elegir los parametros, otro para generar los datos del conjunto."""
    if equipo is None:
        semilla_datos = np.random.SeedSequence([seed_base, zlib.crc32(b"s13-sin-equipo")])
        return {**PARAMS_SIN_EQUIPO, "semilla_datos": semilla_datos}
    validar_equipo(equipo)
    seed_seq = np.random.SeedSequence([seed_base, zlib.crc32(equipo.encode("utf-8")), zlib.crc32(b"s13")])
    semilla_parametros, semilla_datos = seed_seq.spawn(2)
    rng_parametros = np.random.default_rng(semilla_parametros)
    return {
        "total_registros": int(rng_parametros.integers(60, 151)),          # volumen: 60 a 150 medidores
        "page_size": int(rng_parametros.integers(10, 21)),                 # tamano de pagina: 10 a 20
        "prob_429": round(float(rng_parametros.uniform(0.10, 0.25)), 3),   # probabilidad de limite de tasa
        "prob_5xx": round(float(rng_parametros.uniform(0.05, 0.15)), 3),   # probabilidad de error del proveedor
        "latencia_base_ms": round(float(rng_parametros.uniform(80, 300)), 1),
        "jitter_ms": round(float(rng_parametros.uniform(20, 120)), 1),
        "semilla_datos": semilla_datos,
    }


def generar_estado(equipo=None, seed_base=SEED, total_override=None):
    """Genera el DataFrame de estado de facturacion y devuelve tambien los
    parametros de comportamiento de la API derivados para ese equipo.
    total_override permite fijar un total distinto sin pasar por --equipo
    (uso interno para pruebas, no expuesto por linea de comandos)."""
    params = parametros_equipo(equipo, seed_base=seed_base)
    if total_override is not None:
        params = {**params, "total_registros": total_override}
    rng = np.random.default_rng(params["semilla_datos"])
    filas = []
    for i in range(params["total_registros"]):
        meter_id = f"MED{i + 1:04d}"
        sector = SECTORES[i % len(SECTORES)]
        estado = str(rng.choice(ESTADOS_SERVICIO, p=PROB_ESTADOS))
        if estado == "activo":
            saldo = max(0.0, rng.normal(45_000, 15_000))
        else:
            saldo = max(0.0, rng.normal(120_000, 40_000))
        filas.append((meter_id, sector, round(float(saldo), 2), estado, ACTUALIZADO_EN.isoformat()))
    df = pd.DataFrame(filas, columns=COLUMNAS)
    return df, params


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--equipo", choices=EQUIPOS_VALIDOS, default=None,
                     help="Identificador de equipo (lista cerrada); fija total/page-size/prob_429/"
                          "prob_5xx/latencia de forma reproducible")
    ap.add_argument("--total", type=int, default=None, help="Medidores del conjunto (incompatible con --equipo)")
    ap.add_argument("--page-size", type=int, default=None, help="Tamano de pagina (incompatible con --equipo)")
    ap.add_argument("--salida", default="estado_medidores.csv", help="Ruta del CSV de salida")
    ap.add_argument("--seed", type=int, default=SEED, help="Semilla base (fija en 42 por defecto)")
    a = ap.parse_args()

    if a.equipo and (a.total is not None or a.page_size is not None):
        raise SystemExit("--equipo es incompatible con --total/--page-size explicitos: usen uno u otro, no ambos.")

    t0 = time.time()
    df, params = generar_estado(equipo=a.equipo, seed_base=a.seed, total_override=a.total)
    if a.page_size is not None and a.equipo is None:
        params["page_size"] = a.page_size

    Path(a.salida).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(a.salida, index=False)
    tam_kb = Path(a.salida).stat().st_size / 1024
    etiqueta = a.equipo or "sin equipo (ejemplo resuelto)"
    print(f"Equipo: {etiqueta}")
    print(f"  total_registros={params['total_registros']}, page_size={params['page_size']}, "
          f"prob_429={params['prob_429']}, prob_5xx={params['prob_5xx']}, "
          f"latencia_base_ms={params['latencia_base_ms']}, jitter_ms={params['jitter_ms']}")
    print(f"Resumen: {len(df)} filas, {tam_kb:.1f} KB, {time.time() - t0:.2f} s -> {a.salida}")


if __name__ == "__main__":
    main()
