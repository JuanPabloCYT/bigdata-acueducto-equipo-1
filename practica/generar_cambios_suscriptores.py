"""Generador del maestro de suscriptores y de su flujo de cambios (captura de cambios, CDC).

Dos responsabilidades, deliberadamente separadas:

  - poblacion_inicial(equipo): el ESTADO con el que arranca `maestro_suscriptores`
    (es lo que trae ya cargado el archivo 04_esquema_cdc_suscriptores.sql del
    paquete). Los identificadores son sinteticos, de la forma `SUS-0000001`: no hay
    nombre, direccion ni documento reales ni verosimiles, asi que no son datos
    personales (Ley 1581 de 2012).

  - flujo_cambios(equipo): la lista ORDENADA de eventos (alta, modificacion, baja)
    que el cuaderno aplica EN VIVO contra `maestro_suscriptores` ya poblada. Cada alta
    inserta una fila nueva; cada modificacion cambia `sector` o `estrato` y
    `updated_at` de una fila existente; cada baja borra FISICAMENTE una fila
    existente (un DELETE real, no una bandera logica). Asi, a proposito, se produce
    el punto ciego que la practica mide: una fila borrada ya no tiene marca temporal
    que consultar.

Que cambia de un equipo a otro: no el volumen de lecturas ni la tasa de fallos de una
API, sino la PROPORCION DE BAJAS DURAS dentro de la mezcla de cambios (entre 0,05 y
0,35), junto con el tamano del maestro y el numero de cambios. A mayor proporcion, mas
severo el punto ciego que el equipo va a medir.

Semilla fija SEED = 42. La semilla se combina con el identificador del equipo y con la
etiqueta b"s14", de modo que el mismo identificador de equipo no produce los mismos
numeros que los generadores de otras practicas: son flujos de aleatoriedad
independientes aunque compartan `--equipo` y SEED.

Uso sin diferenciar por equipo (ejemplo resuelto del Nivel 1): produce siempre 90
suscriptores y 27 eventos (9 altas, 13 modificaciones y 5 bajas, con
prob_baja_dura = 0,20), para que la guia y las comprobaciones describan siempre el
mismo escenario.
    python generar_cambios_suscriptores.py --salida-maestro maestro.csv --salida-eventos eventos.csv

Uso diferenciado por equipo (Nivel 2, Nivel 3 y reto de diseno): --equipo fija de forma
reproducible el volumen, el numero de cambios y prob_baja_dura. El identificador es de
una lista cerrada (equipo-1 a equipo-6); cualquier otro se rechaza.
    python generar_cambios_suscriptores.py --equipo equipo-3 --salida-maestro m3.csv --salida-eventos e3.csv

Tamano y tiempo: entre 60 y 150 suscriptores y menos de 50 eventos por equipo (unos
pocos KB de CSV); se genera en una fraccion de segundo. El mismo comando, con los
mismos parametros, produce los mismos archivos byte a byte.
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
ESTRATOS = [1, 2, 3, 4, 5, 6]
PROB_ESTRATOS = [0.10, 0.22, 0.28, 0.22, 0.12, 0.06]
COLUMNAS_MAESTRO = ["subscriber_id", "sector", "estrato", "created_at", "updated_at"]
COLUMNAS_EVENTOS = ["orden", "tipo_cambio", "subscriber_id", "sector", "estrato"]

# Dos marcas temporales fijas (nunca datetime.now()): para que los archivos no dependan
# de cuando se ejecuta el generador. FECHA_POBLACION es el momento en que se cargo la
# poblacion inicial (created_at y updated_at valen lo mismo: nadie ha cambiado todavia).
# FECHA_CAMBIOS es el instante unico y compartido en que se aplican todos los eventos
# del flujo dentro del cuaderno.
FECHA_POBLACION = dt.datetime(2024, 6, 1, 6, 0, 0)
FECHA_CAMBIOS = dt.datetime(2024, 6, 2, 9, 0, 0)

# Identificadores de equipo: una lista cerrada, los seis equipos del curso.
EQUIPOS_VALIDOS = [f"equipo-{i}" for i in range(1, 7)]

# Valores fijos del ejemplo resuelto (sin --equipo): deterministas.
PARAMS_SIN_EQUIPO = {
    "total_suscriptores": 90,
    "n_cambios": 27,          # 30 % del total
    "prob_baja_dura": 0.20,
}


def validar_equipo(equipo):
    """Rechaza con un mensaje explicito cualquier identificador fuera de la lista cerrada."""
    if equipo not in EQUIPOS_VALIDOS:
        raise SystemExit(
            f"Equipo '{equipo}' no reconocido. Identificadores validos: {', '.join(EQUIPOS_VALIDOS)}."
        )


def parametros_equipo(equipo, seed_base=SEED):
    """Deriva, de forma reproducible, los parametros que varian por equipo: volumen de
    suscriptores (60 a 150), numero de cambios simulados (20 % a 40 % del volumen, con
    un piso de 6) y proporcion de bajas duras dentro de esa mezcla (0,05 a 0,35). El
    mismo equipo con la misma semilla produce siempre los mismos parametros y datos.

    Se usan tres flujos de aleatoriedad independientes (numpy.random.SeedSequence.spawn):
    uno para elegir los parametros, otro para la poblacion inicial y otro para el flujo
    de cambios. Asi, cambiar la forma de generar uno no altera los otros dos."""
    if equipo is None:
        semilla_poblacion, semilla_cambios = np.random.SeedSequence(
            [seed_base, zlib.crc32(b"s14-sin-equipo")]
        ).spawn(2)
        return {**PARAMS_SIN_EQUIPO, "semilla_poblacion": semilla_poblacion, "semilla_cambios": semilla_cambios}
    validar_equipo(equipo)
    seed_seq = np.random.SeedSequence([seed_base, zlib.crc32(equipo.encode("utf-8")), zlib.crc32(b"s14")])
    semilla_parametros, semilla_poblacion, semilla_cambios = seed_seq.spawn(3)
    rng_parametros = np.random.default_rng(semilla_parametros)
    total_suscriptores = int(rng_parametros.integers(60, 151))
    n_cambios = int(round(total_suscriptores * float(rng_parametros.uniform(0.20, 0.40))))
    prob_baja_dura = round(float(rng_parametros.uniform(0.05, 0.35)), 3)
    return {
        "total_suscriptores": total_suscriptores,
        "n_cambios": max(n_cambios, 6),   # piso minimo para que el flujo no quede vacio
        "prob_baja_dura": prob_baja_dura,
        "semilla_poblacion": semilla_poblacion,
        "semilla_cambios": semilla_cambios,
    }


def poblacion_inicial(equipo=None, seed_base=SEED):
    """Genera el DataFrame de maestro_suscriptores tal como se carga al inicio
    (created_at == updated_at == FECHA_POBLACION). Devuelve tambien los parametros
    derivados para ese equipo."""
    params = parametros_equipo(equipo, seed_base=seed_base)
    rng = np.random.default_rng(params["semilla_poblacion"])
    filas = []
    for i in range(params["total_suscriptores"]):
        subscriber_id = f"SUS-{i + 1:07d}"
        sector = SECTORES[i % len(SECTORES)]
        estrato = int(rng.choice(ESTRATOS, p=PROB_ESTRATOS))
        filas.append((subscriber_id, sector, estrato, FECHA_POBLACION.isoformat(), FECHA_POBLACION.isoformat()))
    df = pd.DataFrame(filas, columns=COLUMNAS_MAESTRO)
    return df, params


def flujo_cambios(equipo=None, seed_base=SEED):
    """Genera la lista ORDENADA de eventos de cambio (alta, modificacion, baja) que se
    aplican EN VIVO contra maestro_suscriptores ya poblada. Ningun subscriber_id se
    repite entre eventos: cada suscriptor de la poblacion recibe, como maximo, un
    evento (asi no se modifica una fila que ya se dio de baja en el mismo flujo). Usa
    la MISMA poblacion (misma semilla) que poblacion_inicial."""
    df_poblacion, params = poblacion_inicial(equipo, seed_base=seed_base)
    rng = np.random.default_rng(params["semilla_cambios"])

    prob_baja = params["prob_baja_dura"]
    prob_resto = 1.0 - prob_baja
    prob_alta = prob_resto * 0.4
    prob_modificacion = prob_resto * 0.6
    n_cambios = params["n_cambios"]
    n_baja = int(round(n_cambios * prob_baja))
    n_modificacion = int(round(n_cambios * prob_modificacion))
    n_alta = max(n_cambios - n_baja - n_modificacion, 0)

    ids_poblacion = df_poblacion["subscriber_id"].to_numpy().copy()
    rng.shuffle(ids_poblacion)
    total_existente_afectado = n_baja + n_modificacion
    if total_existente_afectado > len(ids_poblacion):
        # Salvaguarda: nunca se piden mas cambios sobre filas existentes que filas hay.
        n_baja = min(n_baja, len(ids_poblacion))
        n_modificacion = min(n_modificacion, len(ids_poblacion) - n_baja)
    ids_baja = ids_poblacion[:n_baja]
    ids_modificacion = ids_poblacion[n_baja:n_baja + n_modificacion]

    eventos = []
    orden = 0
    for subscriber_id in ids_baja:
        eventos.append((orden, "baja", subscriber_id, None, None))
        orden += 1
    for subscriber_id in ids_modificacion:
        fila_actual = df_poblacion.loc[df_poblacion["subscriber_id"] == subscriber_id].iloc[0]
        nuevo_sector = SECTORES[int(rng.integers(0, len(SECTORES)))]
        nuevo_estrato = int(rng.choice(ESTRATOS, p=PROB_ESTRATOS))
        # Fuerza que el cambio sea real (al menos un atributo distinto), para que la
        # modificacion sea observable y no una escritura identica.
        intentos = 0
        while nuevo_sector == fila_actual["sector"] and nuevo_estrato == fila_actual["estrato"] and intentos < 10:
            nuevo_sector = SECTORES[int(rng.integers(0, len(SECTORES)))]
            nuevo_estrato = int(rng.choice(ESTRATOS, p=PROB_ESTRATOS))
            intentos += 1
        eventos.append((orden, "modificacion", subscriber_id, nuevo_sector, nuevo_estrato))
        orden += 1
    total_poblacion = params["total_suscriptores"]
    for i in range(n_alta):
        subscriber_id = f"SUS-{total_poblacion + i + 1:07d}"
        sector = SECTORES[int(rng.integers(0, len(SECTORES)))]
        estrato = int(rng.choice(ESTRATOS, p=PROB_ESTRATOS))
        eventos.append((orden, "alta", subscriber_id, sector, estrato))
        orden += 1

    # El orden de aplicacion no altera lo que la captura por consulta puede ver (todos los
    # eventos comparten FECHA_CAMBIOS), pero se mezcla para que el flujo no aplique
    # "primero todas las bajas": es mas fiel a una secuencia real de eventos intercalados.
    rng.shuffle(eventos)
    eventos = [(i, t, s, sec, est) for i, (_, t, s, sec, est) in enumerate(eventos)]

    df_eventos = pd.DataFrame(eventos, columns=COLUMNAS_EVENTOS)
    resumen = {
        "n_alta": n_alta, "n_modificacion": n_modificacion, "n_baja": n_baja,
        "prob_baja_dura": prob_baja,
    }
    return df_eventos, {**params, **resumen}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--equipo", choices=EQUIPOS_VALIDOS, default=None,
                    help="Identificador de equipo (lista cerrada); fija el volumen, el numero de "
                         "cambios y prob_baja_dura de forma reproducible")
    ap.add_argument("--salida-maestro", default="maestro_suscriptores.csv", help="CSV de la poblacion inicial")
    ap.add_argument("--salida-eventos", default="flujo_cambios.csv", help="CSV del flujo de eventos de cambio")
    ap.add_argument("--seed", type=int, default=SEED, help="Semilla base (42 por defecto)")
    a = ap.parse_args()

    t0 = time.time()
    df_poblacion, params = poblacion_inicial(equipo=a.equipo, seed_base=a.seed)
    df_eventos, params_eventos = flujo_cambios(equipo=a.equipo, seed_base=a.seed)

    Path(a.salida_maestro).parent.mkdir(parents=True, exist_ok=True)
    Path(a.salida_eventos).parent.mkdir(parents=True, exist_ok=True)
    df_poblacion.to_csv(a.salida_maestro, index=False)
    df_eventos.to_csv(a.salida_eventos, index=False)

    etiqueta = a.equipo or "sin equipo (ejemplo resuelto)"
    n_total = params_eventos["n_alta"] + params_eventos["n_modificacion"] + params_eventos["n_baja"]
    print(f"Equipo: {etiqueta}")
    print(f"  total_suscriptores={params['total_suscriptores']}, n_cambios={n_total}, "
          f"prob_baja_dura={params_eventos['prob_baja_dura']}")
    print(f"  altas={params_eventos['n_alta']}, modificaciones={params_eventos['n_modificacion']}, "
          f"bajas={params_eventos['n_baja']}")
    print(f"Resumen: {len(df_poblacion)} suscriptores -> {a.salida_maestro}")
    print(f"Resumen: {len(df_eventos)} eventos -> {a.salida_eventos}")
    print(f"Tiempo: {time.time() - t0:.2f} s")


if __name__ == "__main__":
    main()
