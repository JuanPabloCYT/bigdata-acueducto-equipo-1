"""Generador de la ampliacion de volumen de cada equipo para experimentar con Spark.

Que hace
    Crea, en un arbol APARTE llamado datalake_equipo/, una version mas grande del lago
    del acueducto: entre 300 y 600 medidores durante 120 dias, con una lectura por hora.
    Es el TOTAL de la ampliacion, no un incremento: no se suma a los 200 medidores del
    lago base (datalake/), que no se toca. Cada equipo recibe, de forma reproducible,
    un volumen y un reparto de medidores por sector propios.

    Produce:
      datalake_equipo/cruda/acueducto/anio=AAAA/mes=MM/dia=DD/lecturas.csv
      datalake_equipo/refinada/acueducto/anio=AAAA/mes=MM/dia=DD/lecturas.parquet
          (mismas columnas y mismo tipo de marca temporal, en nanosegundos, que la
          capa refinada del lago base)
      datalake_equipo/totales_control.csv
          filas y suma de consumo por dia y sector, SIN filtro de calidad: es la
          referencia contra la que se reconcilia la carga de Spark
      datalake_equipo/parametros_equipo.json
          volumen, sector dominante y fraccion de medidores que recibe

El desbalance entre sectores
    En el lago base cada sector tiene el 25 % de los medidores. Aqui un sector
    "dominante" recibe una fraccion p de los medidores (entre 0,30 y 0,60) y los otros
    tres se reparten el resto por igual. Ese desbalance deliberado permite observar en
    la interfaz de Spark que ocurre cuando una clave concentra mas datos que las demas.
    El numero de particiones de Spark NO es parametro de este generador: es una decision
    de cada equipo.

Identificadores
    Los medidores de la ampliacion se llaman MEQ-0000001, MEQ-0000002, ... Son distintos
    de los MED0001... del lago base, para que nunca se confundan. Aunque los sectores se
    llamen igual, mezclar ambos arboles en un mismo agregado seria un error de ingenieria.

Como usarlo (desde la carpeta raiz del paquete, en su equipo o dentro del contenedor)
    python practica/generar_ampliacion_s15.py --equipo equipo-3
    python practica/generar_ampliacion_s15.py --equipo equipo-3 --salida otra_carpeta

Reproducibilidad
    El mismo equipo produce siempre los mismos parametros y los mismos archivos, byte a
    byte. Los parametros salen de una semilla derivada de la semilla base 42, del
    identificador del equipo y de una etiqueta propia de esta ampliacion. Los datos son
    sinteticos: no hay personas ni predios reales.

Ejercicio didactico y produccion
    Aqui los archivos diarios son diminutos (decenas de KB) para que todo corra en un
    portatil. En un lago real, el tamano y el numero de archivos son una decision de
    diseno del productor, y miles de archivos pequenos encarecen el listado y la lectura.
"""
import argparse
import json
import math
import sys
import time
import zlib
from pathlib import Path

import numpy as np
import pandas as pd

SEED = 42                      # semilla base del proyecto
ETIQUETA = b"s15"              # etiqueta propia: independiente de los otros generadores
SECTORES = ["S-NORTE", "S-SUR", "S-ESTE", "S-OESTE"]
EQUIPOS_VALIDOS = [f"equipo-{i}" for i in range(1, 7)]
DIAS = 120
INICIO = pd.Timestamp("2024-01-01")
MEDIDORES_MIN, MEDIDORES_MAX = 300, 600
P_MIN, P_MAX = 0.30, 0.60
FLAGS = np.array(["OK", "ESTIMADO", "SIN_SENAL"])
P_FLAGS = [0.94, 0.05, 0.01]   # misma mezcla de calidad que el lago base


def validar_equipo(equipo):
    if equipo not in EQUIPOS_VALIDOS:
        raise SystemExit(
            f"Equipo '{equipo}' no reconocido. Identificadores validos: {', '.join(EQUIPOS_VALIDOS)}."
        )


def parametros_equipo(equipo, seed_base=SEED):
    """Deriva los parametros del equipo: medidores, sector dominante y su fraccion.

    Se usan dos flujos de aleatoriedad independientes (SeedSequence.spawn): uno para
    elegir los parametros y otro para generar las lecturas. Asi, si se ajusta el rango
    de parametros, las lecturas de un equipo no cambian por un efecto colateral del
    orden en que se consumen los numeros aleatorios."""
    validar_equipo(equipo)
    seq = np.random.SeedSequence([seed_base, zlib.crc32(equipo.encode("utf-8")), zlib.crc32(ETIQUETA)])
    seq_parametros, seq_datos = seq.spawn(2)
    rng = np.random.default_rng(seq_parametros)
    n = int(rng.integers(MEDIDORES_MIN, MEDIDORES_MAX + 1))
    p_objetivo = float(rng.uniform(P_MIN, P_MAX))
    dominante = SECTORES[int(rng.integers(0, len(SECTORES)))]
    # Los otros tres sectores deben repartirse el resto por igual, asi que el numero
    # de medidores del dominante debe cumplir (n - dominante) divisible por 3.
    candidatos = [d for d in range(math.ceil(P_MIN * n), math.floor(P_MAX * n) + 1) if (n - d) % 3 == 0]
    n_dom = min(candidatos, key=lambda d: abs(d - p_objetivo * n))
    n_otro = (n - n_dom) // 3
    return {
        "equipo": equipo,
        "semilla_base": seed_base,
        "etiqueta": ETIQUETA.decode(),
        "medidores": n,
        "sector_dominante": dominante,
        "medidores_sector_dominante": n_dom,
        "medidores_otro_sector": n_otro,
        "fraccion_dominante": round(n_dom / n, 4),
        "dias": DIAS,
        "inicio": INICIO.strftime("%Y-%m-%d"),
        "lecturas_totales": n * 24 * DIAS,
        "semilla_datos": seq_datos,
    }


def asignar_sectores(params, rng):
    """Sector de cada medidor: el dominante recibe n_dom medidores y los otros tres,
    n_otro cada uno. El orden se baraja para que los identificadores no queden
    agrupados por sector."""
    sectores = [params["sector_dominante"]] * params["medidores_sector_dominante"]
    for s in SECTORES:
        if s != params["sector_dominante"]:
            sectores += [s] * params["medidores_otro_sector"]
    sectores = np.array(sectores)
    return sectores[rng.permutation(len(sectores))]


def generar_dia(dia, n_med, ids, sector_por_medidor, rng):
    """Lecturas de un dia: una fila por medidor y hora, en el mismo orden (por medidor)
    y con las mismas columnas que el lago base."""
    horas = np.arange(24)
    marcas = dia + pd.to_timedelta(horas, unit="h")
    n = n_med * 24
    hora = np.tile(horas, n_med)
    base = 0.12 + 0.08 * np.sin((hora - 6) * np.pi / 12) ** 2   # perfil horario de consumo
    consumo = np.maximum(0.0, base + rng.normal(0, 0.03, n)).round(3)
    presion = rng.normal(3.0, 0.3, n).round(3)
    flag = rng.choice(FLAGS, size=n, p=P_FLAGS)
    return pd.DataFrame({
        "meter_id": np.repeat(ids, 24),
        "timestamp": np.tile(marcas.values, n_med),   # datetime64[ns]: nanosegundos
        "sector": np.repeat(sector_por_medidor, 24),
        "consumo_m3": consumo,
        "presion_bar": presion,
        "quality_flag": flag,
    })


def ruta_particion(raiz, capa, dia, archivo):
    return raiz / capa / "acueducto" / f"anio={dia.year:04d}" / f"mes={dia.month:02d}" / f"dia={dia.day:02d}" / archivo


def generar(equipo, salida, seed_base=SEED):
    salida = Path(salida)
    params = parametros_equipo(equipo, seed_base)
    # Proteccion: este script nunca escribe dentro del lago base ni mezcla equipos.
    if salida.resolve().name == "datalake":
        raise SystemExit("La ampliacion no se escribe en datalake/: use otra carpeta (por omision datalake_equipo).")
    previo = salida / "parametros_equipo.json"
    if previo.exists():
        otro = json.loads(previo.read_text(encoding="utf-8")).get("equipo")
        if otro != equipo:
            raise SystemExit(
                f"{salida} ya contiene la ampliacion de {otro}. Use otra carpeta con --salida "
                f"o borre esa carpeta si quiere generar la de {equipo}."
            )

    seed_datos = params.pop("semilla_datos")
    rng = np.random.default_rng(seed_datos)
    n_med = params["medidores"]
    ids = np.array([f"MEQ-{i:07d}" for i in range(1, n_med + 1)])
    sector_por_medidor = asignar_sectores(params, rng)

    totales = []
    for d in range(DIAS):
        dia = INICIO + pd.Timedelta(days=d)
        df = generar_dia(dia, n_med, ids, sector_por_medidor, rng)

        ruta_csv = ruta_particion(salida, "cruda", dia, "lecturas.csv")
        ruta_pq = ruta_particion(salida, "refinada", dia, "lecturas.parquet")
        ruta_csv.parent.mkdir(parents=True, exist_ok=True)
        ruta_pq.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(ruta_csv, index=False, lineterminator="\n")      # fin de linea fijo: mismos bytes en Windows, macOS y Linux
        df.to_parquet(ruta_pq, index=False)

        # Totales de control SIN filtro de calidad. La suma se hace en milesimas
        # enteras (el consumo tiene tres decimales) para que sea exacta.
        milesimas = (df["consumo_m3"] * 1000).round().astype("int64")
        g = pd.DataFrame({"sector": df["sector"], "milesimas": milesimas}).groupby("sector")["milesimas"].agg(["count", "sum"])
        for sector in SECTORES:
            totales.append({
                "fecha": dia.strftime("%Y-%m-%d"),
                "sector": sector,
                "n_lecturas": int(g.loc[sector, "count"]),
                "consumo_total_m3": round(int(g.loc[sector, "sum"]) / 1000, 3),
            })

    salida.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(totales).to_csv(salida / "totales_control.csv", index=False, lineterminator="\n")
    (salida / "parametros_equipo.json").write_text(
        json.dumps(params, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return params


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--equipo", required=True, choices=EQUIPOS_VALIDOS,
                    help="Identificador de equipo (equipo-1 a equipo-6)")
    ap.add_argument("--salida", default="datalake_equipo", help="Carpeta de la ampliacion (por omision datalake_equipo)")
    a = ap.parse_args()

    t0 = time.time()
    p = generar(a.equipo, a.salida)
    salida = Path(a.salida)
    tam_mib = sum(f.stat().st_size for f in salida.rglob("*") if f.is_file()) / (1024 * 1024)
    print(f"Equipo {p['equipo']}: {p['medidores']} medidores, {p['dias']} dias, {p['lecturas_totales']:,} lecturas")
    print(f"Sector dominante {p['sector_dominante']}: {p['medidores_sector_dominante']} medidores "
          f"({p['fraccion_dominante']:.1%}); cada uno de los otros tres: {p['medidores_otro_sector']}")
    print(f"Escrito en {salida} ({tam_mib:.1f} MiB, {time.time() - t0:.1f} s)")


if __name__ == "__main__":
    main()
