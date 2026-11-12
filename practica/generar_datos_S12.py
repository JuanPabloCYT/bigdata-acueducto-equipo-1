"""Generador de datos sinteticos del acueducto para la practica de ingesta incremental.

Produce lotes diarios de lecturas horarias con dos comportamientos que ocurren
en una ingesta real y que hay que aprender a absorber:
  - Reentregas: el sistema SCADA reenvia lecturas ya cargadas tras un corte de
    comunicacion.
  - Registros tardios: lecturas del dia anterior que llegan un dia despues.

Sirve para ejercitar:
  - Nivel 1: la prueba de doble ejecucion (idempotencia).
  - Nivel 2: el reproceso de una ventana ya cargada sin duplicar.
  - Nivel 3: una fuente legacy sin marca temporal confiable (--incluir-legacy).

Los datos son sinteticos (no hay personas ni predios reales). Semilla fija
SEED = 42: el mismo comando produce siempre los mismos archivos. Esquema de
columnas: meter_id, "timestamp", sector, consumo_m3, presion_bar, quality_flag.

Uso general, sin diferenciar por equipo (es el ejemplo resuelto del Nivel 1):
    python generar_datos_S12.py --medidores 40 --dias 5 --inicio 2024-06-01 --salida lotes
        --formato csv --dup-rate 0.05 --late-rate 0.02

Uso por equipo (Nivel 2 y reto de diseno): --equipo fija, de forma
reproducible, el numero de medidores, la tasa de reentregas y la proporcion de
tardios propios de su equipo. El codigo es el mismo para los seis equipos; lo
que cambia es la instancia de datos. Los identificadores validos son
equipo-1 a equipo-6 (lista cerrada; cualquier otro se rechaza con un mensaje).
    python generar_datos_S12.py --equipo equipo-3 --dias 2 --inicio 2024-06-01
        --salida lotes_equipo-3 --formato csv

--equipo no se combina con --medidores, --dup-rate ni --late-rate: son las
mismas tres cantidades que --equipo ya fija, y mezclarlas dejaria ambiguo cual
valor manda.

Con los mismos parametros el script produce el mismo archivo byte a byte
(semilla fija); pueden comprobarlo comparando el hash SHA-256 de dos corridas.
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
COLUMNAS = ["meter_id", "timestamp", "sector", "consumo_m3", "presion_bar", "quality_flag"]

# Identificadores de equipo: lista cerrada de seis equipos. --equipo rechaza
# cualquier otro valor con un mensaje claro (ver validar_equipo()).
EQUIPOS_VALIDOS = [f"equipo-{i}" for i in range(1, 7)]


def validar_equipo(equipo):
    if equipo not in EQUIPOS_VALIDOS:
        raise SystemExit(
            f"Equipo '{equipo}' no reconocido. Identificadores validos: {', '.join(EQUIPOS_VALIDOS)}."
        )


def parametros_equipo(equipo, seed_base=SEED):
    """Deriva, de forma reproducible y determinista, los tres parametros que
    varian por equipo: volumen de medidores, tasa de reentregas y proporcion
    de tardios. El mismo equipo, con la misma
    semilla base, produce siempre los mismos parametros y los mismos datos.

    Se usan dos flujos de aleatoriedad independientes (numpy.random.SeedSequence
    .spawn): uno para elegir los parametros, otro para generar los datos. Asi,
    si en el futuro se ajusta el rango de parametros, los datos generados con
    la misma semilla de equipo no cambian por un efecto colateral del orden de
    consumo de numeros aleatorios."""
    validar_equipo(equipo)
    seed_seq = np.random.SeedSequence([seed_base, zlib.crc32(equipo.encode("utf-8"))])
    semilla_parametros, semilla_datos = seed_seq.spawn(2)
    rng_parametros = np.random.default_rng(semilla_parametros)
    medidores = int(rng_parametros.integers(20, 41))                    # volumen: 20 a 40 medidores
    dup_rate = round(float(rng_parametros.uniform(0.02, 0.08)), 3)      # tasa de reentregas
    late_rate = round(float(rng_parametros.uniform(0.01, 0.05)), 3)     # proporcion de tardios
    return {
        "medidores": medidores, "dup_rate": dup_rate, "late_rate": late_rate,
        "semilla_datos": semilla_datos,
    }


def perfil_consumo(hora, rng):
    """Perfil horario de consumo domestico: un consumo base con un pico a lo
    largo del dia (funcion seno al cuadrado) mas ruido normal."""
    base = 0.12 + 0.08 * (np.sin((hora - 6) * np.pi / 12) ** 2)
    return max(0.0, base + rng.normal(0, 0.03))


def generar_lote(dia, medidores, rng):
    """Genera las lecturas horarias normales de un dia: 24 filas por medidor."""
    filas = []
    for m in range(medidores):
        meter_id = f"MED{m + 1:04d}"
        sector = SECTORES[m % len(SECTORES)]
        for h in range(24):
            ts = dt.datetime.combine(dia, dt.time(h, 0))
            filas.append((
                meter_id, ts, sector,
                round(perfil_consumo(h, rng), 3),
                round(float(rng.normal(3.0, 0.3)), 3),
                str(rng.choice(["OK", "ESTIMADO", "SIN_SENAL"], p=[0.94, 0.05, 0.01])),
            ))
    return pd.DataFrame(filas, columns=COLUMNAS)


def aplicar_reentregas_y_tardios(df_hoy, lote_anterior, rng, dup_rate, late_rate):
    """Mezcla, dentro del lote del dia, una fraccion de reentregas (filas
    identicas del lote anterior, ya cargadas) y una fraccion de registros
    tardios (otras filas del lote anterior que llegan justo hoy)."""
    piezas = [df_hoy]
    if lote_anterior is not None and dup_rate > 0:
        n = int(len(lote_anterior) * dup_rate)
        if n > 0:
            piezas.append(lote_anterior.sample(n=n, random_state=int(rng.integers(1_000_000))))
    if lote_anterior is not None and late_rate > 0:
        n = int(len(lote_anterior) * late_rate)
        if n > 0:
            piezas.append(lote_anterior.sample(n=n, random_state=int(rng.integers(1_000_000))))
    return pd.concat(piezas, ignore_index=True).sort_values("timestamp").reset_index(drop=True)


def generar_fuente_legacy(rng, medidores=6):
    """Fuente sin marca temporal confiable, para el Nivel 3. Los medidores
    legacy se leen manualmente una vez al dia y solo traen la fecha (sin hora).
    Incluye reentregas exactas (formulario radicado dos veces) y una fila
    trampa: dos lecturas legitimas y distintas con el mismo contenido, el
    mismo dia, para ejercitar el riesgo de la huella de fila descrito en la
    guia (dos consumos nulos en un predio desocupado, capturados por dos
    rutas de lectura que se solapan)."""
    dia = dt.date(2024, 6, 10)
    filas = []
    for m in range(medidores):
        meter_id = f"LEG{m + 1:03d}"
        consumo = round(max(0.0, rng.normal(0.15, 0.05)), 3)
        filas.append((meter_id, dia, SECTORES[m % 4], consumo))
    df = pd.DataFrame(filas, columns=["meter_id", "fecha", "sector", "consumo_m3"])

    # Reentrega exacta: el formulario del medidor LEG001 se radico dos veces
    df = pd.concat([df, df.iloc[[0]]], ignore_index=True)

    # Caso trampa: dos lecturas legitimas y distintas (predio desocupado,
    # dos rutas de lectura), con exactamente el mismo contenido
    trampa = pd.DataFrame([
        {"meter_id": "LEG900", "fecha": dia, "sector": "S-SUR", "consumo_m3": 0.0},
        {"meter_id": "LEG900", "fecha": dia, "sector": "S-SUR", "consumo_m3": 0.0},
    ])
    df = pd.concat([df, trampa], ignore_index=True)
    return df.reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--equipo", choices=EQUIPOS_VALIDOS, default=None,
                     help="Identificador de equipo (lista cerrada equipo-1 a equipo-6); "
                          "fija --medidores/--dup-rate/--late-rate de forma reproducible")
    ap.add_argument("--medidores", type=int, default=None, help="Medidores del acueducto (por defecto 40 sin --equipo)")
    ap.add_argument("--dias", type=int, default=5, help="Lotes diarios a generar (por defecto 5)")
    ap.add_argument("--inicio", default="2024-06-01", help="Fecha del primer lote, AAAA-MM-DD")
    ap.add_argument("--salida", default="lotes", help="Carpeta donde se escriben los lotes")
    ap.add_argument("--formato", choices=["csv", "parquet"], default="csv")
    ap.add_argument("--dup-rate", type=float, default=None, help="Proporcion de reentregas del lote anterior (por defecto 0.05 sin --equipo)")
    ap.add_argument("--late-rate", type=float, default=None, help="Proporcion de registros tardios del lote anterior (por defecto 0.02 sin --equipo)")
    ap.add_argument("--seed", type=int, default=SEED, help="Semilla base del generador (fija en 42 por defecto)")
    ap.add_argument("--incluir-legacy", action="store_true", help="Genera tambien la fuente sin marca temporal confiable del Nivel 3")
    a = ap.parse_args()

    t0 = time.time()
    if a.equipo:
        if a.medidores is not None or a.dup_rate is not None or a.late_rate is not None:
            raise SystemExit(
                "--equipo es incompatible con --medidores/--dup-rate/--late-rate explicitos: "
                "--equipo ya fija esas tres cantidades. Usen uno u otro, no ambos."
            )
        params = parametros_equipo(a.equipo, seed_base=a.seed)
        a.medidores, a.dup_rate, a.late_rate = params["medidores"], params["dup_rate"], params["late_rate"]
        rng = np.random.default_rng(params["semilla_datos"])
        print(f"Equipo {a.equipo} -> medidores={a.medidores}, dup_rate={a.dup_rate}, "
              f"late_rate={a.late_rate} (derivados de forma reproducible de --seed {a.seed})")
    else:
        # Sin --equipo: valores por defecto fijos (40 medidores, 5 % de
        # reentregas, 2 % de tardios) y la semilla base directamente.
        if a.medidores is None:
            a.medidores = 40
        if a.dup_rate is None:
            a.dup_rate = 0.05
        if a.late_rate is None:
            a.late_rate = 0.02
        rng = np.random.default_rng(a.seed)

    inicio = dt.date.fromisoformat(a.inicio)
    out = Path(a.salida)
    out.mkdir(parents=True, exist_ok=True)

    lote_anterior = None
    total_filas = 0
    for d in range(a.dias):
        dia = inicio + dt.timedelta(days=d)
        df_dia = generar_lote(dia, a.medidores, rng)
        df_final = aplicar_reentregas_y_tardios(df_dia, lote_anterior, rng, a.dup_rate, a.late_rate)
        # Nombre con indice de dia (no la fecha ISO) para evitar ambiguedad de separador
        nombre = out / f"lote_dia{d + 1:02d}.{a.formato}"
        if a.formato == "csv":
            df_final.to_csv(nombre, index=False)
        else:
            df_final.to_parquet(nombre, index=False)
        total_filas += len(df_final)
        lote_anterior = df_dia  # el lote "limpio" del dia es la fuente de reentregas/tardios del dia siguiente
        print(f"{nombre.name} ({dia.isoformat()}): {len(df_final)} filas ({len(df_dia)} propias del dia)")

    if a.incluir_legacy:
        df_legacy = generar_fuente_legacy(rng, medidores=6)
        nombre_legacy = out / f"fuente_legacy.{a.formato}"
        if a.formato == "csv":
            df_legacy.to_csv(nombre_legacy, index=False)
        else:
            df_legacy.to_parquet(nombre_legacy, index=False)
        print(f"{nombre_legacy.name}: {len(df_legacy)} filas (sin marca temporal confiable, para el Nivel 3)")

    tam_mb = sum(f.stat().st_size for f in out.glob(f"*.{a.formato}")) / (1024 * 1024)
    print(f"Resumen: {a.dias} lotes diarios, {total_filas} filas de lecturas, {tam_mb:.3f} MB, {time.time() - t0:.1f} s")


if __name__ == "__main__":
    main()
