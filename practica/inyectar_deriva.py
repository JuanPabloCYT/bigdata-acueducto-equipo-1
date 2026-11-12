"""Inyecta una alteracion en una COPIA del lago para practicar la deteccion de deriva en la frontera.

Que hace
    Copia a _deriva/ (en la raiz del paquete, junto a datalake/) las carpetas cruda/, refinada/ y
    consolidada/ del lago, y la tabla totales_control.csv si existe, y altera UN dia de la copia. La capa
    real nunca se toca. Con la copia alterada se puede ver que hace un trabajo que transforma antes de
    guardar y que hace un contrato verificado en la frontera.

    Variantes (--variante):
      coma_entrecomillada  consumo_m3 con coma decimal ENTRECOMILLADA ("0,209"), como la exporta una hoja de
                           calculo configurada en espanol. El archivo sigue siendo un CSV valido de seis
                           columnas, pero ese valor no es un numero: con lectura permisiva queda nulo.
      coma_sin_comillas    consumo_m3 con coma decimal SIN comillas (0,209). Como la coma es el separador,
                           la fila se parte en siete campos y las columnas se corren: es un fallo distinto.
      columna_renombrada   el encabezado cambia consumo_m3 por consumo: un cambio ruidoso, no silencioso.

Como usarlo (desde la carpeta raiz del paquete)
    python practica/inyectar_deriva.py                                   # base, dia 2024-02-15
    python practica/inyectar_deriva.py --dia 2024-02-10
    python practica/inyectar_deriva.py --carpeta datalake_equipo --dia 2024-04-21
    python practica/inyectar_deriva.py --borrar                          # elimina _deriva/

Despues, para reconstruir la preparacion de la copia (con el cuaderno cerrado):
    python pipeline/refinada_spark.py --carpeta _deriva --salida _deriva/refinada_spark \\
           --bitacora _deriva/bitacora_refinada_spark.jsonl
    (termina con codigo distinto de cero porque su reconciliacion contra el CONTROL de la cruda falla; la reconciliacion
    contra la capa heredada puede dar un falso OK, porque esa comparacion ignora los nulos de las sumas)

Ejercicio didactico y produccion
    Aqui se altera una copia a proposito. En produccion la deriva llega sola (un proveedor cambia el
    formato de una exportacion) y se detecta con contratos que se verifican en cada carga, antes de que
    la capa siguiente lea.
"""
import argparse
import csv
import shutil
from pathlib import Path

CAPAS = ["cruda", "refinada", "consolidada"]


def leer_parametros(argv=None):
    ap = argparse.ArgumentParser(description="Altera un dia de una copia del lago.")
    ap.add_argument("--carpeta", default="datalake", help="Lago de origen (datalake o datalake_equipo)")
    ap.add_argument("--raiz", default=".")
    ap.add_argument("--salida", default="_deriva")
    ap.add_argument("--dia", default="2024-02-15", help="Dia a alterar, AAAA-MM-DD")
    ap.add_argument("--variante", choices=["coma_entrecomillada", "coma_sin_comillas", "columna_renombrada"], default="coma_entrecomillada")
    ap.add_argument("--borrar", action="store_true", help="Elimina la copia y termina")
    return ap.parse_args(argv)


def copiar_lago(origen, destino):
    destino.mkdir(parents=True, exist_ok=True)
    for capa in CAPAS:
        if (origen / capa).exists():
            shutil.copytree(origen / capa, destino / capa, dirs_exist_ok=True)
    if (origen / "totales_control.csv").exists():
        shutil.copy2(origen / "totales_control.csv", destino / "totales_control.csv")


def alterar_dia(destino, dia, variante):
    """Reescribe el CSV de un dia de la copia. Devuelve (ruta, filas alteradas)."""
    anio, mes, d = dia.split("-")
    ruta = destino / "cruda" / "acueducto" / f"anio={anio}" / f"mes={mes}" / f"dia={d}" / "lecturas.csv"
    with open(ruta, newline="", encoding="utf-8") as f:
        filas = list(csv.reader(f))
    encabezado, datos = filas[0], filas[1:]
    if variante == "columna_renombrada":
        encabezado = ["consumo" if c == "consumo_m3" else c for c in encabezado]
        with open(ruta, "w", newline="", encoding="utf-8") as f:
            csv.writer(f, lineterminator="\r\n").writerows([encabezado] + datos)
        return ruta, len(datos)
    i = encabezado.index("consumo_m3")
    for fila in datos:
        fila[i] = fila[i].replace(".", ",")
    if variante == "coma_entrecomillada":
        # el escritor del modulo csv entrecomilla solo los campos que contienen el separador
        with open(ruta, "w", newline="", encoding="utf-8") as f:
            csv.writer(f, lineterminator="\r\n", quoting=csv.QUOTE_MINIMAL).writerows([encabezado] + datos)
    else:
        with open(ruta, "w", newline="", encoding="utf-8") as f:
            f.write("\r\n".join(",".join(fila) for fila in [encabezado] + datos) + "\r\n")
    return ruta, len(datos)


def main(argv=None):
    a = leer_parametros(argv)
    destino = Path(a.raiz) / a.salida
    if a.borrar:
        shutil.rmtree(destino, ignore_errors=True)
        print(f"{destino} eliminada")
        return 0
    if destino.exists():
        shutil.rmtree(destino)
    copiar_lago(Path(a.raiz) / a.carpeta, destino)
    ruta, n = alterar_dia(destino, a.dia, a.variante)
    print(f"Copia en {destino}; dia alterado {a.dia} ({a.variante}): {n} filas en {ruta}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
