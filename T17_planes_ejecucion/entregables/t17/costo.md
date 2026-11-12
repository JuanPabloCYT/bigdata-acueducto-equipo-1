# Costo de leer · equipo equipo-1

IFPN0025 · Big Data e Ingeniería de Datos · Universidad Ean

Un cierre diario que lee más de lo que necesita cuesta cada día. Esta sección convierte lo que medimos en archivos y bytes por cierre, lo proyecta a un año y calcula si cambiar el filtro o el diseño se paga. **No se usa ningún precio por unidad**: la unidad de costo es el archivo abierto, y los bytes leídos van en una segunda columna. Así la comparación no depende de una tarifa inventada.

## 1. Supuestos

| Supuesto | Valor |
|---|---|
| Tamaño por archivo diario (de la bitácora, `tamano_archivos_leidos_b` de A) | 37 605 B (36,7 KiB) |
| Historia inicial de la capa (días ya cargados) $H$ | 120 |
| Cierres proyectados $D$ (un año de cierres diarios) | 365 |
| Crecimiento del tamaño diario | ninguno (348 medidores constantes; supuesto) |
| Precio por unidad (archivo abierto, solicitud de listado) | **no se usa precio**: la unidad es el archivo abierto |

## 2. Archivos y bytes por cierre y en un año

Con partición por día y una historia inicial de $H$ días, un cierre sin poda lee todos los archivos cargados. En $D$ cierres:
$$\sum_{d=H+1}^{H+D} d = \frac{D\,(2H+D+1)}{2} = \frac{365\,(240+366)}{2} = 110\,595\ \text{archivos-día},\qquad \text{razón frente a la poda}=\frac{2H+D+1}{2}=\frac{606}{2}=303.$$

| Diseño y filtro | Archivos por cierre | Bytes por cierre | Archivos abiertos en $D$ cierres | Bytes en $D$ cierres |
|---|---|---|---|---|
| Por día, filtro por las columnas de la ruta (poda) | 1 | 36,7 KiB | 365 | 13,1 MiB |
| Por día, filtro por `fecha` (sin poda) | de 121 a 485 (303 en promedio) | de 4,3 MiB a 17,4 MiB (unos 10,9 MiB en promedio) | 110 595 | 3 966,3 MiB (unos 3,9 GiB) |
| Por mes, cierre del día (filtro por la ruta) | 1 | 818,2 KiB (cota superior: un mes completo) | 365 | unos 291,6 MiB |

Los archivos y bytes por cierre salen de las líneas de A y A-fecha de la bitácora (1 archivo de 37 605 B; 120 archivos de 4 519 275 B en la historia actual) y de la medición del diseño por mes (N2.8a). Los totales salen de la fórmula. La razón sin poda frente a con poda en 365 cierres es **303**.

## 3. Costo de cambiar y punto de equilibrio

Unidad de costo $c$: archivos abiertos por cierre (los bytes son la segunda columna). Punto de equilibrio:
$$n^{*}=\frac{C_{\text{cambio}}}{c_{\text{actual}}-c_{\text{nuevo}}}\quad\text{definido solo si } c_{\text{actual}}>c_{\text{nuevo}};$$
si no, el cambio no se paga y $n^{*}$ es infinito.

| Cambio | Ahorro por cierre | Costo de cambiar | Punto de equilibrio |
|---|---|---|---|
| Corregir el filtro (de `fecha` a las columnas de la ruta) | 302 archivos por cierre en promedio (119 hoy), y unos 10,9 MiB | 0 archivos: es cambiar una línea de la consulta, sin reescribir datos | $n^{*} = 0/302 = $ **0 cierres** (definido: se paga desde el primer cierre) |
| Rediseñar la capa por mes, para el cierre diario | 0 archivos (1 frente a 1), y **+781,5 KiB** leídos por cierre | Reescribir los 120 archivos diarios en 4 mensuales | **$n^{*}$ infinito**: $c_{\text{actual}} = c_{\text{nuevo}} = 1$ archivo, y en bytes el nuevo diseño cuesta 22 veces más (818,2 frente a 36,7 KiB). Nunca se paga. |
| Rediseñar la capa por mes, para el reporte mensual | 30 archivos por reporte (31 frente a 1) | Reescribir 124 archivos | $n^{*} = 124/30 = $ **4,1 reportes** (definido) |

## 4. Recomendación

**Mantener la partición por día y corregir cualquier cierre que filtre por `fecha` para que filtre por `anio`, `mes` y `dia`.** Ese cambio no cuesta nada y evita abrir 110 230 archivos (y unos 3,9 GiB) en un año. Con los mismos medidores, el costo del cierre queda **constante**: 1 archivo y 36,7 KiB por cierre, sin importar cuánta historia acumule la capa. Sin poda, el costo crece con cada día cargado.

**Riesgo que aceptamos:** el reporte mensual abre 31 archivos pequeños en lugar de 1. Con 818 KiB por mes es un costo menor, y el diseño por mes no se justifica para el cierre diario, que es la consulta dominante.

**Cifra que revisaría la recomendación:** que el reporte mensual se ejecute más de unas 4 veces por cada reescritura del diseño (es decir, que domine sobre el cierre), o que el archivo diario crezca por encima de unos 128 MiB, cuando habría que partir cada día en varios archivos y volver a medir.
