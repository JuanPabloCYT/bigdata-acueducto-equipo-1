# Comparación de costo · ETL frente a ELT

Equipo: `equipo-1` · Volumen: ampliación del equipo (1 002 240 lecturas) · Fuente de las cifras: `bitacora_capas.jsonl` y `comparacion.csv`

## 1. Métrica de bytes

Los bytes se cuentan con **`bytes_columnas_leidas`**: para un CSV, el tamaño de los archivos que se abren; para un Parquet, los bytes comprimidos de las columnas del `ReadSchema`. Es una **cota inferior** de lo que pide el lector, porque con archivos pequeños y un solo grupo de filas por archivo el lector pide el archivo completo. Por ejemplo, la preparación ocupa 4 519 275 B en archivos, y el producto de consumo cuenta solo los 1 382 744 B de sus cuatro columnas. Los bytes de los archivos seleccionados y los de entrada de las etapas quedan en la bitácora como contraste.

## 2. Costos por producto y enfoque

| Producto | Enfoque | Bytes leídos | Bytes escritos | Archivos escritos | Filas descartadas visibles |
|---|---|---|---|---|---|
| consumo | ETL por producto | 54 371 830 | 6 852 | 1 | 0 |
| presión | ETL por producto | 54 371 830 | 6 238 | 1 | 0 |
| ambos | ETL de una pasada | 54 371 830 | 12 996 | 2 | 0 |
| consumo | ETL por ventana (6 días) | 2 718 973 | 8 150 | 6 | 0 |
| consumo | control de formato (ETL sobre Parquet) | 1 382 744 | 7 007 | 1 | 0 |
| presión | control de formato (ETL sobre Parquet) | 2 281 385 | 6 238 | 1 | 0 |
| consumo | **ELT** | **1 382 744** | 275 768 | 120 | **60 209** |
| presión | **ELT** | **401 505** | 253 897 | 120 | **60 209** |

Costo que entra: bytes leídos, bytes escritos, archivos escritos y almacenamiento retenido. La preparación retenida ocupa 4 519 275 B (37 661 B por día, unos 13,1 MiB al año). El tiempo es informativo y no entra en la decisión. **No se usa precio por unidad.**

El **control de formato** separa el efecto del formato del efecto del enfoque. El mismo ETL leyendo Parquet en lugar de CSV lee 1 382 744 B en consumo (39 veces menos que el CSV). En presión lee 2 281 385 B, porque el ETL recalcula la clase desde `presion_bar` (la columna más pesada: 2 076 896 B), mientras que el ELT usa `nivel_presion`, ya calculada (197 016 B). Por eso el ELT de presión lee **5,7 veces menos** que el ETL sobre Parquet.

## 3. Punto de equilibrio

Sean $R_c$ los bytes de la cruda que lee un trabajo ETL (54 371 830 B), $W_e$ lo que escribe (6 852 B en consumo), $r_p$ los bytes de preparación que lee un producto ELT (1 382 744 B), $W_p$ lo que escribe (275 768 B) y $K$ el costo de construir la preparación, con $W_{\text{prep}}$ = 4 519 275 B. Con un trabajo ETL por producto y $n$ productos:

$$C_{\text{ETL}}(n)=n\,(R_c+W_e),\qquad C_{\text{ELT}}(n)=K+n\,(r_p+W_p),\qquad n^{*}=\frac{K}{(R_c+W_e)-(r_p+W_p)}=\frac{K}{54\,378\,682-1\,658\,512}=\frac{K}{52\,720\,170}$$

| Escenario | $K$ (B) | $n^{*}$ frente al ETL por producto | ¿El ELT supera al ETL de una pasada con productos fijos y reglas estables? |
|---|---|---|---|
| Preparación por construir, una lectura ($K=R_c+W_{\text{prep}}$) | 58 891 105 | **1,12 productos** | **No.** Con los 2 productos, el ELT cuesta $K$ + 2 313 914 = 61 205 019 B, frente a 54 384 826 B del ETL de una pasada. |
| Preparación por construir, dos lecturas ($K=2R_c+W_{\text{prep}}$, lo que lee `refinada_spark.py`) | 113 262 935 | **2,15 productos** | **No.** 115 576 849 B frente a 54 384 826 B: le faltan unos 61 MB. |
| Preparación ya existente ($K$ = 0) — **nuestro caso** | 0 | **0**: se paga desde el primer producto | **Sí.** 2 313 914 B frente a 54 384 826 B: 23,5 veces menos. |

La pregunta "¿el ELT supera al ETL de una pasada?" sale como sigue. El ETL de una pasada cuesta $R_c$ + 12 996 B para los dos productos. Con productos fijos y reglas estables, solo lo supera si la preparación ya existe o si se agregan productos: cada producto ELT adicional cuesta unos 0,66 a 1,66 MB, y cada producto ETL adicional en la misma pasada cuesta casi 0, pero obliga a cambiar el trabajo y a releer la cruda completa en cada cambio de regla.

## 4. Supuestos del equipo

- **Retención:** la preparación se conserva completa (120 días = 4,5 MB; 13,1 MiB al año a 37 661 B por día) mientras exista el proyecto.
- **Crecimiento:** ninguno en el número de medidores (348). El volumen crece un día por cierre, igual para todos los enfoques.
- **Productos futuros:** al menos uno más en un año (por ejemplo, consumo por estrato, como la consulta C de T17).
- **Cambios de regla:** al menos uno al año (ya ocurrió uno en la práctica: "ESTIMADO también cuenta").
- **Precio por unidad:** **no se usa precio**. La comparación se hace en bytes y archivos.

## 5. Cifra que cambiaría su decisión

- **$K$ (si la preparación no existiera):** con $K$ = 113 262 935 B y productos fijos, el ETL de una pasada sería más barato que el ELT en unos 61 MB. Si la preparación dejara de mantenerse por otras razones, pasaríamos al ETL de una pasada.
- **Bytes leídos por producto ELT:** si un producto ELT tuviera que leer más de unos **54 MB** de columnas ($r_p$ > $R_c$), es decir, casi todas las columnas de una preparación mucho más ancha, el ELT perdería frente al ETL incluso con la preparación existente.
