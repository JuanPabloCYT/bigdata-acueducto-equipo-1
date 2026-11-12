# Comparación de costo · ETL frente a ELT

Equipo: `@@EQUIPO@@` · Volumen: ampliación del equipo · Fuente de las cifras: `bitacora_capas.jsonl` y `comparacion.csv`

## 1. Métrica de bytes

Los bytes se cuentan con `bytes_columnas_leidas`: para un CSV, el tamaño de los archivos que se abren; para un Parquet, los bytes comprimidos de las columnas del `ReadSchema`. Es una cota inferior de lo que pide el lector, porque con archivos pequeños y un solo grupo de filas el lector pide el archivo completo. Los bytes de los archivos seleccionados y los de entrada de las etapas quedan en la bitácora como contraste.

## 2. Costos por producto y enfoque

| Producto | Enfoque | Bytes leídos | Bytes escritos | Archivos escritos | Filas descartadas visibles |
|---|---|---|---|---|---|
| | | | | | |

Costo que entra: bytes leídos, bytes escritos, archivos escritos y almacenamiento retenido (bytes por tiempo de retención). El tiempo es informativo. Todo precio por unidad es un supuesto del equipo y se rotula como tal.

## 3. Punto de equilibrio

Sean $R_c$ los bytes de la cruda que lee un trabajo ETL, $W_e$ lo que escribe, $r_p$ los bytes de preparación que lee un producto ELT, $W_p$ lo que escribe y $K$ el costo de construir la preparación. Con un trabajo ETL por producto y $n$ productos:

$$C_{\text{ETL}}(n)=n\,(R_c+W_e),\qquad C_{\text{ELT}}(n)=K+n\,(r_p+W_p),\qquad n^{*}=\frac{K}{(R_c+W_e)-(r_p+W_p)}$$

Declaren $K$ con **una** lectura de la cruda ($K=R_c+W_{\text{prep}}$) y con **dos** ($K=2R_c+W_{\text{prep}}$, que es lo que lee la construcción de la capa refinada del proyecto), y el caso **preparación ya existente** ($K=0$).

| Escenario | $K$ (B) | $n^{*}$ frente al ETL por producto | ¿El ELT supera al ETL de una pasada con productos fijos y reglas estables? |
|---|---|---|---|
| Preparación por construir, una lectura | | | |
| Preparación por construir, dos lecturas | | | |
| Preparación ya existente | | | |

## 4. Supuestos del equipo

_(Retención, crecimiento, precios si los usan, productos futuros.)_

## 5. Cifra que cambiaría su decisión

_(Qué valor de la tabla, en qué dirección.)_
