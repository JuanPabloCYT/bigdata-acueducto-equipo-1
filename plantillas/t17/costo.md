# Costo de leer · equipo @@EQUIPO@@

IFPN0025 · Big Data e Ingeniería de Datos · Universidad Ean

Un cierre diario que lee más de lo que necesita cuesta cada día. Esta sección convierte lo que midieron en archivos y bytes por cierre, lo proyecta a un año y calcula si cambiar el filtro o el diseño se paga. **Ningún precio por unidad se inventa:** si usan uno (por ejemplo, el costo de abrir un archivo o de listar una carpeta), es un parámetro suyo, rotulado como supuesto.

## 1. Supuestos

| Supuesto | Valor |
|---|---|
| Tamaño por archivo diario (de su bitácora, `tamano_archivos_leidos_b` de A) | (complete) |
| Historia inicial de la capa (días ya cargados) $H$ | 120 |
| Cierres proyectados $D$ (un año de cierres diarios) | 365 |
| Crecimiento del tamaño diario | ninguno |
| Precio por unidad (archivo abierto, solicitud de listado), si lo usan | (parámetro suyo, rotulado como supuesto) |

## 2. Archivos y bytes por cierre y en un año

Con partición por día y una historia inicial de $H$ días, un cierre sin poda lee todos los archivos cargados; en $D$ cierres:
$$\sum_{d=H+1}^{H+D} d = \frac{D\,(2H+D+1)}{2}\ \text{archivos-día},\qquad \text{razón frente a la poda}=\frac{2H+D+1}{2}.$$

| Diseño y filtro | Archivos por cierre | Bytes por cierre | Archivos abiertos en $D$ cierres | Bytes en $D$ cierres |
|---|---|---|---|---|
| Por día, filtro por las columnas de la ruta (poda) | | | | |
| Por día, filtro por `fecha` (sin poda) | | | | |
| (Otro diseño que evalúen) | | | | |

Los archivos y bytes por cierre salen de las líneas de A y A-fecha de su bitácora; los totales, de la fórmula.

## 3. Costo de cambiar y punto de equilibrio

Unidad de costo $c$: archivos abiertos por cierre (los bytes son una segunda columna). Punto de equilibrio:
$$n^{*}=\frac{C_{\text{cambio}}}{c_{\text{actual}}-c_{\text{nuevo}}}\quad\text{definido solo si } c_{\text{actual}}>c_{\text{nuevo}};$$
si no, el cambio no se paga y $n^{*}$ es infinito.

| Cambio | Ahorro por cierre | Costo de cambiar | Punto de equilibrio |
|---|---|---|---|
| Corregir el filtro (de `fecha` a las columnas de la ruta) | | | |
| (Cambio de diseño físico que evalúen) | | | |

Incluyan al menos un cambio con $n^{*}$ definido y uno con $n^{*}$ infinito justificado con su cifra.

## 4. Recomendación

Qué recomiendan con criterio económico y de uso de recursos, qué riesgo aceptan y qué cifra los haría revisar la recomendación.
