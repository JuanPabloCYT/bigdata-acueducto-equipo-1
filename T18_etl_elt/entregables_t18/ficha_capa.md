# Ficha de la capa consolidada

Para: dirección financiera y equipo de operaciones. Una página, sin jerga.

## Qué contiene

Dos tablas. **Consumo por día y sector**: cuántas lecturas hubo, cuántas son confiables y cuántos metros cúbicos suman en total y solo con las confiables. **Presión por día, sector y nivel** (baja, normal o alta): cuántas lecturas cayeron en cada nivel.

## De dónde viene

De la capa de preparación, que guarda **todas** las lecturas de los medidores tal como llegaron, cada una marcada como confiable o no. Una lectura es confiable cuando el medidor la reportó sin problemas.

## Qué se garantiza

Antes de publicar, una revisión automática (el contrato) compara cada capa con la anterior: las mismas lecturas, el mismo consumo total y una sola fila por día y sector. Si algo no cuadra, la tabla no se publica. Lo probamos con un día dañado a propósito: la revisión lo detuvo, mientras que el tablero anterior lo habría publicado sin aviso, con 1 263 m³ de menos.

## Qué no se garantiza

Que una lectura sea correcta en el medidor. La revisión comprueba que no se pierde ni se inventa nada entre capas, no que el medidor midió bien.

## Por qué hay dos cifras

Guardamos **1 002 240 lecturas**. El tablero cuenta **942 031**, las confiables. Las otras **60 209 (6,0 %)** no se borran: quedan guardadas y marcadas. Por eso hay dos consumos: **160 402 m³** con todas las lecturas y **150 767 m³** con las confiables, que es la cifra del tablero. La diferencia no es un error: son lecturas estimadas o sin señal que se pueden revisar una por una.

## Cómo pedir un recálculo

Escriba al equipo de datos indicando los días afectados. Se rehacen solo esos días. Por ejemplo, rehacer 6 días toca 6 de 120 archivos y deja intactos los demás. Si cambia la regla de qué lectura es confiable, se recalculan las tablas sin volver a cargar los datos de origen.
