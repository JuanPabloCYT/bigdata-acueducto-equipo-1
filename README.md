# Pipeline del acueducto · repositorio del equipo equipo-1

IFPN0025 · Big Data e Ingeniería de Datos · Universidad Ean · Autor: Juan Pablo Castro (trabajo individual)

Este repositorio reúne el pipeline del acueducto construido en el Módulo 2:

- ingesta incremental (T12);
- conector de la API (T13);
- captura de cambios (T14);
- carga con Spark (T15);
- capa refinada con DataFrames (T16);
- lectura de planes de ejecución (T17);
- comparación entre transformar antes o después de guardar, con contratos por frontera y reproceso por ventana (T18).

Quien lo clone debe poder levantar la última etapa **sin preguntarle nada al equipo**, siguiendo solo este archivo.

**Identificador del equipo:** `equipo-1` (define la ampliación de datos: 348 medidores, 1 002 240 lecturas y S-SUR como sector dominante).

## 0. Mapa del repositorio

| Carpeta o archivo | Qué contiene |
|---|---|
| `T12_ingesta_incremental/` … `T18_etl_elt/` | Una carpeta por tarea, con la **plantilla de entrega diligenciada** (`T1x_entrega_equipo-1.md`), el cuaderno ejecutado, la evidencia del Nivel 2, los memos y las bitácoras de esa tarea |
| `pipeline/` | Código del pipeline. Escrito o completado por el equipo: `ingesta_incremental.py` (T12), `conector_api_medidores.py` (T13), `carga_spark.py` (T15), `refinada_spark.py` (T16), `planes_s17.py` (T17) y `elt_s18.py` (T18). `ingesta_s12.py`, `conector_s13.py` y `cdc_s14.py` son las soluciones de referencia del paquete del curso, y `verificar_etapa.py` es el verificador del curso |
| `sql/consolidada/`, `contratos/` | Los dos productos ELT en SQL y los dos contratos por frontera (T18) |
| `entregables/t17/`, `entregables/t18/` | Informe de planes, decisiones, costo, glosario y ficha de la capa |
| `bitacora_*.jsonl`, `hipotesis_*.json`, `planes*/`, `comparacion*.csv` | Bitácoras y evidencias que leen los verificadores de T15 a T18 |
| `indice_evidencias.md` | Salida de la verificación de cada etapa, con su commit |
| `datalake/` | Base común del curso (opción A, versionada), con nuestras capas `refinada_spark/` y `consolidada_spark/` |
| `datalake_equipo/` | Capas de la ampliación `equipo-1`. `cruda/` y `refinada/` **no** se versionan: se regeneran con el paso 1 |

## 1. Requisitos

| Requisito | Valor |
|---|---|
| Memoria | 8 GB de RAM en el equipo. El servicio `spark` está limitado a 3 GiB; el pico medido con el cuaderno completo es de 2,2 a 2,3 GiB. **Una sola JVM de Spark a la vez.** |
| Software | Docker con Compose v2 y Git. Python 3.11, Java 11, PySpark 3.5.3, pandas, pyarrow y JupyterLab ya vienen en la imagen. Python 3 en el computador solo se necesita para las verificaciones de T12 a T14 |
| Disco | Unos 3 GB libres (imagen, repositorio y ampliación) |
| Tiempo | Paso 1: de 1 a 3 minutos. Paso 2: unos 70 segundos. Verificación: menos de 1 minuto. Desde un clon limpio, con la imagen ya construida, la secuencia completa tomó unos 126 s. Si hay que construir la imagen, sume unos 3 minutos |

## 2. De dónde sale `datalake/`

La base común (`datalake/`, con 200 medidores y 576 000 lecturas) **no la genera ningún comando de este README**: viene del paquete del curso `BiD_S18_proyecto_base_v1.zip`. Este repositorio usa la **opción A: está versionada**. `datalake/cruda/`, `datalake/refinada/`, `datalake/consolidada/` y `datalake/referencia/` están en el clon, junto con nuestras capas `datalake/refinada_spark/` (T16) y `datalake/consolidada_spark/` (T15). No hace falta ningún paso adicional.

La ampliación del equipo (`datalake_equipo/`) sí se regenera, con los comandos del paso 1 de la sección 3. El generador tiene semilla fija y produce siempre los mismos archivos.

## 3. Levantar la etapa de ETL frente a ELT desde un clon limpio

Ejecuten los comandos **desde la carpeta raíz del repositorio**, en este orden. Apaguen antes cualquier otro perfil de Compose (`docker compose --profile s12 down`).

```bash
git clone https://github.com/JuanPabloCYT/bigdata-acueducto-equipo-1 pipeline-acueducto
cd pipeline-acueducto

docker compose --profile s18 build                  # la primera vez construye la imagen; si ya existe, tarda segundos
docker compose --profile s18 up -d
docker compose --profile s18 ps                     # el servicio spark debe aparecer como healthy

# 1. Ampliacion del equipo (cruda y refinada heredada, que no se versionan), su capa de preparacion, su referencia y la copia alterada
docker compose --profile s18 exec spark python practica/generar_ampliacion_s15.py --equipo equipo-1
docker compose --profile s18 exec spark python pipeline/refinada_spark.py --equipo equipo-1 --volumen ampliacion
docker compose --profile s18 exec spark python practica/generar_referencia_s17.py --equipo equipo-1
docker compose --profile s18 exec spark python practica/inyectar_deriva.py
docker compose --profile s18 exec spark python pipeline/refinada_spark.py --carpeta _deriva --salida _deriva/refinada_spark --bitacora _deriva/bitacora_refinada_spark.jsonl   # termina con codigo 1 a proposito

# 2. Ejecutar el cuaderno de la practica (ya tiene EQUIPO = equipo-1): regenera bitacora_capas.jsonl, comparacion.csv,
#    hipotesis_s18.json y las capas consolidada_elt/ y tablero_etl/ (unos 70 s). Es OBLIGATORIO despues del paso 1:
#    el paso 1 reescribe refinada_spark/ con archivos de nombre nuevo, y la verificacion compara el arbol con la bitacora.
docker compose --profile s18 exec -w /proyecto/practica spark jupyter nbconvert --to notebook --execute --inplace BiD_S18_P4_cuaderno_v1.ipynb

# 3. Verificacion de la etapa
docker compose --profile s18 exec spark python pipeline/verificar_etapa.py --etapa t18 --volumen ampliacion
```

Resultado esperado del paso 3: `Etapa t18: APROBADA (18 de 18 comprobaciones, N s)`, donde `N` es el tiempo de la verificación en segundos.

Los comandos del paso 1 se ejecutan **antes** de abrir el cuaderno o con el cuaderno cerrado. Si `docker compose` falla o no encuentra el servicio, revisen que estén en la carpeta que contiene `docker-compose.yml` y que usen siempre `--profile s18`. Los perfiles `s15` a `s18` levantan el mismo servicio `spark` (contenedor `acueducto_spark_s15`).

### Reconstruir `consolidada_elt/` desde la preparación (T18)

```bash
docker compose --profile s18 exec spark python pipeline/elt_s18.py construir --producto consumo --equipo equipo-1 --volumen ampliacion
docker compose --profile s18 exec spark python pipeline/elt_s18.py construir --producto presion --equipo equipo-1 --volumen ampliacion
docker compose --profile s18 exec spark python pipeline/elt_s18.py contratos --equipo equipo-1 --volumen ampliacion
```

### Reprocesar una ventana de días (T18)

La ventana del equipo es del **2024-01-09 al 2024-01-14**: los 6 días que terminan en el día de mayor consumo, según `ventana_de_equipo`. Se reprocesa con sobrescritura **dinámica**, que reescribe solo las 6 particiones de la ventana y deja intactas las otras 114:

```bash
docker compose --profile s18 exec spark python pipeline/elt_s18.py reprocesar --producto consumo --desde 2024-01-09 --hasta 2024-01-14 --sobrescritura dinamica --equipo equipo-1 --volumen ampliacion
```

El comando trabaja sobre una copia de la salida (`_copias_reproceso/consumo_dia_sector`), que crea si no existe, y anota en `bitacora_capas.jsonl` las particiones intactas, reescritas y perdidas. Si se repite con la misma ventana, el `hash_resultado` es el mismo (`00fc866148…`): el reproceso es idempotente. Nunca usen `--sobrescritura estatica` sobre la capa real: borra las 114 particiones que no están en la ventana.

## 4. Lo particular de este equipo

- **Ampliación y ventana de reproceso:** 348 medidores, 120 días y 1 002 240 lecturas. Sector dominante S-SUR (120 medidores, 34,5 %). Ventana del 2024-01-09 al 2024-01-14.
- **Qué producto quedó en ETL y cuál en ELT:** **los dos en ELT**. En la bitácora, consumo lee 1 382 744 B y presión 401 505 B, frente a 54 371 830 B del ETL por producto. Cambiaría la decisión que la preparación no existiera ($K$ = 113 MB con dos lecturas de la cruda): con 2 productos fijos y reglas estables, el ETL de una pasada sería más barato.
- **Pasos que hubo que agregar a este archivo para que el clon funcione:** (1) la regeneración de `datalake_equipo/cruda` y `refinada` con el generador (paso 1), porque no se versionan; y (2) dejar **obligatoria** la ejecución del cuaderno (paso 2). En la prueba desde un clon limpio, saltarse el paso 2 dejó `t18` en 17 de 18: la comprobación del lago intacto compara el árbol de `refinada_spark/` con el de la bitácora, y el paso 1 lo reescribe con nombres de archivo nuevos.

## 5. Reproducir un producto en otro computador

```bash
docker compose --profile s18 exec spark python pipeline/elt_s18.py construir --producto consumo --volumen ampliacion --equipo equipo-1 --bitacora bitacora_repeticion.jsonl
```

El `hash_resultado` de `bitacora_repeticion.jsonl` debe ser igual al de la última línea `construir` del producto de consumo en `bitacora_capas.jsonl` (`00fc866148f940ee…`). Para T17, el plan se repite con `python pipeline/planes_s17.py --equipo equipo-1 --volumen ampliacion --corridas 1 --hipotesis hipotesis_s17.json --bitacora bitacora_repeticion.jsonl --planes planes_repeticion`.

## 6. Verificar las demás etapas

Las etapas T12 a T14 usan PostgreSQL (perfil `s12`) y **no se levantan junto con el perfil `s18`**: apaguen uno antes de levantar el otro.

```bash
docker compose --profile s18 down
docker compose --profile s12 up -d                  # esperen a healthy
python pipeline/verificar_etapa.py --etapa t12      # tambien t13 y t14
docker compose --profile s12 down
docker compose --profile s18 up -d
docker compose --profile s18 exec spark python pipeline/verificar_etapa.py --etapa t15
docker compose --profile s18 exec spark python pipeline/verificar_etapa.py --etapa t16
docker compose --profile s18 exec spark python pipeline/verificar_etapa.py --etapa t17 --volumen ampliacion
docker compose --profile s18 exec spark python pipeline/verificar_etapa.py --etapa t18 --volumen ampliacion
```

Para t12 a t14, el comando usa `psycopg2` si está instalado en su Python (`pip install -r practica/requirements.txt`) y, si no, ejecuta `psql` dentro del contenedor con el cliente de Docker. Esas tres verificaciones comprueban las tablas de **referencia** del paquete (base `acueducto`). Las cifras de nuestra instancia `equipo-1` (bases `acueducto_equipo_1`) están en las evidencias de cada carpeta `T12` a `T14`, con las instrucciones para reproducirlas.

## 7. Apagar

```bash
docker compose --profile s18 down                   # los archivos del repositorio se conservan
```

## Declaración de uso de inteligencia artificial

Se usó **Claude Code (Anthropic)** para escribir el código de las celdas y funciones por completar, calcular hipótesis antes de medir y redactar la primera versión de los textos (memos, registros de decisión, informes, fichas y glosarios). Todas las cifras salen de ejecutar el código de este repositorio sobre la instancia `equipo-1`. El detalle está en la sección de declaración de cada plantilla de entrega.
