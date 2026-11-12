# Pipeline del acueducto · repositorio del equipo @@EQUIPO@@

IFPN0025 · Big Data e Ingeniería de Datos · Universidad Ean

Este repositorio reúne el pipeline del acueducto construido hasta hoy: ingesta incremental (T12), conector de la API (T13), captura de cambios (T14), carga con Spark (T15), la capa refinada con DataFrames (T16), la lectura de planes de ejecución (T17) y la comparación entre transformar antes o después de guardar, con contratos por frontera y reproceso por ventana (T18). Quien lo clone debe poder levantar la última etapa **sin preguntarle nada al equipo**, siguiendo solo este archivo.

**Identificador del equipo:** `@@EQUIPO@@` (de `equipo-1` a `equipo-6`; define su ampliación de datos).

## 1. Requisitos

| Requisito | Valor |
|---|---|
| Memoria | 8 GB de RAM en el equipo; el servicio `spark` está limitado a 3 GiB. Esperen un pico neto de 1,7 a 2,3 GiB con el cuaderno completo en un núcleo (el tope de 3 GiB es un límite, no lo que la etapa necesita). **Una sola JVM de Spark a la vez:** `spark.stop()` no libera la JVM de un cuaderno, y un script lanzado mientras el cuaderno sigue abierto levanta una segunda, con lo que el consumo se acerca al límite del contenedor |
| Software | Docker con Compose v2 y Git. Python 3.11, Java 11, PySpark 3.5.3, pandas, pyarrow y JupyterLab ya vienen en la imagen |
| Disco | Unos 3 GB libres (imagen, paquete y ampliación) |
| Tiempo | Los comandos del paso 1 de la sección 3, de 3 a 10 minutos; las celdas del cuaderno del Nivel 2, de 7 a 20 minutos; la etapa `t18`, de 30 segundos a 1 minuto. Si hay que construir la imagen, sume unos minutos |

## 2. De dónde sale `datalake/`

La base común (`datalake/`, con 200 medidores y 576 000 lecturas) **no la genera ningún comando de este README**: viene del paquete `BiD_S18_proyecto_base_v1.zip`. La ampliación del equipo (`datalake_equipo/`), su capa de preparación, su referencia y la copia alterada sí se regeneran con los comandos del paso 1 de la sección 3. Este repositorio obtiene la base común así (marquen una opción y borren la otra):

- **Opción A, versionada en el repositorio:** `datalake/cruda/`, `datalake/refinada/`, `datalake/consolidada/`, `datalake/consolidada_spark/`, `datalake/refinada_spark/` y `datalake/referencia/` están en el clon (unos 40 MiB). No hace falta ningún paso adicional.
- **Opción B, extraída del paquete:** el clon no las trae. Antes de los comandos de la sección 3, extraigan **solo esas carpetas** del paquete sobre el clon (así no se sobrescribe `pipeline/`):

  ```bash
  unzip BiD_S18_proyecto_base_v1.zip "datalake/*"
  ```

  En Windows: `Expand-Archive` extrae todo; extraigan el paquete en una carpeta aparte y copien de allí la carpeta `datalake/`.

## 3. Levantar la etapa de ETL frente a ELT desde un clon limpio

Ejecuten los comandos **desde la carpeta raíz del repositorio**, en este orden. Apaguen antes cualquier otro perfil de Compose (`docker compose --profile s12 down`).

```bash
git clone @@URL_DEL_REPOSITORIO@@ pipeline-acueducto
cd pipeline-acueducto

docker compose --profile s18 build                  # la primera vez construye la imagen; si ya existe, tarda segundos
docker compose --profile s18 up -d
docker compose --profile s18 ps                     # el servicio spark debe aparecer como healthy

# 1. Antes de abrir Spark en el cuaderno: ampliación de su equipo, su capa de preparación, su referencia y la copia alterada
docker compose --profile s18 exec spark python practica/generar_ampliacion_s15.py --equipo @@EQUIPO@@
docker compose --profile s18 exec spark python pipeline/refinada_spark.py --equipo @@EQUIPO@@ --volumen ampliacion
docker compose --profile s18 exec spark python practica/generar_referencia_s17.py --equipo @@EQUIPO@@
docker compose --profile s18 exec spark python practica/inyectar_deriva.py
docker compose --profile s18 exec spark python pipeline/refinada_spark.py --carpeta _deriva --salida _deriva/refinada_spark --bitacora _deriva/bitacora_refinada_spark.jsonl   # termina con código 1 a propósito

# 2. Con los comandos anteriores terminados y el cuaderno de la práctica de ETL frente a ELT (carpeta practica/, abierto desde JupyterLab en http://localhost:8888),
#    ejecuten el Nivel 2 con EQUIPO = @@EQUIPO@@: produce bitacora_capas.jsonl, comparacion.csv, hipotesis_s18.json y entregables/t18/

# 3. Verificación de la etapa
docker compose --profile s18 exec spark python pipeline/verificar_etapa.py --etapa t18 --volumen ampliacion
```

Resultado esperado del paso 3: `Etapa t18: APROBADA (18 de 18 comprobaciones, N s)`, donde `N` es el tiempo de la verificación en segundos.

Los comandos del paso 1 se ejecutan **antes** de abrir el cuaderno o con el cuaderno cerrado, nunca mientras tiene una sesión de Spark abierta. Si `docker compose` falla o no encuentra el servicio, revisen que estén en la carpeta que contiene `docker-compose.yml` y que usen siempre `--profile s18`.

## 4. Lo particular de este equipo

Completen en pocas líneas, sin repetir la guía:

- Ampliación de su equipo (medidores, sector dominante) y ventana de reproceso: @@COMPLETAR@@
- Qué producto dejaron por ETL y cuál por ELT, y la cifra de su bitácora que cambiaría esa decisión: @@COMPLETAR@@
- Cualquier paso que hubo que agregar a este archivo para que el clon funcione: @@COMPLETAR@@

## 5. Reproducir un producto en otro computador

Una persona distinta de quien escribió el código repite un producto siguiendo solo este archivo y compara el hash de su resultado:

```bash
docker compose --profile s18 exec spark python pipeline/elt_s18.py construir --producto consumo --volumen ampliacion --equipo @@EQUIPO@@ --bitacora bitacora_repeticion.jsonl
```

El `hash_resultado` de `bitacora_repeticion.jsonl` debe ser igual al de la última línea `construir` del producto de consumo en `bitacora_capas.jsonl`. Anoten en el índice de evidencias quién lo hizo y cuándo.

## 6. Verificar las demás etapas

Las etapas T12 a T14 usan PostgreSQL (perfil `s12`) y **no se levantan junto con el perfil `s18`**: apaguen uno antes de levantar el otro. La salida de cada verificación se pega en `indice_evidencias.md`.

```bash
docker compose --profile s18 down
docker compose --profile s12 up -d                  # esperen a healthy
python pipeline/verificar_etapa.py --etapa t12      # también t13 y t14
docker compose --profile s12 down
docker compose --profile s18 up -d
docker compose --profile s18 exec spark python pipeline/verificar_etapa.py --etapa t15   # también t16, t17 y t18 (en t17 y t18, con --volumen base o ampliacion)
```

Para t12 a t14, el comando usa `psycopg2` si está instalado en su Python (`pip install -r practica/requirements.txt`) y, si no, ejecuta `psql` dentro del contenedor con el cliente de Docker.

## 7. Apagar

```bash
docker compose --profile s18 down                   # los archivos del repositorio se conservan
```
