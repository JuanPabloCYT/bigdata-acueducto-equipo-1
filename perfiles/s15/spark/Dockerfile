# Imagen del servicio "spark": un entorno de trabajo con Python 3.11,
# Java 11 (Spark necesita una JVM), PySpark 3.5.3 y JupyterLab, listo para
# cargar y agregar el lago del acueducto en modo local.
#
# Por que una imagen propia y no una con Spark ya instalado: aqui se ve de
# que se compone el entorno (Python, JVM, la biblioteca) y se fijan las
# versiones, de modo que las cifras que mida cada equipo sean comparables.
#
# Se construye una sola vez ("docker compose --profile s15 build") y despues
# se reutiliza; no hace falta volver a descargar nada en cada arranque.

# Etapa 1: solo se toma de aqui el entorno de ejecucion de Java 11.
# PySpark 3.5 funciona con Java 8, 11 o 17; se usa 11 por ser la version
# del resto del proyecto.
FROM eclipse-temurin:11-jre-jammy AS java

# Etapa 2: la imagen final, basada en Python 3.11.
FROM python:3.11-slim-bookworm

COPY --from=java /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/java/openjdk/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# procps: Spark lanza algunos procesos auxiliares con herramientas de sistema.
RUN apt-get update \
    && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Usuario sin privilegios: el contenedor no corre como administrador.
RUN useradd --create-home --uid 1000 spark
USER spark
WORKDIR /proyecto

# JupyterLab escucha en el puerto 8888. Sin clave porque el puerto solo se
# publica en el propio equipo (127.0.0.1); en un servidor compartido se
# configura una clave o un token.
EXPOSE 8888 4040 4041
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--ServerApp.token=", "--ServerApp.password=", "--notebook-dir=/proyecto"]
