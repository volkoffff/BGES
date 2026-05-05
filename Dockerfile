FROM python:3.13-slim

# ─── Dépendances système nécessaires pour SDKMAN ──────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    zip \
    unzip \
    bash \
    procps \
    findutils \
    && rm -rf /var/lib/apt/lists/*

# ─── SDKMAN + Temurin 21 LTS ──────────────────────────────────────────────────
ARG JAVA_VERSION=21.0.7-tem
ENV SDKMAN_DIR=/opt/sdkman
SHELL ["/bin/bash", "-c"]

RUN curl -s "https://get.sdkman.io" | bash \
    && source "${SDKMAN_DIR}/bin/sdkman-init.sh" \
    && sdk install java ${JAVA_VERSION} \
    && sdk flush archives \
    && sdk flush temp

ENV JAVA_HOME="${SDKMAN_DIR}/candidates/java/current"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ─── Variables Spark ──────────────────────────────────────────────────────────
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# ─── Dépendances Python ───────────────────────────────────────────────────────
WORKDIR /app

COPY pyproject.toml .

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir \
        pyspark==3.5.5 \
        jupyterlab==4.4.2 \
        ipykernel==6.29.5 \
        pandas==2.2.3 \
        matplotlib==3.10.1 \
        nbstripout==0.8.1

# ─── Ports ────────────────────────────────────────────────────────────────────
# 8888 : Jupyter Lab
# 4040 : Spark UI (driver)
EXPOSE 8888 4040

CMD ["jupyter", "lab", \
     "--ip=0.0.0.0", "--port=8888", \
     "--no-browser", "--allow-root", \
     "--ServerApp.token=''", "--ServerApp.password=''", \
     "--notebook-dir=/app"]
