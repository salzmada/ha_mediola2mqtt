# Dockerfile für mediola2mqtt v0.5

# Verwende feste Version des Home Assistant Base Image
ARG BUILD_FROM #=ghcr.io/home-assistant/amd64-base:9.8.1
FROM ${BUILD_FROM}

# Setze Umgebungsvariablen falls nötig
ENV LANG C.UTF-8

# Zertifikate aktualisieren (vermeidet TLS-Fehler)
RUN apk add --no-cache ca-certificates \
    && update-ca-certificates

# Python + Pip + Basis Libraries installieren
RUN apk add --no-cache python3 py3-pip py3-requests py3-yaml \
    && pip3 install --no-cache-dir paho-mqtt

# Arbeitsverzeichnis setzen
WORKDIR /data

# Addon-Dateien kopieren
COPY run.sh /
COPY mediola2mqtt.py /

# Berechtigungen setzen
RUN chmod a+x /run.sh /mediola2mqtt.py

# Entrypoint / Startscript
CMD [ "/run.sh" ]
