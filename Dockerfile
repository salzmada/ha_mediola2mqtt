# ============================================================================
# Dockerfile für mediola2mqtt Home Assistant Add-on
# ============================================================================
ARG BUILD_FROM
# hadolint ignore=DL3006
FROM $BUILD_FROM

# Zeichensatz setzen
ENV LANG C.UTF-8

# Benötigte Python-Pakete installieren (in einer einzigen Schicht)
# hadolint ignore=DL3018
RUN apk add --no-cache \
    python3 \
    py3-pip \
    py3-paho-mqtt \
    py3-requests \
    py3-yaml

# Anwendungsdateien kopieren
COPY mediola2mqtt.py /
COPY run.sh /

# Startskript ausführbar machen
RUN chmod a+x /run.sh

# Startbefehl definieren
CMD [ "/run.sh" ]