#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ============================================================================
# mediola2mqtt.py
# (c) 2021 Andreas Böhler
# License: Apache 2.0
#
# Brücke zwischen Mediola AIO Gateway (v4/v5/v6) und MQTT.
# Empfängt UDP-Statuspakete vom Mediola-Gateway und leitet sie als
# MQTT-Nachrichten weiter. Empfängt MQTT-Befehle und sendet sie per
# HTTP an das Gateway.
# ============================================================================

import paho.mqtt.client as mqtt
import socket
import json
import yaml
import os
import sys
import signal
import logging
import time
from datetime import datetime
from typing import Optional, Tuple, Any, Dict, List, Union

import requests
from requests.exceptions import RequestException, Timeout, ConnectionError

# ============================================================================
# Logging-Konfiguration
# ============================================================================

def setup_logging(debug: bool = False) -> logging.Logger:
    """
    Richtet das Logging-System ein.

    Args:
        debug: Wenn True, wird DEBUG-Level aktiviert, sonst INFO.

    Returns:
        Konfigurierter Logger.
    """
    logger = logging.getLogger("mediola2mqtt")
    logger.setLevel(logging.DEBUG if debug else logging.INFO)

    # Verhindere doppelte Handler bei mehrfachem Aufruf
    if logger.handlers:
        logger.handlers.clear()

    # Konsolen-Handler mit Timestamp-Format
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG if debug else logging.INFO)

    # Format: Timestamp - Level - Nachricht
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)-8s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


# Globaler Logger – wird nach Konfigurationsladung neu konfiguriert
log = setup_logging(debug=False)

# ============================================================================
# Konfiguration laden
# ============================================================================

def load_config() -> dict:
    """
    Lädt die Konfiguration aus einer der unterstützten Quellen.
    Priorität:
      1. /data/options.json        (Home Assistant Add-on Modus)
      2. /config/mediola2mqtt.yaml (Legacy Add-on Modus)
      3. ./mediola2mqtt.yaml       (Lokaler Modus)

    Returns:
        Dictionary mit der geladenen Konfiguration.

    Raises:
        SystemExit: Wenn keine Konfigurationsdatei gefunden wird.
    """
    config_sources = [
        ("/data/options.json", "json", "Home Assistant Add-on Modus"),
        ("/config/mediola2mqtt.yaml", "yaml", "Legacy Add-on Modus"),
        ("mediola2mqtt.yaml", "yaml", "Lokaler Modus"),
    ]

    for filepath, filetype, mode_name in config_sources:
        if os.path.exists(filepath):
            log.info("Konfigurationsdatei gefunden: %s (%s)", filepath, mode_name)
            try:
                with open(filepath, "r", encoding="utf-8") as fp:
                    if filetype == "json":
                        config = json.load(fp)
                    else:
                        config = yaml.safe_load(fp)
                log.info("Konfiguration erfolgreich geladen.")
                return config
            except json.JSONDecodeError as e:
                log.error("Fehler beim Parsen der JSON-Konfiguration '%s': %s", filepath, e)
                sys.exit(1)
            except yaml.YAMLError as e:
                log.error("Fehler beim Parsen der YAML-Konfiguration '%s': %s", filepath, e)
                sys.exit(1)
            except IOError as e:
                log.error("Fehler beim Lesen der Konfigurationsdatei '%s': %s", filepath, e)
                sys.exit(1)

    log.error("Keine Konfigurationsdatei gefunden. Programm wird beendet.")
    sys.exit(1)


def validate_config(config: dict) -> bool:
    """
    Überprüft, ob die Konfiguration die minimal benötigten Felder enthält.

    Args:
        config: Das Konfigurationsdictionary.

    Returns:
        True, wenn die Konfiguration gültig ist.

    Raises:
        SystemExit: Bei ungültiger Konfiguration.
    """
    # MQTT-Konfiguration ist zwingend erforderlich
    if "mqtt" not in config:
        log.error("MQTT-Konfiguration fehlt in der Konfigurationsdatei.")
        sys.exit(1)

    required_mqtt_fields = ["host", "port", "topic"]
    for field in required_mqtt_fields:
        if field not in config["mqtt"]:
            log.error("Pflichtfeld 'mqtt.%s' fehlt in der Konfiguration.", field)
            sys.exit(1)

    # Mediola-Konfiguration ist zwingend erforderlich
    if "mediola" not in config:
        log.error("Mediola-Konfiguration fehlt in der Konfigurationsdatei.")
        sys.exit(1)

    # Debug-Flag als Boolean sicherstellen (Default: False)
    config["mqtt"]["debug"] = bool(config["mqtt"].get("debug", False))

    # Discovery-Prefix mit Default belegen
    if "discovery_prefix" not in config["mqtt"]:
        config["mqtt"]["discovery_prefix"] = "homeassistant"

    log.info("Konfigurationsvalidierung erfolgreich.")
    return True


# ============================================================================
# DNS-Cache für Mediola-Hosts
# ============================================================================

class DNSCache:
    """
    Einfacher DNS-Cache, um wiederholte DNS-Lookups bei jedem
    empfangenen UDP-Paket zu vermeiden.
    Einträge verfallen nach `ttl` Sekunden.
    """

    def __init__(self, ttl: int = 300):
        """
        Args:
            ttl: Time-to-Live für Cache-Einträge in Sekunden.
        """
        self._cache: Dict[str, Tuple[str, float]] = {}
        self._ttl = ttl

    def resolve(self, hostname: str) -> Optional[str]:
        """
        Löst einen Hostnamen in eine IP-Adresse auf (mit Caching).

        Args:
            hostname: Der aufzulösende Hostname.

        Returns:
            Die IP-Adresse als String oder None bei Fehler.
        """
        now = time.monotonic()

        # Prüfe Cache
        if hostname in self._cache:
            ip, timestamp = self._cache[hostname]
            if now - timestamp < self._ttl:
                return ip
            else:
                log.debug("DNS-Cache abgelaufen für '%s', erneuere...", hostname)

        # DNS-Lookup durchführen
        try:
            ip = socket.gethostbyname(hostname)
            self._cache[hostname] = (ip, now)
            log.debug("DNS aufgelöst: %s -> %s", hostname, ip)
            return ip
        except socket.gaierror as e:
            log.error("DNS-Auflösung fehlgeschlagen für '%s': %s", hostname, e)
            return None


# Globaler DNS-Cache
dns_cache = DNSCache(ttl=300)

# ============================================================================
# Lookup-Indices für schnellere Suche
# ============================================================================

class DeviceLookup:
    """
    Baut Index-Strukturen auf, um Geräte (Blinds, Buttons) schnell
    anhand von Typ+Adresse nachschlagen zu können, anstatt bei jedem
    Paket alle Listen linear zu durchsuchen.
    """

    def __init__(self, config: dict):
        """
        Args:
            config: Die geladene Konfiguration.
        """
        self._config = config
        self._blind_index: Dict[str, List[dict]] = {}
        self._button_index: Dict[str, List[dict]] = {}
        self._build_indices()

    def _build_indices(self):
        """Erstellt Lookup-Dictionaries für Blinds und Buttons."""
        if "blinds" in self._config:
            for blind in self._config["blinds"]:
                key = (blind["type"] + "_" + blind["adr"]).lower()
                if key not in self._blind_index:
                    self._blind_index[key] = []
                self._blind_index[key].append(blind)
            log.debug("Blind-Index erstellt: %d Einträge", len(self._blind_index))

        if "buttons" in self._config:
            for button in self._config["buttons"]:
                key = (button["type"] + "_" + button["adr"]).lower()
                if key not in self._button_index:
                    self._button_index[key] = []
                self._button_index[key].append(button)
            log.debug("Button-Index erstellt: %d Einträge", len(self._button_index))

    def find_blinds(self, dtype: str, adr: str) -> List[dict]:
        """
        Findet alle Blind-Konfigurationen für einen gegebenen Typ und Adresse.

        Args:
            dtype: Gerätetyp (z.B. 'RT', 'ER').
            adr: Geräteadresse.

        Returns:
            Liste passender Blind-Konfigurationen.
        """
        key = (dtype + "_" + adr).lower()
        return self._blind_index.get(key, [])

    def find_buttons(self, dtype: str, adr: str) -> List[dict]:
        """
        Findet alle Button-Konfigurationen für einen gegebenen Typ und Adresse.

        Args:
            dtype: Gerätetyp (z.B. 'IT').
            adr: Geräteadresse.

        Returns:
            Liste passender Button-Konfigurationen.
        """
        key = (dtype + "_" + adr).lower()
        return self._button_index.get(key, [])


# ============================================================================
# Mediola-Host-Auflösung
# ============================================================================

def get_mediola_host(config: dict, mediolaid: str) -> Optional[str]:
    """
    Ermittelt den Hostnamen/IP für eine gegebene Mediola-ID.

    Args:
        config: Die geladene Konfiguration.
        mediolaid: Die Mediola-Geräte-ID.

    Returns:
        Den Host als String oder None, wenn nicht gefunden.
    """
    if isinstance(config["mediola"], list):
        for mediola in config["mediola"]:
            if mediola["id"] == mediolaid:
                return mediola["host"]
        return None
    else:
        return config["mediola"].get("host")


def get_mediola_password(config: dict, mediolaid: str) -> Optional[str]:
    """
    Ermittelt das Passwort für eine gegebene Mediola-ID.

    Args:
        config: Die geladene Konfiguration.
        mediolaid: Die Mediola-Geräte-ID.

    Returns:
        Das Passwort als String oder None.
    """
    if isinstance(config["mediola"], list):
        for mediola in config["mediola"]:
            if mediola["id"] == mediolaid:
                return mediola.get("password", "")
        return ""
    else:
        return config["mediola"].get("password", "")


def get_mediolaid_by_address(config: dict, addr: tuple) -> str:
    """
    Ermittelt die Mediola-ID anhand der Absender-IP-Adresse eines
    empfangenen UDP-Pakets.

    Args:
        config: Die geladene Konfiguration.
        addr: Tuple (ip, port) des Absenders.

    Returns:
        Die Mediola-ID als String. Fallback: 'mediola'.
    """
    default_id = "mediola"

    if not isinstance(config["mediola"], list):
        return default_id

    sender_ip = addr[0]
    for mediola in config["mediola"]:
        host = mediola.get("host", "")
        resolved_ip = dns_cache.resolve(host)
        if resolved_ip and resolved_ip == sender_ip:
            log.debug(
                "Mediola-Gateway identifiziert: ID='%s', Host='%s', IP='%s'",
                mediola["id"], host, sender_ip
            )
            return mediola["id"]

    log.warning(
        "Kein Mediola-Gateway für Absender-IP '%s' gefunden. "
        "Verwende Standard-ID '%s'.",
        sender_ip, default_id
    )
    return default_id


# ============================================================================
# MQTT Callback-Funktionen
# ============================================================================

def on_connect(client: mqtt.Client, userdata: Any, flags: dict, rc: int):
    """
    Callback bei MQTT-Verbindungsaufbau.
    Wird automatisch aufgerufen, wenn die Verbindung zum Broker
    hergestellt (oder abgelehnt) wird.

    Args:
        client: Die MQTT-Client-Instanz.
        userdata: Benutzerdefinierte Daten (hier: config dict).
        flags: Response-Flags vom Broker.
        rc: Result-Code (0 = Erfolg).
    """
    connect_statuses = {
        0: "Verbindung erfolgreich hergestellt",
        1: "Verbindung abgelehnt: Inkorrektes Protokoll",
        2: "Verbindung abgelehnt: Ungültige Client-ID",
        3: "Verbindung abgelehnt: Server nicht verfügbar",
        4: "Verbindung abgelehnt: Benutzername oder Passwort ungültig",
        5: "Verbindung abgelehnt: Nicht autorisiert",
    }

    status = connect_statuses.get(rc, f"Unbekannter Fehlercode: {rc}")

    if rc != 0:
        log.error("MQTT-Verbindung fehlgeschlagen: %s (rc=%d)", status, rc)
    else:
        log.info("MQTT: %s", status)
        # Discovery erst nach erfolgreicher Verbindung einrichten
        config = userdata
        setup_discovery(client, config)


def on_disconnect(client: mqtt.Client, userdata: Any, rc: int):
    """
    Callback bei MQTT-Verbindungstrennung.

    Args:
        client: Die MQTT-Client-Instanz.
        userdata: Benutzerdefinierte Daten.
        rc: Result-Code (0 = geplante Trennung).
    """
    if rc != 0:
        log.warning(
            "Unerwartete MQTT-Verbindungstrennung (rc=%d). "
            "Automatischer Reconnect wird versucht.",
            rc
        )
    else:
        log.info("MQTT-Verbindung planmäßig getrennt.")


def on_message(client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage):
    """
    Callback bei eingehender MQTT-Nachricht (Steuerbefehl).
    Verarbeitet Befehle wie 'open', 'close', 'stop' und sendet
    entsprechende HTTP-Requests an das Mediola-Gateway.

    Args:
        client: Die MQTT-Client-Instanz.
        userdata: Benutzerdefinierte Daten (hier: config dict).
        msg: Die empfangene MQTT-Nachricht.
    """
    config = userdata

    log.debug(
        "MQTT-Nachricht empfangen: Topic='%s', QoS=%d, Payload='%s'",
        msg.topic, msg.qos, msg.payload.decode("utf-8", errors="replace")
    )

    # Topic-Struktur parsen:
    # <prefix>/blinds/<mediolaid>/<type>_<adr>/set
    try:
        dtype, adr = msg.topic.split("_")
        mediolaid = dtype.split("/")[-2]
        dtype = dtype[dtype.rfind("/") + 1:]
        adr = adr[:adr.find("/")]
    except (ValueError, IndexError) as e:
        log.error(
            "Konnte MQTT-Topic '%s' nicht parsen: %s",
            msg.topic, e
        )
        return

    # Passendes Blind in der Konfiguration suchen
    for blind in config.get("blinds", []):
        if dtype != blind["type"] or adr != blind["adr"]:
            continue

        # Bei Multi-Mediola-Setup: Prüfen ob die Mediola-ID übereinstimmt
        if isinstance(config["mediola"], list):
            if blind.get("mediola") != mediolaid:
                continue

        # Befehlsdaten für das Gateway zusammenbauen
        command = msg.payload.decode("utf-8", errors="replace").strip().lower()
        data = _build_command_data(dtype, adr, command)

        if data is None:
            log.warning(
                "Unbekannter Befehl '%s' für Gerät Typ='%s', Adr='%s'",
                command, dtype, adr
            )
            return

        # HTTP-Payload zusammenbauen
        payload = {
            "XC_FNC": "SendSC",
            "type": dtype,
            "data": data,
        }

        # Host und Passwort ermitteln
        host = get_mediola_host(config, mediolaid if isinstance(config["mediola"], list) else "")
        password = get_mediola_password(config, mediolaid if isinstance(config["mediola"], list) else "")

        if not host:
            # Fallback für Single-Mediola-Konfiguration
            if not isinstance(config["mediola"], list):
                host = config["mediola"].get("host", "")
                password = config["mediola"].get("password", "")

        if not host:
            log.error(
                "Kein Mediola-Host gefunden für ID='%s'. "
                "Befehl kann nicht gesendet werden.",
                mediolaid
            )
            return

        if password:
            payload["XC_PASS"] = password

        # HTTP-Request an das Mediola-Gateway senden
        _send_to_mediola(host, payload, config)
        return

    log.warning(
        "Kein passendes Gerät gefunden für Topic='%s' "
        "(Typ='%s', Adr='%s', MediolaID='%s')",
        msg.topic, dtype, adr, mediolaid
    )


def _build_command_data(dtype: str, adr: str, command: str) -> Optional[str]:
    """
    Erstellt den Daten-String für einen Befehl an das Mediola-Gateway.

    Args:
        dtype: Gerätetyp ('RT' oder 'ER').
        adr: Geräteadresse.
        command: Befehl ('open', 'close', 'stop').

    Returns:
        Den Daten-String oder None bei unbekanntem Befehl/Typ.
    """
    # Kommando-Mapping: (typ, befehl) -> Daten-Format
    command_map = {
        ("RT", "open"):  lambda a: "20" + a,
        ("RT", "close"): lambda a: "40" + a,
        ("RT", "stop"):  lambda a: "10" + a,
        ("ER", "open"):  lambda a: format(int(a), "02x") + "01",
        ("ER", "close"): lambda a: format(int(a), "02x") + "00",
        ("ER", "stop"):  lambda a: format(int(a), "02x") + "02",
    }

    key = (dtype, command)
    builder = command_map.get(key)

    if builder is None:
        return None

    try:
        return builder(adr)
    except (ValueError, TypeError) as e:
        log.error(
            "Fehler beim Erstellen der Befehlsdaten: Typ='%s', Adr='%s', "
            "Befehl='%s', Fehler: %s",
            dtype, adr, command, e
        )
        return None


def _send_to_mediola(host: str, payload: dict, config: dict):
    """
    Sendet einen HTTP-GET-Request an das Mediola-Gateway.

    Args:
        host: Hostname oder IP des Gateways.
        payload: Die zu sendenden Parameter.
        config: Die geladene Konfiguration (für Debug-Logging).
    """
    url = f"http://{host}/command"

    # Passwort aus den Log-Daten entfernen
    safe_payload = {k: v for k, v in payload.items() if k != "XC_PASS"}

    log.debug(
        "Sende HTTP-Request an Mediola-Gateway: URL='%s', Parameter=%s",
        url, safe_payload
    )

    try:
        response = requests.get(
            url,
            params=payload,
            headers={"Connection": "close"},
            timeout=10  # 10 Sekunden Timeout
        )
        response.raise_for_status()

        log.debug(
            "Mediola-Gateway Antwort: Status=%d, Body='%s'",
            response.status_code,
            response.text[:200]  # Maximal 200 Zeichen loggen
        )
        log.info(
            "Befehl erfolgreich an Mediola gesendet: Host='%s', "
            "Funktion='%s', Typ='%s', Daten='%s'",
            host,
            payload.get("XC_FNC"),
            payload.get("type"),
            payload.get("data"),
        )

    except Timeout:
        log.error(
            "Timeout beim Senden an Mediola-Gateway '%s'. "
            "Gateway möglicherweise nicht erreichbar.",
            host
        )
    except ConnectionError as e:
        log.error(
            "Verbindungsfehler zum Mediola-Gateway '%s': %s",
            host, e
        )
    except RequestException as e:
        log.error(
            "HTTP-Fehler beim Senden an Mediola-Gateway '%s': %s",
            host, e
        )


def on_publish(client: mqtt.Client, userdata: Any, mid: int):
    """
    Callback nach erfolgreichem MQTT-Publish (nur bei Debug aktiv).

    Args:
        client: Die MQTT-Client-Instanz.
        userdata: Benutzerdefinierte Daten.
        mid: Message-ID der veröffentlichten Nachricht.
    """
    log.debug("MQTT-Nachricht veröffentlicht: MID=%d", mid)


def on_subscribe(client: mqtt.Client, userdata: Any, mid: int, granted_qos: tuple):
    """
    Callback nach erfolgreichem MQTT-Subscribe.

    Args:
        client: Die MQTT-Client-Instanz.
        userdata: Benutzerdefinierte Daten.
        mid: Message-ID der Subscription.
        granted_qos: Gewährtes QoS-Level.
    """
    log.debug("MQTT-Subscription bestätigt: MID=%d, QoS=%s", mid, granted_qos)


def on_log(client: mqtt.Client, userdata: Any, level: int, string: str):
    """
    Callback für interne MQTT-Client-Logs (nur bei Debug aktiv).

    Args:
        client: Die MQTT-Client-Instanz.
        userdata: Benutzerdefinierte Daten.
        level: Log-Level des MQTT-Clients.
        string: Log-Nachricht.
    """
    log.debug("MQTT-Client intern: %s", string)


# ============================================================================
# MQTT Discovery Setup
# ============================================================================

def setup_discovery(client: mqtt.Client, config: dict):
    """
    Richtet die MQTT Home Assistant Discovery-Einträge ein.
    Registriert alle konfigurierten Geräte (Buttons, Blinds) beim
    Home Assistant über das MQTT Discovery-Protokoll.

    Args:
        client: Die MQTT-Client-Instanz.
        config: Die geladene Konfiguration.
    """
    log.info("Starte MQTT Discovery Setup...")

    _setup_button_discovery(client, config)
    _setup_blind_discovery(client, config)

    log.info("MQTT Discovery Setup abgeschlossen.")


def _setup_button_discovery(client: mqtt.Client, config: dict):
    """
    Erstellt Discovery-Einträge für alle konfigurierten Buttons.
    Buttons werden als MQTT Device Triggers registriert.

    Args:
        client: Die MQTT-Client-Instanz.
        config: Die geladene Konfiguration.
    """
    if "buttons" not in config:
        log.debug("Keine Buttons konfiguriert, überspringe Button-Discovery.")
        return

    for button in config["buttons"]:
        identifier = button["type"] + "_" + button["adr"]
        mediolaid = "mediola"

        # Host für diesen Button ermitteln
        host = ""
        if isinstance(config["mediola"], list):
            mediolaid = button.get("mediola", "mediola")
            host = get_mediola_host(config, mediolaid) or ""
        else:
            host = config["mediola"].get("host", "")

        if not host:
            log.error(
                "Kein Mediola-Host für Button '%s' gefunden. "
                "Discovery-Eintrag wird übersprungen.",
                identifier
            )
            continue

        device_id = "mediola_buttons_" + host.replace(".", "")
        discovery_topic = (
            f"{config['mqtt']['discovery_prefix']}/device_automation/"
            f"{mediolaid}_{identifier}/config"
        )
        state_topic = (
            f"{config['mqtt']['topic']}/buttons/{mediolaid}/{identifier}"
        )

        # Name des Buttons ermitteln (Bug-Fix: war vorher 'type' statt Button-Dict)
        name = button.get("name", "Mediola Button")

        payload = {
            "automation_type": "trigger",
            "topic": state_topic,
            "type": "button_short_press",
            "subtype": "button_1",
            "device": {
                "identifiers": device_id,
                "manufacturer": "Mediola",
                "name": name,
            },
        }

        payload_json = json.dumps(payload)
        client.publish(discovery_topic, payload=payload_json, retain=True)

        log.debug(
            "Button Discovery veröffentlicht: ID='%s', Topic='%s'",
            identifier, discovery_topic
        )


def _setup_blind_discovery(client: mqtt.Client, config: dict):
    """
    Erstellt Discovery-Einträge für alle konfigurierten Blinds/Rollos.
    Blinds werden als MQTT Cover registriert.

    Args:
        client: Die MQTT-Client-Instanz.
        config: Die geladene Konfiguration.
    """
    if "blinds" not in config:
        log.debug("Keine Blinds konfiguriert, überspringe Blind-Discovery.")
        return

    for blind in config["blinds"]:
        identifier = blind["type"] + "_" + blind["adr"]
        mediolaid = "mediola"

        # Host für diesen Blind ermitteln
        host = ""
        if isinstance(config["mediola"], list):
            mediolaid = blind.get("mediola", "mediola")
            host = get_mediola_host(config, mediolaid) or ""
        else:
            host = config["mediola"].get("host", "")

        if not host:
            log.error(
                "Kein Mediola-Host für Blind '%s' gefunden. "
                "Discovery-Eintrag wird übersprungen.",
                identifier
            )
            continue

        device_id = "mediola_blinds_" + host.replace(".", "")
        discovery_topic = (
            f"{config['mqtt']['discovery_prefix']}/cover/"
            f"{mediolaid}_{identifier}/config"
        )
        command_topic = (
            f"{config['mqtt']['topic']}/blinds/{mediolaid}/{identifier}"
        )

        name = blind.get("name", "Mediola Blind")

        payload = {
            "command_topic": command_topic + "/set",
            "payload_open": "open",
            "payload_close": "close",
            "payload_stop": "stop",
            "optimistic": True,
            "device_class": "blind",
            "unique_id": f"{mediolaid}_{identifier}",
            "name": name,
            "device": {
                "identifiers": device_id,
                "manufacturer": "Mediola",
                "name": name,
            },
        }

        # ER-Typ-Geräte unterstützen Statusrückmeldungen
        if blind["type"] == "ER":
            payload["state_topic"] = command_topic + "/state"

        payload_json = json.dumps(payload)

        # Topic abonnieren, um Steuerbefehle zu empfangen
        client.subscribe(command_topic + "/set")
        client.publish(discovery_topic, payload=payload_json, retain=True)

        log.debug(
            "Blind Discovery veröffentlicht: ID='%s', Name='%s', Topic='%s'",
            identifier, name, discovery_topic
        )


# ============================================================================
# Paketverarbeitung (vom Mediola-Gateway empfangen)
# ============================================================================

def handle_button(
    config: dict,
    device_lookup: DeviceLookup,
    packet_type: str,
    address: str,
    state: str,
    mediolaid: str,
) -> Tuple[Optional[str], Optional[str], bool]:
    """
    Verarbeitet ein empfangenes Button-Ereignis.

    Args:
        config: Die geladene Konfiguration.
        device_lookup: Index für schnelle Gerätesuche.
        packet_type: Der Pakettyp (z.B. 'IT').
        address: Die Geräteadresse (lowercase).
        state: Der Status-Wert (lowercase).
        mediolaid: Die Mediola-Gateway-ID.

    Returns:
        Tuple von (topic, payload, retain).
        topic/payload sind None, wenn kein passender Button gefunden wurde.
    """
    buttons = device_lookup.find_buttons(packet_type, address)

    for button in buttons:
        # Bei Multi-Mediola: ID-Abgleich
        if isinstance(config["mediola"], list):
            if button.get("mediola") != mediolaid:
                continue

        identifier = button["type"] + "_" + button["adr"]
        topic = f"{config['mqtt']['topic']}/buttons/{mediolaid}/{identifier}"

        log.debug(
            "Button-Ereignis erkannt: Typ='%s', Adr='%s', Status='%s', "
            "MediolaID='%s' -> Topic='%s'",
            packet_type, address, state, mediolaid, topic
        )

        return topic, state, False

    return None, None, False


def handle_blind(
    config: dict,
    device_lookup: DeviceLookup,
    packet_type: str,
    address: str,
    state: str,
    mediolaid: str,
) -> Tuple[Optional[str], Optional[str], bool]:
    """
    Verarbeitet ein empfangenes Blind/Rollo-Statusereignis.
    Übersetzt den Mediola-Statuscode in einen Home-Assistant-
    kompatiblen Status (open, closed, opening, closing, stopped).

    Args:
        config: Die geladene Konfiguration.
        device_lookup: Index für schnelle Gerätesuche.
        packet_type: Der Pakettyp (muss 'ER' sein).
        address: Die Geräteadresse (lowercase).
        state: Der Status-Hex-Wert (lowercase, 2 Zeichen).
        mediolaid: Die Mediola-Gateway-ID.

    Returns:
        Tuple von (topic, payload, retain).
        topic/payload sind None, wenn kein passender Blind gefunden wurde.
    """
    # Status-Mapping: Mediola-Hex-Code -> HA-Status
    state_map = {
        "01": "open",
        "0e": "open",
        "02": "closed",
        "0f": "closed",
        "08": "opening",
        "0a": "opening",
        "09": "closing",
        "0b": "closing",
        "0d": "stopped",
        "05": "stopped",
    }

    # Nur ER-Typ unterstützt Statusrückmeldungen
    if packet_type != "ER":
        return None, None, True

    blinds = device_lookup.find_blinds(packet_type, address)

    for blind in blinds:
        # Bei Multi-Mediola: ID-Abgleich
        if isinstance(config["mediola"], list):
            if blind.get("mediola") != mediolaid:
                continue

        identifier = blind["type"] + "_" + blind["adr"]
        topic = (
            f"{config['mqtt']['topic']}/blinds/{mediolaid}/"
            f"{identifier}/state"
        )
        payload = state_map.get(state, "unknown")

        if payload == "unknown":
            log.warning(
                "Unbekannter Blind-Status '0x%s' für Gerät Typ='%s', "
                "Adr='%s'. Setze 'unknown'.",
                state, packet_type, address
            )

        log.debug(
            "Blind-Status erkannt: Typ='%s', Adr='%s', "
            "RawState='0x%s', MappedState='%s', MediolaID='%s' -> Topic='%s'",
            packet_type, address, state, payload, mediolaid, topic
        )

        return topic, payload, True

    return None, None, True


def handle_packet_v4(
    data: bytes,
    addr: tuple,
    config: dict,
    client: mqtt.Client,
    device_lookup: DeviceLookup,
) -> bool:
    """
    Verarbeitet ein UDP-Paket vom Mediola Gateway v4/v5.
    Das Paket-Format ist JSON mit 'type' und 'data' Feldern.

    Args:
        data: Die empfangenen Rohdaten (ohne '{XC_EVT}' Prefix).
        addr: Absender-Adresse als Tuple (ip, port).
        config: Die geladene Konfiguration.
        client: Die MQTT-Client-Instanz.
        device_lookup: Index für schnelle Gerätesuche.

    Returns:
        True bei erfolgreicher Verarbeitung, sonst False.
    """
    try:
        data_dict = json.loads(data)
    except (json.JSONDecodeError, ValueError) as e:
        log.error(
            "Ungültiges JSON in v4-Paket von %s: %s (Daten: '%s')",
            addr[0], e, data[:100]
        )
        return False

    log.debug(
        "Mediola v4-Paket empfangen von %s:%d: %s",
        addr[0], addr[1], data_dict
    )

    mediolaid = get_mediolaid_by_address(config, addr)
    packet_type = data_dict.get("type", "")
    raw_data = data_dict.get("data", "")

    if not packet_type or not raw_data:
        log.warning(
            "v4-Paket von %s enthält kein 'type' oder 'data' Feld: %s",
            addr[0], data_dict
        )
        return False

    # Zuerst als Button versuchen
    topic, payload, retain = handle_button(
        config, device_lookup,
        packet_type,
        raw_data[:-2].lower(),
        raw_data[-2:].lower(),
        mediolaid,
    )

    # Falls kein Button, als Blind versuchen
    if not topic:
        try:
            blind_address = format(int(raw_data[:2].lower(), 16), "02d")
        except ValueError as e:
            log.error(
                "Ungültige Adresse im v4-Paket: '%s' (%s)",
                raw_data[:2], e
            )
            return False

        topic, payload, retain = handle_blind(
            config, device_lookup,
            packet_type,
            blind_address,
            raw_data[-2:].lower(),
            mediolaid,
        )

    if topic and payload:
        log.debug(
            "Veröffentliche MQTT: Topic='%s', Payload='%s', Retain=%s",
            topic, payload, retain
        )
        client.publish(topic, payload=payload, retain=retain)
        return True
    else:
        log.debug(
            "Kein passendes Gerät für v4-Paket: Typ='%s', Daten='%s'",
            packet_type, raw_data
        )
        return False


def handle_packet_v6(
    data: bytes,
    addr: tuple,
    config: dict,
    client: mqtt.Client,
    device_lookup: DeviceLookup,
) -> bool:
    """
    Verarbeitet ein UDP-Paket vom Mediola Gateway v6.
    Das Paket-Format ist JSON mit 'type', 'adr' und 'state' Feldern.

    Args:
        data: Die empfangenen Rohdaten (ohne 'STA:' Prefix).
        addr: Absender-Adresse als Tuple (ip, port).
        config: Die geladene Konfiguration.
        client: Die MQTT-Client-Instanz.
        device_lookup: Index für schnelle Gerätesuche.

    Returns:
        True bei erfolgreicher Verarbeitung, sonst False.
    """
    try:
        data_dict = json.loads(data)
    except (json.JSONDecodeError, ValueError) as e:
        log.error(
            "Ungültiges JSON in v6-Paket von %s: %s (Daten: '%s')",
            addr[0], e, data[:100]
        )
        return False

    log.debug(
        "Mediola v6-Paket empfangen von %s:%d: %s",
        addr[0], addr[1], data_dict
    )

    mediolaid = get_mediolaid_by_address(config, addr)
    packet_type = data_dict.get("type", "")
    address = data_dict.get("adr", "").lower()
    state_raw = data_dict.get("state", "")

    if not packet_type or not address or not state_raw:
        log.warning(
            "v6-Paket von %s enthält unvollständige Felder: %s",
            addr[0], data_dict
        )
        return False

    state = state_raw[-2:].lower()

    # Zuerst als Button versuchen
    topic, payload, retain = handle_button(
        config, device_lookup,
        packet_type, address, state, mediolaid,
    )

    # Falls kein Button, als Blind versuchen
    if not topic:
        try:
            blind_address = format(int(address, 16), "02d")
        except ValueError as e:
            log.error(
                "Ungültige Adresse im v6-Paket: '%s' (%s)",
                address, e
            )
            return False

        topic, payload, retain = handle_blind(
            config, device_lookup,
            packet_type, blind_address, state, mediolaid,
        )

    if topic and payload:
        log.debug(
            "Veröffentliche MQTT: Topic='%s', Payload='%s', Retain=%s",
            topic, payload, retain
        )
        client.publish(topic, payload=payload, retain=retain)
        return True
    else:
        log.debug(
            "Kein passendes Gerät für v6-Paket: Typ='%s', Adr='%s', "
            "State='%s'",
            packet_type, address, state
        )
        return False


# ============================================================================
# Graceful Shutdown
# ============================================================================

class GracefulShutdown:
    """
    Handler für sauberes Beenden des Programms bei SIGTERM/SIGINT.
    Stellt sicher, dass MQTT-Verbindung und UDP-Socket ordnungsgemäß
    geschlossen werden.
    """

    def __init__(self):
        self.should_exit = False
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum: int, frame):
        """Signal-Handler für SIGTERM und SIGINT."""
        sig_name = signal.Signals(signum).name
        log.info("Signal %s empfangen. Starte sauberes Beenden...", sig_name)
        self.should_exit = True


# ============================================================================
# Hauptprogramm
# ============================================================================

def main():
    """
    Hauptfunktion: Initialisiert alle Komponenten und startet die
    Hauptschleife, die UDP-Pakete vom Mediola-Gateway empfängt und
    an MQTT weiterleitet.
    """
    global log

    # --- Konfiguration laden und validieren ---
    config = load_config()
    validate_config(config)

    # --- Logging mit korrektem Debug-Level neu konfigurieren ---
    debug_enabled = config["mqtt"].get("debug", False)
    log = setup_logging(debug=debug_enabled)

    if debug_enabled:
        log.info("Debug-Logging ist aktiviert.")
        log.debug("Geladene Konfiguration: %s",
                   json.dumps(config, indent=2, default=str))

    # --- Device-Lookup-Index aufbauen ---
    device_lookup = DeviceLookup(config)

    # --- Graceful Shutdown einrichten ---
    shutdown = GracefulShutdown()

    # --- MQTT-Client einrichten ---
    mqttc = mqtt.Client(userdata=config)

    mqttc.on_connect = on_connect
    mqttc.on_subscribe = on_subscribe
    mqttc.on_disconnect = on_disconnect
    mqttc.on_message = on_message

    if debug_enabled:
        log.info("MQTT Debug-Callbacks werden aktiviert.")
        mqttc.on_log = on_log
        mqttc.on_publish = on_publish

    # MQTT-Authentifizierung, falls konfiguriert
    mqtt_user = config["mqtt"].get("username", "")
    mqtt_pass = config["mqtt"].get("password", "")
    if mqtt_user and mqtt_pass:
        mqttc.username_pw_set(mqtt_user, mqtt_pass)
        log.info("MQTT-Authentifizierung konfiguriert für Benutzer '%s'.", mqtt_user)

    # MQTT-Verbindung herstellen
    mqtt_host = config["mqtt"]["host"]
    mqtt_port = config["mqtt"]["port"]

    try:
        log.info("Verbinde mit MQTT-Broker: %s:%d ...", mqtt_host, mqtt_port)
        mqttc.connect(mqtt_host, mqtt_port, keepalive=60)
    except (socket.error, OSError) as e:
        log.error(
            "Fehler beim Verbinden mit MQTT-Broker '%s:%d': %s. "
            "Programm wird beendet.",
            mqtt_host, mqtt_port, e
        )
        sys.exit(1)

    log.info("MQTT-Verbindung hergestellt zu %s:%d", mqtt_host, mqtt_port)

    # MQTT-Client-Loop im Hintergrund starten
    mqttc.loop_start()

    # --- UDP-Socket für Mediola-Gateway einrichten ---
    listen_port = 1902
    if "general" in config and "port" in config["general"]:
        listen_port = config["general"]["port"]

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.settimeout(1.0)  # 1 Sekunde Timeout für Shutdown-Check
        sock.bind(("", listen_port))
        log.info("UDP-Socket geöffnet auf Port %d.", listen_port)
    except OSError as e:
        log.error(
            "Fehler beim Öffnen des UDP-Sockets auf Port %d: %s. "
            "Programm wird beendet.",
            listen_port, e
        )
        mqttc.loop_stop()
        mqttc.disconnect()
        sys.exit(1)

    # --- Hauptschleife: UDP-Pakete empfangen und verarbeiten ---
    log.info(
        "mediola2mqtt gestartet. Warte auf UDP-Pakete auf Port %d...",
        listen_port
    )

    try:
        while not shutdown.should_exit:
            try:
                data, addr = sock.recvfrom(1024)
            except socket.timeout:
                # Timeout ist normal – ermöglicht regelmäßigen Shutdown-Check
                continue
            except OSError as e:
                log.error("Fehler beim Empfangen von UDP-Daten: %s", e)
                continue

            log.debug(
                "UDP-Paket empfangen von %s:%d (%d Bytes): %s",
                addr[0], addr[1], len(data),
                data.decode("utf-8", errors="replace")[:200]
            )

            # Debug: Rohdaten an MQTT-Topic senden
            if debug_enabled:
                mqttc.publish(
                    config["mqtt"]["topic"] + "/debug/raw",
                    payload=data,
                    retain=False
                )

            # --- Paket-Typ erkennen und verarbeiten ---
            if data.startswith(b"{XC_EVT}"):
                # v4/v5 Gateway-Format
                cleaned_data = data.replace(b"{XC_EVT}", b"")
                log.debug("Erkannt als v4/v5-Paket (XC_EVT)")

                if not handle_packet_v4(cleaned_data, addr, config, mqttc, device_lookup):
                    log.warning(
                        "v4-Paket konnte nicht verarbeitet werden: %s",
                        cleaned_data.decode("utf-8", errors="replace")[:200]
                    )

            elif data.startswith(b"STA:"):
                # v6 Gateway-Format
                cleaned_data = data.replace(b"STA:", b"")
                log.debug("Erkannt als v6-Paket (STA)")

                if not handle_packet_v6(cleaned_data, addr, config, mqttc, device_lookup):
                    log.warning(
                        "v6-Paket konnte nicht verarbeitet werden: %s",
                        cleaned_data.decode("utf-8", errors="replace")[:200]
                    )

            else:
                log.warning(
                    "Unbekanntes Paket-Format von %s:%d: %s",
                    addr[0], addr[1],
                    data.decode("utf-8", errors="replace")[:200]
                )

    except Exception as e:
        log.error("Unerwarteter Fehler in der Hauptschleife: %s", e, exc_info=True)

    finally:
        # --- Sauberes Beenden ---
        log.info("Beende mediola2mqtt...")

        try:
            sock.close()
            log.info("UDP-Socket geschlossen.")
        except OSError as e:
            log.error("Fehler beim Schließen des UDP-Sockets: %s", e)

        try:
            mqttc.loop_stop()
            mqttc.disconnect()
            log.info("MQTT-Verbindung geschlossen.")
        except Exception as e:
            log.error("Fehler beim Schließen der MQTT-Verbindung: %s", e)

        log.info("mediola2mqtt beendet.")


# ============================================================================
# Einstiegspunkt
# ============================================================================

if __name__ == "__main__":
    main()
