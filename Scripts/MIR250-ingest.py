import json
import logging
import os
import time
from datetime import datetime
from threading import Thread

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None

from kafka import KafkaConsumer

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("mir250-ingest")

# Transport selection: "kafka", "mqtt", or "both"
TRANSPORT = os.getenv("MIR250_TRANSPORT", "kafka").lower()
VALID_TRANSPORTS = {"kafka", "mqtt", "both"}

# Configuración Kafka
KAFKA_BROKERS = ["localhost:9093"]
TOPIC_BATTERY = "mir250.battery"
TOPIC_MISSION_CURRENT = "mir250.mission.current"
TOPIC_MISSION_COMPLETED = "mir250.mission.completed"

CONSUMER_GROUP_BATTERY = "battery-influxdb-group"
CONSUMER_GROUP_MISSION_CURRENT = "mission-current-influxdb-group"
CONSUMER_GROUP_MISSION_COMPLETED = "mission-completed-influxdb-group"

# Configuración MQTT
MQTT_BROKER = os.getenv("MIR250_MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MIR250_MQTT_PORT", "1883"))
MQTT_TOPICS = [
    ("mir250/battery", TOPIC_BATTERY),
    ("mir250/mission/current", TOPIC_MISSION_CURRENT),
    ("mir250/mission/completed", TOPIC_MISSION_COMPLETED),
]

# Configuración InfluxDB
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "-Kn9BQmY6lgwBTKpt1QZgbAOa4p8YrQ3YVGoKdH9G-U3g6Z5w97bJjVknHoZmTkqgN3uX428s1_x2MKYpU260g==")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "my-org")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "my-bucket")

TOPIC_TO_MEASUREMENT = {
    TOPIC_BATTERY: "Datos_MIR",
    TOPIC_MISSION_CURRENT: "Misiones",
    TOPIC_MISSION_COMPLETED: "Misiones",
}

MQTT_TOPIC_TO_KAFKA_TOPIC = {mqtt_topic: kafka_topic for mqtt_topic, kafka_topic in MQTT_TOPICS}


def create_influxdb_client():
    logger.info(f"Conectando a InfluxDB en {INFLUXDB_URL} (org={INFLUXDB_ORG}, bucket={INFLUXDB_BUCKET})")
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    logger.info("Cliente InfluxDB creado exitosamente")
    return client


def write_to_influxdb(write_api, measurement, data, time_field="timestamp"):
    try:
        timestamp = data.get(time_field, datetime.utcnow().isoformat())
        point = Point(measurement)

        for key, value in data.items():
            if key == time_field:
                continue
            if isinstance(value, str) and value.replace('.', '', 1).isdigit():
                value = float(value)
            point = point.field(key, value)

        point = point.time(timestamp)
        write_api.write(bucket=INFLUXDB_BUCKET, record=point)
        logger.info(f"Escrito en InfluxDB [{measurement}] {data}")
        return True
    except Exception as e:
        logger.error(f"Error al escribir en InfluxDB: {e}")
        return False


def create_kafka_consumer(topic, group_id):
    logger.info(f"Conectando a Kafka para topic '{topic}', grupo '{group_id}'")
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=group_id,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    logger.info(f"Consumidor Kafka creado para topic '{topic}'")
    return consumer


def run_kafka_consumer(topic, group_id, write_api):
    consumer = create_kafka_consumer(topic, group_id)
    measurement = TOPIC_TO_MEASUREMENT.get(topic, "Misiones")
    logger.info(f"Esperando mensajes Kafka en '{topic}'...")
    try:
        for message in consumer:
            try:
                payload = message.value
                write_to_influxdb(write_api, measurement, payload)
            except Exception as e:
                logger.error(f"Error procesando mensaje Kafka '{topic}': {e}")
    except KeyboardInterrupt:
        logger.info(f"Kafka consumer '{topic}' detenido")
    finally:
        consumer.close()


def mqtt_on_connect(client, userdata, flags, rc):
    if rc != 0:
        logger.error(f"MQTT no pudo conectarse, código rc={rc}")
        return
    logger.info(f"Conectado a MQTT en {MQTT_BROKER}:{MQTT_PORT}")
    for mqtt_topic, _ in MQTT_TOPICS:
        client.subscribe(mqtt_topic)
        logger.info(f"Suscrito a MQTT topic '{mqtt_topic}'")


def mqtt_on_message(client, userdata, msg):
    write_api = userdata["write_api"]
    kafka_topic = MQTT_TOPIC_TO_KAFKA_TOPIC.get(msg.topic)
    if kafka_topic is None:
        logger.warning(f"Mensaje MQTT recibido de topic desconocido: {msg.topic}")
        return

    try:
        payload = json.loads(msg.payload.decode('utf-8'))
    except json.JSONDecodeError:
        logger.error(f"Payload MQTT no es JSON válido: {msg.payload}")
        return

    measurement = TOPIC_TO_MEASUREMENT.get(kafka_topic, "Misiones")
    logger.info(f"Mensaje MQTT recibido en '{msg.topic}' -> medir '{measurement}'")
    write_to_influxdb(write_api, measurement, payload)


def run_mqtt_consumer(write_api):
    if mqtt is None:
        raise RuntimeError("paho-mqtt no está instalado. Instala con: pip install paho-mqtt")

    client = mqtt.Client()
    client.user_data_set({"write_api": write_api})
    client.on_connect = mqtt_on_connect
    client.on_message = mqtt_on_message

    logger.info(f"Conectando a MQTT broker {MQTT_BROKER}:{MQTT_PORT}")
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    client.loop_forever()


def validate_transport():
    if TRANSPORT not in VALID_TRANSPORTS:
        raise ValueError(f"TRANSPORT inválido: {TRANSPORT}. Usa kafka, mqtt o both.")


def main():
    logger.info(f"Iniciando ingest para MIR250 usando transport={TRANSPORT}")
    validate_transport()

    influxdb_client = None
    try:
        influxdb_client = create_influxdb_client()
        write_api = influxdb_client.write_api(write_options=SYNCHRONOUS)

        threads = []
        if TRANSPORT in {"kafka", "both"}:
            threads.append(Thread(target=run_kafka_consumer, args=(TOPIC_BATTERY, CONSUMER_GROUP_BATTERY, write_api), daemon=True))
            threads.append(Thread(target=run_kafka_consumer, args=(TOPIC_MISSION_CURRENT, CONSUMER_GROUP_MISSION_CURRENT, write_api), daemon=True))
            threads.append(Thread(target=run_kafka_consumer, args=(TOPIC_MISSION_COMPLETED, CONSUMER_GROUP_MISSION_COMPLETED, write_api), daemon=True))

        for thread in threads:
            thread.start()

        if TRANSPORT in {"mqtt", "both"}:
            run_mqtt_consumer(write_api)
        else:
            while True:
                time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Interrupción recibida, cerrando...")
    except Exception as e:
        logger.error(f"Error en el programa principal: {e}")
    finally:
        if influxdb_client is not None:
            influxdb_client.close()
            logger.info("Cliente InfluxDB cerrado")


if __name__ == "__main__":
    main()
