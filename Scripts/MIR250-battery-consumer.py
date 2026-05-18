import json
import logging
from datetime import datetime
import time
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("mir250-battery-consumer")

# Configuración Kafka
KAFKA_BROKERS = ["localhost:9093"]
TOPIC_NAME = "mir250.battery"
CONSUMER_GROUP = "battery-influxdb-group"

# Configuración InfluxDB
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "-Kn9BQmY6lgwBTKpt1QZgbAOa4p8YrQ3YVGoKdH9G-U3g6Z5w97bJjVknHoZmTkqgN3uX428s1_x2MKYpU260g=="  # Solo para InfluxDB 2.x
INFLUXDB_ORG = "somorrostro"      # Según tu configuración Node-RED
INFLUXDB_BUCKET = "pruebas-MIR-kafka"   # Según tu configuración Node-RED
MEASUREMENT = "Datos_MIR"         # Según tu configuración Node-RED

def create_kafka_consumer():
    """Crea y devuelve un consumidor Kafka configurado."""
    logger.info(f"Conectando a Kafka en {KAFKA_BROKERS}")
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_BROKERS,
            group_id=CONSUMER_GROUP,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logger.info("Consumidor Kafka creado exitosamente")
        return consumer
    except Exception as e:
        logger.error(f"Error al crear consumidor Kafka: {e}")
        raise

def create_influxdb_client():
    """Crea y devuelve un cliente InfluxDB configurado."""
    logger.info(f"Conectando a InfluxDB en {INFLUXDB_URL}")
    try:
        client = InfluxDBClient(
            url=INFLUXDB_URL,
            token=INFLUXDB_TOKEN,
            org=INFLUXDB_ORG
        )
        logger.info("Cliente InfluxDB creado exitosamente")
        return client
    except Exception as e:
        logger.error(f"Error al crear cliente InfluxDB: {e}")
        raise

def write_to_influxdb(write_api, battery_data):
    """Escribe los datos de batería en InfluxDB."""
    try:
        # Extraer valores del mensaje
        battery_percentage = battery_data.get("Battery")
        timestamp = battery_data.get("timestamp", datetime.now().isoformat())
        
        if battery_percentage is None:
            logger.warning("Mensaje sin valor de batería, ignorando")
            return False
            
        # Crear un punto de datos para InfluxDB
        point = Point(MEASUREMENT) \
            .field("Battery", float(battery_percentage)) \
            .time(timestamp)
            
        # Escribir el punto en InfluxDB
        write_api.write(bucket=INFLUXDB_BUCKET, record=point)
        logger.info(f"Datos escritos en InfluxDB: Battery={battery_percentage}%")
        return True
        
    except Exception as e:
        logger.error(f"Error al escribir en InfluxDB: {e}")
        return False

def main():
    """Función principal que ejecuta el consumidor."""
    logger.info("Iniciando consumidor de batería MIR250")
    
    # Crear consumidor Kafka (con reintentos)
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            consumer = create_kafka_consumer()
            break
        except Exception:
            retry_count += 1
            wait_time = 5 * retry_count
            logger.warning(f"Reintento {retry_count}/{max_retries} en {wait_time} segundos...")
            time.sleep(wait_time)
    
    if retry_count >= max_retries:
        logger.error("No se pudo conectar a Kafka después de varios intentos. Saliendo.")
        return
    
    # Crear cliente InfluxDB (con reintentos)
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            influxdb_client = create_influxdb_client()
            break
        except Exception:
            retry_count += 1
            wait_time = 5 * retry_count
            logger.warning(f"Reintento {retry_count}/{max_retries} en {wait_time} segundos...")
            time.sleep(wait_time)
    
    if retry_count >= max_retries:
        logger.error("No se pudo conectar a InfluxDB después de varios intentos. Saliendo.")
        consumer.close()
        return
    
    # Obtener la API de escritura
    write_api = influxdb_client.write_api(write_options=SYNCHRONOUS)
    
    try:
        # Bucle principal de consumo
        logger.info(f"Esperando mensajes en el topic '{TOPIC_NAME}'...")
        
        for message in consumer:
            try:
                logger.debug(f"Mensaje recibido: {message.value}")
                write_to_influxdb(write_api, message.value)
            except Exception as e:
                logger.error(f"Error procesando mensaje: {e}")
                # Continuar con el siguiente mensaje
    
    except KeyboardInterrupt:
        logger.info("Deteniendo el consumidor")
    finally:
        # Cerrar las conexiones
        consumer.close()
        influxdb_client.close()
        logger.info("Consumidor cerrado")

if __name__ == "__main__":
    main()