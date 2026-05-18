import json
import logging
import time
from datetime import datetime
import threading
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("mir250-complete-consumer")

# Configuración Kafka
KAFKA_BROKERS = ["localhost:9093"]
TOPIC_BATTERY = "mir250.battery"
TOPIC_MISSION_CURRENT = "mir250.mission.current"
TOPIC_MISSION_COMPLETED = "mir250.mission.completed"

# Grupos de consumidores (diferentes para cada topic)
CONSUMER_GROUP_BATTERY = "battery-influxdb-group"
CONSUMER_GROUP_MISSION_CURRENT = "mission-current-influxdb-group"
CONSUMER_GROUP_MISSION_COMPLETED = "mission-completed-influxdb-group"

# Configuración InfluxDB
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "-Kn9BQmY6lgwBTKpt1QZgbAOa4p8YrQ3YVGoKdH9G-U3g6Z5w97bJjVknHoZmTkqgN3uX428s1_x2MKYpU260g=="
INFLUXDB_ORG = "somorrostro"
INFLUXDB_BUCKET = "pruebas-MIR-kafka"
MEASUREMENT_BATTERY = "Datos_MIR"
MEASUREMENT_MISSIONS = "Misiones"

# Variables para seguimiento de valores anteriores (evitar duplicados)
last_battery_value = None
last_mission_current = None

def create_kafka_consumer(topic, group_id):
    """Crea y devuelve un consumidor Kafka configurado para un topic específico."""
    logger.info(f"Conectando a Kafka para topic '{topic}', grupo '{group_id}'")
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BROKERS,
            group_id=group_id,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logger.info(f"Consumidor Kafka creado exitosamente para topic '{topic}'")
        return consumer
    except Exception as e:
        logger.error(f"Error al crear consumidor Kafka para topic '{topic}': {e}")
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

def write_to_influxdb(write_api, measurement, data, time_field="timestamp"):
    """Escribe los datos en InfluxDB con el measurement especificado."""
    try:
        # Extraer timestamp si existe, o usar tiempo actual
        timestamp = data.get(time_field, datetime.now().isoformat())
        
        # Crear un punto con todos los campos de datos
        point = Point(measurement)
        
        # Añadir cada campo del diccionario excluyendo el timestamp
        for key, value in data.items():
            if key != time_field:
                # Convertir valores a los tipos correctos para InfluxDB
                if isinstance(value, str) and value.replace('.', '', 1).isdigit():
                    # Si es un string numérico, convertirlo a float
                    value = float(value)
                
                point = point.field(key, value)
        
        # Establecer el tiempo
        point = point.time(timestamp)
            
        # Escribir el punto en InfluxDB
        write_api.write(bucket=INFLUXDB_BUCKET, record=point)
        logger.info(f"Datos escritos en InfluxDB measurement '{measurement}': {data}")
        return True
        
    except Exception as e:
        logger.error(f"Error al escribir en InfluxDB: {e}")
        return False

def consume_battery_data(client):
    """Procesa los mensajes del topic de batería."""
    global last_battery_value
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    consumer = create_kafka_consumer(TOPIC_BATTERY, CONSUMER_GROUP_BATTERY)
    logger.info(f"Esperando mensajes de batería en topic '{TOPIC_BATTERY}'...")
    
    try:
        for message in consumer:
            try:
                data = message.value
                battery_value = data.get("Battery")
                
                # Filtrado RBE (Report By Exception) - solo procesar si hay cambio
                if battery_value != last_battery_value:
                    logger.info(f"Nuevo valor de batería: {battery_value}% (anterior: {last_battery_value}%)")
                    write_to_influxdb(write_api, MEASUREMENT_BATTERY, data)
                    last_battery_value = battery_value
                else:
                    logger.debug(f"Valor de batería sin cambios: {battery_value}%, ignorando")
                    
            except Exception as e:
                logger.error(f"Error procesando mensaje de batería: {e}")
    except KeyboardInterrupt:
        logger.info("Deteniendo consumidor de batería")
    finally:
        consumer.close()
        
def consume_mission_current_data(client):
    """Procesa los mensajes del topic de misión actual."""
    global last_mission_current
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    consumer = create_kafka_consumer(TOPIC_MISSION_CURRENT, CONSUMER_GROUP_MISSION_CURRENT)
    logger.info(f"Esperando mensajes de misión actual en topic '{TOPIC_MISSION_CURRENT}'...")
    
    try:
        for message in consumer:
            try:
                data = message.value
                
                # Para misiones actuales, comparamos el nombre de la misión
                # o todo el objeto si es necesario
                mission_name = data.get("mision_actual")
                
                # Si es un nuevo valor o nunca hemos recibido uno antes
                if not last_mission_current or mission_name != last_mission_current.get("mision_actual"):
                    logger.info(f"Nueva misión actual: {mission_name}")
                    write_to_influxdb(write_api, MEASUREMENT_MISSIONS, data)
                    last_mission_current = data.copy()  # Guardar una copia completa
                else:
                    logger.debug(f"Misión actual sin cambios: {mission_name}, ignorando")
                    
            except Exception as e:
                logger.error(f"Error procesando mensaje de misión actual: {e}")
    except KeyboardInterrupt:
        logger.info("Deteniendo consumidor de misión actual")
    finally:
        consumer.close()
        
def consume_mission_completed_data(client):
    """Procesa los mensajes del topic de misión completada."""
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    consumer = create_kafka_consumer(TOPIC_MISSION_COMPLETED, CONSUMER_GROUP_MISSION_COMPLETED)
    logger.info(f"Esperando mensajes de misión completada en topic '{TOPIC_MISSION_COMPLETED}'...")
    
    try:
        for message in consumer:
            try:
                data = message.value
                mission_name = data.get("nombre_mision")
                
                # Las misiones completadas son eventos únicos, siempre se procesan
                logger.info(f"Misión completada: {mission_name}, tiempo: {data.get('tiempo_trabajo')}s")
                write_to_influxdb(write_api, MEASUREMENT_MISSIONS, data)
                    
            except Exception as e:
                logger.error(f"Error procesando mensaje de misión completada: {e}")
    except KeyboardInterrupt:
        logger.info("Deteniendo consumidor de misión completada")
    finally:
        consumer.close()

def main():
    """Función principal que lanza los consumidores en hilos separados."""
    logger.info("Iniciando consumidor completo para MIR250")
    
    # Creamos un único cliente InfluxDB que compartirán todos los hilos
    max_retries = 5
    retry_count = 0
    
    # Intentar conectar a InfluxDB con reintentos
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
        return
        
    try:
        # Crear hilos para cada consumidor
        thread_battery = threading.Thread(target=consume_battery_data, args=(influxdb_client,))
        thread_mission_current = threading.Thread(target=consume_mission_current_data, args=(influxdb_client,))
        thread_mission_completed = threading.Thread(target=consume_mission_completed_data, args=(influxdb_client,))
        
        # Iniciar los hilos
        thread_battery.start()
        thread_mission_current.start()
        thread_mission_completed.start()
        
        # Esperar a que todos los hilos terminen (lo que no ocurrirá a menos que haya una excepción)
        thread_battery.join()
        thread_mission_current.join()
        thread_mission_completed.join()
        
    except KeyboardInterrupt:
        logger.info("Deteniendo el programa")
    except Exception as e:
        logger.error(f"Error en el programa principal: {e}")
    finally:
        # Cerrar la conexión con InfluxDB
        influxdb_client.close()
        logger.info("Programa finalizado")

if __name__ == "__main__":
    main()