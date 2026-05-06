import requests
import json
import time
import logging
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

# Configuración básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("mir250-complete-producer")

# Configuración del MIR250
MIR250_IP = "192.168.250.33"  # Ajusta a la IP de tu robot
BASE_URL = f"http://{MIR250_IP}/api/v2.0.0"

# Configuración de Kafka
KAFKA_BROKERS = ["localhost:9092"]  # Ajusta según tu configuración
TOPIC_BATTERY = "mir250.battery"
TOPIC_MISSION_CURRENT = "mir250.mission.current"
TOPIC_MISSION_COMPLETED = "mir250.mission.completed"
POLL_INTERVAL = 5  # Segundos entre consultas

# Variables globales para seguimiento
previous_queue_id = None

# Credenciales para la API del MIR250 (como en tu flujo Node-RED)
AUTH_HEADERS = {
    "Authorization": "Basic VXNlcjowNGY4OTk2ZGE3NjNiN2E5NjliMTAyOGVlMzAwNzU2OWVhZjNhNjM1NDg2ZGRhYjIxMWQ1MTJjODViOWRmOGZi",
    "Content-Type": "application/json"
}

def create_kafka_producer():
    """Crea y devuelve un productor Kafka configurado."""
    logger.info(f"Conectando a brokers Kafka: {KAFKA_BROKERS}")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        logger.info("Productor Kafka creado exitosamente")
        return producer
    except Exception as e:
        logger.error(f"Error al crear productor Kafka: {e}")
        raise

def get_status_data():
    """Consulta el estado general del robot MIR250."""
    try:
        response = requests.get(f"{BASE_URL}/status", headers=AUTH_HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Estado del robot obtenido (Batería: {data.get('battery_percentage')}%)")
        return data
    except Exception as e:
        logger.error(f"Error al obtener estado del robot: {e}")
        return None

def get_missions_list():
    """Obtiene la lista de todas las misiones disponibles."""
    try:
        response = requests.get(f"{BASE_URL}/missions", headers=AUTH_HEADERS, timeout=10)
        response.raise_for_status()
        missions = response.json()
        logger.info(f"Lista de misiones obtenida ({len(missions)} misiones)")
        return missions
    except Exception as e:
        logger.error(f"Error al obtener lista de misiones: {e}")
        return None

def get_mission_details(mission_id):
    """Obtiene los detalles de una misión específica."""
    if mission_id is None or mission_id == "Robot sin misión":
        return None
        
    try:
        response = requests.get(
            f"{BASE_URL}/mission_queue/{mission_id}", 
            headers=AUTH_HEADERS, 
            timeout=10
        )
        response.raise_for_status()
        mission_details = response.json()
        logger.info(f"Detalles de misión {mission_id} obtenidos")
        return mission_details
    except Exception as e:
        logger.error(f"Error al obtener detalles de misión {mission_id}: {e}")
        return None

def check_mission_changes(current_queue_id, missions_list):
    """
    Detecta cambios en las misiones actuales y terminadas.
    Similar a la lógica del nodo 'Lectura misiones' en Node-RED.
    """
    global previous_queue_id
    
    mission_current = None
    mission_completed = None
    
    # Si el valor actual es null o undefined
    if current_queue_id is None:
        mission_current = {"mision_actual": "Robot sin misión"}
        
        # Si había una misión previa, entonces terminó
        if previous_queue_id is not None:
            details = get_mission_details(previous_queue_id)
            if details:
                mission_completed = process_completed_mission(details, missions_list)
    else:
        # Hay una misión actual
        details = get_mission_details(current_queue_id)
        if details:
            mission_guid = details.get("mission_id")
            mission_name = find_mission_name(mission_guid, missions_list)
            
            mission_current = {
                "mision_actual": mission_name or "Misión desconocida",
                "mission_id": current_queue_id,
                "mission_guid": mission_guid,
                "tiempo_inicio": details.get("started")
            }
        
        # Si hubo un cambio desde la última vez
        if previous_queue_id is not None and previous_queue_id != current_queue_id:
            details = get_mission_details(previous_queue_id)
            if details:
                mission_completed = process_completed_mission(details, missions_list)
    
    # Actualizar el valor anterior para la próxima vez
    previous_queue_id = current_queue_id
    
    return mission_current, mission_completed

def process_completed_mission(mission_details, missions_list):
    """Procesa la información de una misión completada."""
    mission_guid = mission_details.get("mission_id")
    mission_state = mission_details.get("state")
    start_time = mission_details.get("started")
    finish_time = mission_details.get("finished")
    
    # Calcular tiempo de trabajo
    if start_time and finish_time:
        try:
            total_time = (datetime.fromisoformat(finish_time.replace('Z', '+00:00')) - 
                         datetime.fromisoformat(start_time.replace('Z', '+00:00'))).total_seconds()
        except:
            total_time = None
    else:
        total_time = None
    
    # Obtener el nombre de la misión
    mission_name = find_mission_name(mission_guid, missions_list)
    
    # Si fue abortada, añadir prefijo
    if mission_state == "Aborted" and mission_name:
        mission_name = "(ABORTED) " + mission_name
    
    return {
        "nombre_mision": mission_name or "Misión no encontrada",
        "mission_id": mission_details.get("id"),
        "mission_guid": mission_guid,
        "tiempo_inicio": start_time,
        "tiempo_final": finish_time,
        "tiempo_trabajo": total_time,
        "state": mission_state
    }

def find_mission_name(mission_guid, missions_list):
    """Encuentra el nombre de una misión por su GUID."""
    if not mission_guid or not missions_list:
        return None
        
    mission = next((m for m in missions_list if m.get("guid") == mission_guid), None)
    return mission.get("name") if mission else None

def send_to_kafka(producer, topic, data):
    """Envía los datos al topic de Kafka especificado."""
    if not data:
        return
        
    # Añadir timestamp a los datos
    local_tz = timezone(timedelta(hours=1))  # UTC+1 para España
    local_time = datetime.now(local_tz)
    
    message = data.copy()
    message["timestamp"] = local_time.isoformat()
    
    # Enviar el mensaje a Kafka
    try:
        producer.send(topic, value=message)
        producer.flush()  # Asegurar que el mensaje se envía inmediatamente
        logger.info(f"Datos enviados a Kafka topic '{topic}'")
    except Exception as e:
        logger.error(f"Error al enviar datos a Kafka topic '{topic}': {e}")

def main():
    """Función principal que ejecuta el ciclo de consulta y envío."""
    logger.info("Iniciando productor completo para MIR250")
    
    # Crear el productor Kafka
    producer = create_kafka_producer()
    
    try:
        # Bucle principal
        while True:
            # 1. Obtener el estado general del robot
            status = get_status_data()
            if status:
                # Enviar datos de batería
                battery_data = {"Battery": status.get("battery_percentage")}
                send_to_kafka(producer, TOPIC_BATTERY, battery_data)
                
                # 2. Obtener la lista de misiones
                missions_list = get_missions_list()
                if missions_list:
                    
                    # 3. Comprobar cambios en las misiones
                    current_queue_id = status.get("mission_queue_id")
                    mission_current, mission_completed = check_mission_changes(current_queue_id, missions_list)
                    
                    # Enviar información sobre la misión actual
                    if mission_current:
                        send_to_kafka(producer, TOPIC_MISSION_CURRENT, mission_current)
                    
                    # Enviar información sobre la misión completada
                    if mission_completed:
                        send_to_kafka(producer, TOPIC_MISSION_COMPLETED, mission_completed)
            
            # Esperar antes de la siguiente consulta
            time.sleep(POLL_INTERVAL)
            
    except KeyboardInterrupt:
        logger.info("Deteniendo el productor")
    finally:
        # Cerrar el productor de Kafka
        producer.close()
        logger.info("Productor cerrado")

if __name__ == "__main__":
    main()