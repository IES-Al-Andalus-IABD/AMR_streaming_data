import requests
import json
import time
import logging
from datetime import datetime, timezone, timedelta
import paho.mqtt.client as mqtt

# Configuración básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("mir250-battery-producer")

# Configuración del MIR250
MIR250_IP = "192.168.250.33"  # Ajusta a la IP de tu robot
BASE_URL = f"http://{MIR250_IP}/api/v2.0.0"

# Configuración de MQTT
MQTT_BROKER = "localhost"  # Ajusta según tu configuración
MQTT_PORT = 1883
TOPIC_NAME = "mir250/battery"
POLL_INTERVAL = 5  # Segundos entre consultas

# Credenciales para la API del MIR250 (como en tu flujo Node-RED)
AUTH_HEADERS = {
    "Authorization": "Basic VXNlcjowNGY4OTk2ZGE3NjNiN2E5NjliMTAyOGVlMzAwNzU2OWVhZjNhNjM1NDg2ZGRhYjIxMWQ1MTJjODViOWRmOGZi",
    "Content-Type": "application/json"
}

def get_battery_percentage():
    """Consulta el porcentaje de batería del robot MIR250."""
    try:
        # Consultar el estado general del robot
        response = requests.get(f"{BASE_URL}/status", headers=AUTH_HEADERS, timeout=10)
        response.raise_for_status()  # Lanza excepción si hay un error HTTP
        
        # Extraer el porcentaje de batería
        data = response.json()
        battery_percentage = data.get("battery_percentage")
        
        if battery_percentage is not None:
            logger.info(f"Batería actual: {battery_percentage}%")
            return battery_percentage
        else:
            logger.warning("No se encontró información de batería en la respuesta")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Error al conectar con el robot: {e}")
        return None
    except Exception as e:
        logger.error(f"Error inesperado: {e}")
        return None

def send_battery_data_to_mqtt(client, battery_percentage):
    """Envía los datos de batería a MQTT."""
    if battery_percentage is None:
        return
        
    # Crear el mensaje con el porcentaje de batería y timestamp
    # Usar UTC+1 (España) explícitamente
    local_tz = timezone(timedelta(hours=1))  # UTC+1 para España
    local_time = datetime.now(local_tz)
    
    message = {
        "Battery": battery_percentage,
        "timestamp": local_time.isoformat()
    }
    
    # Enviar el mensaje a MQTT
    try:
        client.publish(TOPIC_NAME, json.dumps(message))
        logger.info(f"Datos enviados a MQTT: {message}")
    except Exception as e:
        logger.error(f"Error al enviar datos a MQTT: {e}")

def main():
    """Función principal que ejecuta el ciclo de consulta y envío."""
    logger.info("Iniciando productor de batería para MIR250")
    
    # Crear el productor MQTT
    producer = mqtt.Client()
    producer.connect(MQTT_BROKER, MQTT_PORT)
    
    try:
        # Bucle principal
        while True:
            battery = get_battery_percentage()
            # Paso 1: Obtener el porcentaje de batería
            
            # Paso 2: Enviar los datos a MQTT (si hay datos válidos)
            if battery is not None:
                send_battery_data_to_mqtt(producer, battery)
            
            # Esperar antes de la siguiente consulta
            time.sleep(POLL_INTERVAL)
            
    except KeyboardInterrupt:
        logger.info("Deteniendo el productor")
    finally:
        # Cerrar el productor de MQTT
        producer.disconnect()
        logger.info("Productor cerrado")

if __name__ == "__main__":
    main()