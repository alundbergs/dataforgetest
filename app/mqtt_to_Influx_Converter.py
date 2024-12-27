import json
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

MQTT_BROKER = "host.docker.internal"
MQTT_PORT = 1883
MQTT_TOPIC = "plant1/#"
INFLUXDB_URL = "host.docker.internal:8086"
INFLUXDB_ORG = "PP_Test"
INFLUXDB_BUCKET = "sensor_data"

influx_client = InfluxDBClient(url=INFLUXDB_URL, org=INFLUXDB_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT Broker" if rc == 0 else f"Connection failed, rc: {rc}")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode().replace("'", '"'))
        sensor_name = data.get("node_id", "unknown_sensor")
        sensor_value = float(data.get("value", 0))
        
        point = Point("sensor_data").tag("sensor", sensor_name).field("value", sensor_value)
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        print(f"Data written: {sensor_name} = {sensor_value}")
    except Exception as e:
        print(f"Error processing message: {e}")

mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

try:
    print("Connecting to MQTT Broker...")
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT)
    mqtt_client.loop_forever()
except Exception as e:
    print(f"Error: {e}")