import json
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient

# MQTT Configurations
MQTT_BROKER = "host.docker.internal"
MQTT_PORT = 1883
MQTT_TOPIC = "plant1/#"
MQTT_CLIENT_ID = "mqtt_influx_subscriber"

# InfluxDB Configurations
INFLUXDB_HOST = "host.docker.internal"
INFLUXDB_PORT = 8086
INFLUXDB_DATABASE = "sensor_data"

# Connect to InfluxDB
influx_client = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT)

# Create database if it doesn't exist
databases = influx_client.get_list_database()
if {'name': INFLUXDB_DATABASE} not in databases:
    influx_client.create_database(INFLUXDB_DATABASE)
influx_client.switch_database(INFLUXDB_DATABASE)

# Callback for successful MQTT connection
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"Failed to connect, return code {rc}")

# Callback for received MQTT messages
def on_message(client, userdata, msg):
    print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
    try:
        # Parse the incoming MQTT message as JSON
        data = json.loads(msg.payload.decode())

        # Extract relevant fields
        node_id = data.get("node_id", "unknown_node")
        sensor_value = data.get("value", 0)

        # Extract the last part of the node_id to use as the field name
        field_name = node_id.split(';')[-1]
        # Prepare data for InfluxDB
        json_body = [
            {
                "measurement": "sensor_data",
                "tags": {
                    "node_id": node_id
                },
                "fields": {
                    field_name: float(sensor_value)
                }
            }
        ]

        # Write to InfluxDB
        influx_client.write_points(json_body)
        print(f"Data written to InfluxDB: {node_id} ({field_name}) = {sensor_value}")

    except Exception as e:
        print(f"Error processing message: {e}")

# Create MQTT client and assign callbacks
mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# Connect to MQTT broker and start listening
try:
    print("Connecting to MQTT Broker...")
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    mqtt_client.loop_forever()
except Exception as e:
    print(f"Error: {e}")
finally:
    influx_client.close()
