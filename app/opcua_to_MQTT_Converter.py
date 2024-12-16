
# pip install opcua 
# sudo pip install paho-mqtt==1.6.1

from opcua import Client
from paho.mqtt import client as mqtt_client
import time

# OPC UA Configuration
OPC_SERVER_URL = "opc.tcp://host.docker.internal:4841"  # Replace with your OPC UA server URL
NODES_TO_MONITOR = [
    "ns=2;s=DB15.F101_XTT300_Bruede",  # Example node IDs, replace with your actual node IDs
    "ns=2;s=DB15.R201_XTT610_Manteltemp"
]

# MQTT Configuration
MQTT_BROKER = "host.docker.internal"  # Replace with your MQTT broker
MQTT_PORT = 1883
MQTT_TOPIC = "plant1"
MQTT_CLIENT_ID = "opcua_mqtt_publisher"

def connect_mqtt():
    """
    Connect to the MQTT broker and return the client.
    """
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print(f"Failed to connect, return code {rc}")

    client = mqtt_client.Client(MQTT_CLIENT_ID)
    client.on_connect = on_connect
    client.connect(MQTT_BROKER, MQTT_PORT)
    return client

def read_opcua_data(client, mqtt_client):
    """
    Read values from OPC UA server and publish to MQTT broker.
    """
    try:
        client.connect()
        print("Connected to OPC UA server.")

        while True:
            for node_id in NODES_TO_MONITOR:
                try:
                    node = client.get_node(node_id)
                    value = node.get_value()
                    print(f"Node {node_id} value: {value}")

                    # Publish the value to the MQTT broker
                    mqtt_payload = {
                        "node_id": node_id,
                        "value": value
                    }
                    mqtt_client.publish(MQTT_TOPIC, str(mqtt_payload))
                    print(f"Published to MQTT: {mqtt_payload}")
                except Exception as e:
                    print(f"Failed to read node {node_id}: {e}")
            
            time.sleep(5)  # Adjust polling interval as needed

    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.disconnect()
        print("Disconnected from OPC UA server.")

def main():
    # Connect to MQTT broker
    mqtt_client = connect_mqtt()
    mqtt_client.loop_start()

    # Connect to OPC UA server and start reading data
    opcua_client = Client(OPC_SERVER_URL)
    read_opcua_data(opcua_client, mqtt_client)

if __name__ == "__main__":
    main()
