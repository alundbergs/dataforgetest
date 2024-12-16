from opcua import Client
from paho.mqtt import client as mqtt_client
import time
import json
import csv
import os

# OPC UA Configuration
OPC_SERVER_URL = "opc.tcp://host.docker.internal:4841"

# # Node Configuration
# NODES_TO_MONITOR = [
#     "F101_XTT300_Bruede",
#     "R201_XTT610_Manteltemp"
# ]

# CSV file path
SELECTED_CSV = "app/selected.csv"

def load_nodes_from_csv():
    nodes = []
    if os.path.exists(SELECTED_CSV):
        with open(SELECTED_CSV, mode='r') as file:
            reader = csv.DictReader(file)
            nodes = [row['node_id'] for row in reader]
    return nodes

NODES_TO_MONITOR = load_nodes_from_csv()

# MQTT Configuration
MQTT_BROKER = "host.docker.internal"  # This allows connecting to the host machine from within the container
MQTT_PORT = 1883
MQTT_CLIENT_ID = "opcua_mqtt_publisher"
MQTT_TOPIC = "plant1"

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print(f"Failed to connect to MQTT Broker, return code {rc}")

    client = mqtt_client.Client(MQTT_CLIENT_ID)
    client.on_connect = on_connect
    client.connect(MQTT_BROKER, MQTT_PORT)
    return client

def find_nodes(opcua_client):
    nodes = {}
    root = opcua_client.get_root_node()
    # print(root)
    def recursive_find(node):
        for child in node.get_children():
            if child.get_browse_name().Name in NODES_TO_MONITOR:
                nodes[child.get_browse_name().Name] = child
            else:
                recursive_find(child)

    recursive_find(root)
    return nodes

def read_opcua_data(opcua_client, mqtt_client, nodes):
    try:
        while True:
            for node_name, node in nodes.items():
                value = node.get_value()
                print(f"Node {node_name} value: {value}")

                # Publish to MQTT
                payload = json.dumps({"node_id": node_name, "value": value})
                mqtt_client.publish(MQTT_TOPIC, payload)
                print(f"Published to MQTT: {payload}")

            time.sleep(5)  # Wait for 5 seconds before next read
    except Exception as e:
        print(f"Error: {e}")

def main():
    mqtt_client = connect_mqtt()
    mqtt_client.loop_start()
    

    opcua_client = Client(OPC_SERVER_URL)
    try:
        opcua_client.connect()
        print("Connected to OPC UA server.")
        nodes = find_nodes(opcua_client)
        print(nodes)
        if nodes:
            print(f"Found nodes: {', '.join(nodes.keys())}")
            read_opcua_data(opcua_client, mqtt_client, nodes)
        else:
            print("No nodes found. Check your node names and server configuration.")
    except Exception as e:
        print(f"Failed to connect to OPC UA server: {e}")
    finally:
        opcua_client.disconnect()
        print("Disconnected from OPC UA server.")
        mqtt_client.loop_stop()

if __name__ == "__main__":
    main()

