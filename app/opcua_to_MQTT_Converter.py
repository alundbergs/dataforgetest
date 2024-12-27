from opcua import Client
from paho.mqtt import client as mqtt_client
import time
import json
import csv
import os

OPC_SERVER_URL = "opc.tcp://host.docker.internal:4841"
MQTT_BROKER = "host.docker.internal"
MQTT_PORT = 1883
MQTT_TOPIC = "plant1"
SELECTED_CSV = "app/selected.csv"
DEFAULT_READ_INTERVAL = 5

def read_selected_nodes():
    with open(SELECTED_CSV, mode='r') as file:
        return [row['node_id'] for row in csv.DictReader(file)]

def connect_mqtt():
    client = mqtt_client.Client()
    client.connect(MQTT_BROKER, MQTT_PORT)
    print("Connected to MQTT Broker")
    return client

def read_opcua_data(opcua_client, mqtt_client):
    opcua_client.connect()
    print("Connected to OPC UA server")

    while True:
        for node_id in read_selected_nodes():
            try:
                value = opcua_client.get_node(node_id).get_value()
                node_id_short = node_id.replace("ns=2;s=DB15.", "")
                mqtt_payload = json.dumps({"node_id": node_id_short, "value": value})
                mqtt_client.publish(MQTT_TOPIC, mqtt_payload)
                print(f"Published: {node_id_short} = {value}")
            except Exception as e:
                print(f"Error reading {node_id}: {e}")
        read_interval = int(os.environ.get('READ_INTERVAL', DEFAULT_READ_INTERVAL))
        time.sleep(read_interval)

def main():
    mqtt_client = connect_mqtt()
    opcua_client = Client(OPC_SERVER_URL)
    try:
        read_opcua_data(opcua_client, mqtt_client)
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        opcua_client.disconnect()
        mqtt_client.disconnect()
        print("Disconnected from servers")

if __name__ == "__main__":
    main()