from fastapi import FastAPI, HTTPException, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import csv
import os
import logging
import subprocess
import psutil
import asyncio
import paho.mqtt.client as mqtt
from datetime import datetime

# Set up FastAPI and templates
app = FastAPI()
templates = Jinja2Templates(directory="app/templates")

# Define file paths
DATA_CSV = "app/data.csv"
SELECTED_CSV = "app/selected.csv"
LOG_FILE = "app/logs/application.log"
OPCUA_TO_MQTT_LOG_FILE = "app/logs/opcua_to_mqtt.log"
MQTT_TO_INFLUX_LOG_FILE = "app/logs/mqtt_to_influx.log"

# Ensure log directory exists
os.makedirs("app/logs", exist_ok=True)


# Add these global variables with correct paths
OPCUA_TO_MQTT_SCRIPT = "app/opcua_to_MQTT_Converter.py"
MQTT_TO_INFLUX_SCRIPT = "app/mqtt_to_Influx_Converter.py"

# Set up logging
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global variables to store subprocess objects
opcua_to_mqtt_process = None
mqtt_to_influx_process = None

# Ensure CSV files exist
def ensure_csv(file_path, header):
    if not os.path.exists(file_path):
        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header)
ensure_csv(DATA_CSV, ["node_id", "description"])
ensure_csv(SELECTED_CSV, ["node_id", "description"])

# Add these functions after the existing imports and before the routes

def is_process_running(script_name):
    for process in psutil.process_iter(['name', 'cmdline']):
        if process.info['name'] == 'python' and script_name in process.info['cmdline']:
            return True
    return False

def start_script(script_name):
    if not is_process_running(script_name):
        subprocess.Popen(["python3", script_name])
        return True
    return False

def stop_script(script_name):
    for process in psutil.process_iter(['name', 'cmdline']):
        if process.info['name'] == 'python' and script_name in process.info['cmdline']:
            process.terminate()
            return True
    return False
# API Models
class Node(BaseModel):
    node_id: str
    description: str

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Render the main page with data points, checkboxes, and converter status."""
    try:
        with open(DATA_CSV, mode='r') as file:
            reader = csv.DictReader(file)
            nodes = [row for row in reader]
        with open(SELECTED_CSV, mode='r') as file:
            selected = {row['node_id'] for row in csv.DictReader(file)}

        # Get converter status
        opcua_to_mqtt_status = "running" if is_process_running(OPCUA_TO_MQTT_SCRIPT) else "stopped"
        mqtt_to_influx_status = "running" if is_process_running(MQTT_TO_INFLUX_SCRIPT) else "stopped"
    except Exception as e:
        logging.error(f"Error reading CSV files or getting converter status: {e}")
        nodes = []
        selected = set()
        opcua_to_mqtt_status = "unknown"
        mqtt_to_influx_status = "unknown"

    return templates.TemplateResponse("index.html", {
        "request": request, 
        "nodes": nodes, 
        "selected": selected,
        "opcua_to_mqtt_status": opcua_to_mqtt_status,
        "mqtt_to_influx_status": mqtt_to_influx_status
    })

@app.post("/add_node")
async def add_node(node_id: str = Form(...), description: str = Form(...)):
    """Add a new node to the data.csv file."""
    try:
        with open(DATA_CSV, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([node_id, description])
        logging.info(f"Added new node: {node_id} - {description}")
        return JSONResponse(content={"message": "Node added successfully", "node": {"node_id": node_id, "description": description}})
    except Exception as e:
        logging.error(f"Error adding node: {e}")
        return JSONResponse(content={"error": "Failed to add node"}, status_code=500)

class UpdateRequest(BaseModel):
    node_ids: list[str] = []
@app.post("/update")
async def update_selected(request: UpdateRequest):
    """Update the selected nodes based on checkboxes."""
    try:
        node_ids = request.node_ids
        with open(DATA_CSV, mode='r') as file:
            reader = csv.DictReader(file)
            all_nodes = [row for row in reader]
        selected_nodes = [node for node in all_nodes if node['node_id'] in node_ids]
        with open(SELECTED_CSV, mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=["node_id", "description"])
            writer.writeheader()
            writer.writerows(selected_nodes)

        # Restart OPC UA to MQTT converter if it's running
        if is_process_running(OPCUA_TO_MQTT_SCRIPT):
            stop_script(OPCUA_TO_MQTT_SCRIPT)
            await asyncio.sleep(1)  # Wait for process to stop
            await start_script(OPCUA_TO_MQTT_SCRIPT)
            logging.info("OPC UA to MQTT converter restarted with new node selection")

        # Log the selection update
        if node_ids:
            logging.info(f"Selection updated. Selected nodes: {', '.join(node_ids)}")
        else:
            logging.info("Selection updated. No nodes selected.")

        message = "Selection updated successfully" if node_ids else "All nodes deselected"
        return JSONResponse(content={"message": message, "selected_nodes": selected_nodes})
    except Exception as e:
        logging.error(f"Error updating selection: {e}")
        return JSONResponse(content={"error": "Failed to update selection"}, status_code=500)

@app.post("/clear_logs")
async def clear_logs():
    """Clear the contents of all log files."""
    try:
        for log_file in [LOG_FILE, OPCUA_TO_MQTT_LOG_FILE, MQTT_TO_INFLUX_LOG_FILE]:
            open(log_file, 'w').close()
        return {"message": "All logs cleared successfully"}
    except Exception as e:
        logging.error(f"Error clearing logs: {e}")
        raise HTTPException(status_code=500, detail="Failed to clear logs")

async def run_script(script_name):
    global opcua_to_mqtt_process, mqtt_to_influx_process
    try:
        log_file_path = OPCUA_TO_MQTT_LOG_FILE if script_name == OPCUA_TO_MQTT_SCRIPT else MQTT_TO_INFLUX_LOG_FILE
        with open(log_file_path, 'a') as log_file:
            process = await asyncio.create_subprocess_exec(
                'python3', script_name,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            if script_name == OPCUA_TO_MQTT_SCRIPT:
                opcua_to_mqtt_process = process
            elif script_name == MQTT_TO_INFLUX_SCRIPT:
                mqtt_to_influx_process = process

            while True:
                line = await process.stdout.readline()
                if not line:
                    break
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                log_file.write(f"[{timestamp}] {script_name}: {line.decode().strip()}\n")
                log_file.flush()

            error = await process.stderr.read()
            if error:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                log_file.write(f"[{timestamp}] {script_name} Error: {error.decode().strip()}\n")
                log_file.flush()

            await process.wait()
    except Exception as e:
        logging.error(f"Error running {script_name}: {e}")

def is_process_running(script_name):
    global opcua_to_mqtt_process, mqtt_to_influx_process
    if script_name == OPCUA_TO_MQTT_SCRIPT:
        return opcua_to_mqtt_process is not None and opcua_to_mqtt_process.returncode is None
    elif script_name == MQTT_TO_INFLUX_SCRIPT:
        return mqtt_to_influx_process is not None and mqtt_to_influx_process.returncode is None
    return False

async def start_script(script_name):
    if not is_process_running(script_name):
        asyncio.create_task(run_script(script_name))
        return True
    return False

def stop_script(script_name):
    global opcua_to_mqtt_process, mqtt_to_influx_process
    if script_name == OPCUA_TO_MQTT_SCRIPT and opcua_to_mqtt_process:
        opcua_to_mqtt_process.terminate()
        opcua_to_mqtt_process = None
        return True
    elif script_name == MQTT_TO_INFLUX_SCRIPT and mqtt_to_influx_process:
        mqtt_to_influx_process.terminate()
        mqtt_to_influx_process = None
        return True
    return False

# Add this function after your existing imports
def test_mqtt_connection():
    client = mqtt.Client()
    try:
        client.connect("host.docker.internal", 1883, 60)
        client.disconnect()
        return True
    except Exception as e:
        logging.error(f"MQTT connection test failed: {e}")
        return False
@app.post("/toggle_opcua_to_mqtt")
async def toggle_opcua_to_mqtt():
    if is_process_running(OPCUA_TO_MQTT_SCRIPT):
        if stop_script(OPCUA_TO_MQTT_SCRIPT):
            logging.info("OPC UA to MQTT converter stopped")
            return {"message": "OPC UA to MQTT converter stopped"}
        else:
            logging.error("Failed to stop OPC UA to MQTT converter")
            return JSONResponse(content={"error": "Failed to stop converter"}, status_code=500)
    else:
        if await start_script(OPCUA_TO_MQTT_SCRIPT):
            logging.info("OPC UA to MQTT converter started")
            return {"message": "OPC UA to MQTT converter started"}
        else:
            logging.error("Failed to start OPC UA to MQTT converter")
            return JSONResponse(content={"error": "Failed to start converter"}, status_code=500)

@app.post("/toggle_mqtt_to_influx")
async def toggle_mqtt_to_influx():
    if is_process_running(MQTT_TO_INFLUX_SCRIPT):
        if stop_script(MQTT_TO_INFLUX_SCRIPT):
            logging.info("MQTT to InfluxDB converter stopped")
            return {"message": "MQTT to InfluxDB converter stopped"}
        else:
            logging.error("Failed to stop MQTT to InfluxDB converter")
            return JSONResponse(content={"error": "Failed to stop converter"}, status_code=500)
    else:
        if await start_script(MQTT_TO_INFLUX_SCRIPT):
            logging.info("MQTT to InfluxDB converter started")
            return {"message": "MQTT to InfluxDB converter started"}
        else:
            logging.error("Failed to start MQTT to InfluxDB converter")
            return JSONResponse(content={"error": "Failed to start converter"}, status_code=500)


@app.get("/logs", response_class=HTMLResponse)
async def get_logs(request: Request):
    """Display application logs and errors."""
    log_files = {
        "Application": LOG_FILE,
        "opcua_to_MQTT_Converter.py": OPCUA_TO_MQTT_LOG_FILE,
        "mqtt_to_Influx_Converter.py": MQTT_TO_INFLUX_LOG_FILE
    }

    logs = {}
    for script, log_file in log_files.items():
        try:
            with open(log_file, mode="r") as file:
                logs[script] = file.readlines()
        except Exception as e:
            logs[script] = [f"Error reading log file: {e}\n"]

    return templates.TemplateResponse("logs.html", {"request": request, "logs": logs})

@app.get("/converter_status")
async def get_converter_status():
    opcua_to_mqtt_status = "running" if is_process_running(OPCUA_TO_MQTT_SCRIPT) else "stopped"
    mqtt_to_influx_status = "running" if is_process_running(MQTT_TO_INFLUX_SCRIPT) else "stopped"
    return {
        "opcua_to_mqtt": opcua_to_mqtt_status,
        "mqtt_to_influx": mqtt_to_influx_status
    }

# Add this new endpoint
@app.get("/test_mqtt")
async def test_mqtt():
    if test_mqtt_connection():
        return {"message": "MQTT connection successful"}
    else:
        return JSONResponse(content={"error": "MQTT connection failed"}, status_code=500)
@app.get("/get_latest_logs")
@app.get("/get_latest_logs")
async def get_latest_logs():
    log_files = {
        "Application": LOG_FILE,
        "opcua_to_MQTT_Converter.py": OPCUA_TO_MQTT_LOG_FILE,
        "mqtt_to_Influx_Converter.py": MQTT_TO_INFLUX_LOG_FILE
    }

    latest_logs = {}
    for script, log_file in log_files.items():
        try:
            with open(log_file, mode="r") as file:
                lines = file.readlines()
                latest_logs[script] = lines[-100:][::-1]  # Get last 100 lines and reverse them
        except Exception as e:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            error_message = f"[{timestamp}] Error reading log file: {e}\n"
            latest_logs[script] = [error_message]

    return latest_logs
