#!/bin/bash

set -e

# Set non-interactive frontend to bypass prompts
export DEBIAN_FRONTEND=noninteractive

echo "Updating system and installing required packages..."
sudo apt update
sudo apt install -y python3 python3-pip sshpass

echo "Installing Python packages..."
pip3 install psutil requests
pip3 install python-dotenv

# Define paths
SCRIPT_PATH="/etc/telegraf/hpc-monitor.py"
LOG_PATH="/etc/telegraf/hpc-monitor.log"
SERVICE_PATH="/etc/systemd/system/hpc-monitor.service"

echo "Writing Python monitoring script..."
sudo tee "$SCRIPT_PATH" > /dev/null <<'EOF'
import os
import psutil
import time
import requests
import subprocess
from datetime import datetime
from dotenv import load_dotenv

load_dotenv("/root/.env")

# Access the environment variables
OUTPUT_FILE = os.getenv("OUTPUT_FILE")
REMOTE_USER = os.getenv("REMOTE_USER")
REMOTE_HOST = os.getenv("REMOTE_HOST")
REMOTE_PATH = os.getenv("REMOTE_PATH")
IP = os.getenv("IP")
PASSWORD = os.getenv("CENTRAL_PASSWORD")

def escape_influxdb_tag(value):
    return str(value).replace("\\", r"\\").replace(" ", r"\ ").replace(",", r"\,").replace("=", r"\=")

def get_process_metrics():
    timestamp = int(datetime.utcnow().timestamp() * 1e9
	
    with open(OUTPUT_FILE, "w") as f:
        for proc in psutil.process_iter(['pid', 'name', 'username', 'cpu_percent', 'memory_percent']):
            try:
                pid = proc.info['pid']
                comm = proc.info['name']
                user = proc.info['username']
                cpu = proc.info['cpu_percent']
                mem = proc.info['memory_percent']
                cmdline_raw = " ".join(proc.cmdline()) if proc.cmdline() else comm
                cmdline = escape_influxdb_tag(cmdline_raw)

                ppid = proc.ppid()
                prev_ppid = ppid
                while ppid > 1:
                    parent_proc = psutil.Process(ppid)
                    prev_ppid = ppid
                    ppid = parent_proc.ppid()
                    if ppid == 1:
                        break
                ppid = prev_ppid

                read_bytes = 0
                write_bytes = 0
                ppid_comm = ""

                try:
                    io_stats = proc.io_counters()
                    read_bytes = io_stats.read_bytes
                    write_bytes = io_stats.write_bytes
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass

                try:
                    if ppid == 1:
                        ppid_comm = comm
                    elif ppid > 1:
                        parent_proc = psutil.Process(ppid)
                        ppid_comm = parent_proc.name()
                    else:
                        ppid_comm = "systemd"
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    ppid_comm = "Unknown"

                line_protocol = f"hpc-monitoring,ip={IP},user={user},comm={cmdline},pid={pid},ppid={ppid},app={ppid_comm} cpu_percent={cpu},mem_percent={mem},read_bytes={read_bytes},write_bytes={write_bytes} {timestamp}"
                f.write(line_protocol + "\n")
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue

def send_file():
    try:
        subprocess.run([
            "sshpass", "-p", PASSWORD,
            "scp", "-o", "StrictHostKeyChecking=no",
            OUTPUT_FILE,
            f"{REMOTE_USER}@{REMOTE_HOST}:{REMOTE_PATH}"
        ], check=True)
        print(f"[{datetime.now()}] File sent successfully to {REMOTE_HOST}")
    except subprocess.CalledProcessError as e:
        print(f"[{datetime.now()}] Failed to send file: {e}")

while True:
    get_process_metrics()
    send_file()
    time.sleep(4)
EOF

echo "Setting executable permissions on script..."
sudo chmod +x "$SCRIPT_PATH"

echo "Creating systemd service..."
sudo tee "$SERVICE_PATH" > /dev/null <<EOF
[Unit]
Description=HPC Monitor Service
After=network.target

[Service]
ExecStart=/usr/bin/python3 $SCRIPT_PATH
WorkingDirectory=/etc/telegraf
User=root
Restart=always
RestartSec=3

StandardOutput=append:$LOG_PATH
StandardError=append:$LOG_PATH

[Install]
WantedBy=multi-user.target
EOF

echo "Reloading systemd daemon and starting service..."
sudo systemctl daemon-reload
sudo systemctl enable hpc-monitor
sudo systemctl restart hpc-monitor

echo "HPC Monitor service installed and running!"
