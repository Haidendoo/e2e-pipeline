#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive

# Directory to store all custom monitoring scripts
INSTALL_DIR="/opt/telegraf-scripts"

# Name of the source Python script
TCP_SCRIPT_FILENAME="../src/tcptop.py"

if [ ! -f /root/.env ]; then
    echo "ERROR: /root/.env file not found!"
    exit 1
fi
set -a
source /root/.env
set +a

# Install dependencies for bcc
echo "Updating package lists..."
sudo apt-get update
sudo apt-get install -y linux-headers-$(uname -r) bpfcc-tools python3-bpfcc wget

# VERIFY NVIDIA DRIVERS
if ! command -v nvidia-smi &> /dev/null; then
    echo "WARNING: nvidia-smi not found."
    exit 1
fi

# INSTALL TELEGRAF
if ! command -v telegraf &> /dev/null; then
  echo "Telegraf not found. Downloading Telegraf..."
  wget -q https://dl.influxdata.com/telegraf/releases/telegraf_1.27.1-1_amd64.deb -O telegraf.deb
  echo "Installing Telegraf..."
  sudo dpkg -i telegraf.deb
  rm telegraf.deb
else
  echo "Telegraf is already installed. Skipping download and install."
fi

# SETUP SCRIPTS DIRECTORY
echo "Setting up scripts directory at $INSTALL_DIR..."

# Create directory if it does not exist
if [ ! -d "$INSTALL_DIR" ]; then
    sudo mkdir -p $INSTALL_DIR
    echo "Created directory $INSTALL_DIR"
fi

# copy script to telegraf directory
if [ -f "./$TCP_SCRIPT_FILENAME" ]; then
    sudo cp ./$TCP_SCRIPT_FILENAME "$INSTALL_DIR/$TCP_SCRIPT_FILENAME"
    echo "Copied $TCP_SCRIPT_FILENAME to $INSTALL_DIR"
else
    echo "ERROR: File ./$TCP_SCRIPT_FILENAME not found in current directory!"
    exit 1
fi

# Only root can modify files in this directory to prevent privilege escalation
sudo chown -R root:root $INSTALL_DIR
sudo chmod -R 755 $INSTALL_DIR

# Allow telegraf user to run any python script inside /opt/telegraf-scripts/
echo "telegraf ALL=(root) NOPASSWD: /usr/bin/python3 $INSTALL_DIR/*" | sudo tee /etc/sudoers.d/telegraf_custom_scripts
sudo chmod 0440 /etc/sudoers.d/telegraf_custom_scripts

# CONFIGURE TELEGRAF
echo "Configuring Telegraf..."
cd /etc/telegraf
sudo rm -f telegraf.conf

# Find correct nvidia_smi path
NVIDIA_SMI_PATH=$(which nvidia-smi)

sudo tee telegraf.conf > /dev/null <<EOF
[agent]
  interval = "5s"
  round_interval = true
  metric_batch_size = 1000
  metric_buffer_limit = 10000
  collection_jitter = "0s"
  flush_interval = "5s"
  flush_jitter = "0s"
  hostname = "$INFLUX_HOSTNAME"

[[outputs.influxdb_v2]]
  urls = ["$INFLUX_URL"]
  token = "$INFLUX_TOKEN"
  organization = "$INFLUX_ORG"
  bucket = "$INFLUX_BUCKET_VM"

# System Metrics
[[inputs.cpu]]
  percpu = true
  totalcpu = true
  collect_cpu_time = false
  report_active = false
  core_tags = true

[[inputs.diskio]]
[[inputs.mem]]
[[inputs.processes]]
[[inputs.system]]
[[inputs.net]]

# NVIDIA Metrics
[[inputs.nvidia_smi]]
  bin_path = "$NVIDIA_SMI_PATH"
  timeout = "5s"

# --- CUSTOM SCRIPTS INPUTS ---

# Telegraf exec the python script to get metrics
[[inputs.exec]]
  commands = ["sudo /usr/bin/python3 $INSTALL_DIR/$TCP_SCRIPT_FILENAME 1 1"]
  timeout = "4s"
  data_format = "influx"
EOF

echo "Configuring service overrides..."
sudo mkdir -p /etc/systemd/system/telegraf.service.d
sudo tee /etc/systemd/system/telegraf.service.d/override.conf > /dev/null <<EOF
[Service]
EnvironmentFile=/root/.env
EOF

# Allow telegraf to access to GPU metrics of nvidia_smi
sudo usermod -aG video telegraf

echo "Restarting Telegraf..."
sudo systemctl daemon-reload
sudo systemctl restart telegraf

echo "---------------------------------------------------"
echo "Setup Complete!"
echo "---------------------------------------------------"