#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Set non-interactive frontend to bypass prompts
export DEBIAN_FRONTEND=noninteractive

INSTALL_DIR="/opt/telegraf-scripts" 

# Get the directory where this script is stored
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Name of the source Python script (just the filename)
TCP_SCRIPT_FILENAME="tcptop.py"

# Full path to the source file
SOURCE_FILE_PATH="$SCRIPT_DIR/$TCP_SCRIPT_FILENAME"

# Update and Install dependencies
echo "Updating package lists..."
sudo apt-get update

# Install dependencies for bcc
echo "Updating package lists..."
sudo apt-get update
sudo apt-get install -y linux-headers-$(uname -r) bpfcc-tools python3-bpfcc wget

# Check if nvidia-smi is available
echo "Verifying NVIDIA drivers..."
if ! command -v nvidia-smi &> /dev/null; then
    echo "WARNING: nvidia-smi not found. Please install NVIDIA drivers first."
    echo "Run: sudo apt install -y nvidia-driver-525"
    exit 1
fi

nvidia-smi
echo "NVIDIA drivers verified successfully."

# Download Telegraf
if ! command -v telegraf &> /dev/null; then
  echo "Downloading Telegraf..."
  wget https://dl.influxdata.com/telegraf/releases/telegraf_1.27.1-1_amd64.deb
  # Install Telegraf
  echo "Installing Telegraf..."
  sudo dpkg -i telegraf_1.27.1-1_amd64.deb
  rm telegraf_1.27.1-1_amd64.deb
else
  echo "Telegraf is already installed. Skipping download and install."
fi

# Navigate to Telegraf configuration directory
echo "Navigating to /etc/telegraf..."
cd /etc/telegraf

# Remove default configuration
echo "Removing default config..."
sudo rm -f telegraf.conf

# Load environment variables
echo "Loading environment variables..."
set -a
source <(sudo cat /root/.env)
set +a

# SETUP SCRIPTS DIRECTORY
echo "Setting up scripts directory at $INSTALL_DIR..."

# Create directory if it does not exist
if [ ! -d "$INSTALL_DIR" ]; then
    sudo mkdir -p $INSTALL_DIR
    echo "Created directory $INSTALL_DIR"
fi

# copy script to telegraf directory
# copy script to telegraf directory
if [ -f "$SOURCE_FILE_PATH" ]; then
    # We copy FROM the source path TO the install directory + filename
    sudo cp "$SOURCE_FILE_PATH" "$INSTALL_DIR/$TCP_SCRIPT_FILENAME"
    echo "Copied $TCP_SCRIPT_FILENAME to $INSTALL_DIR"
else
    echo "ERROR: File $TCP_SCRIPT_FILENAME not found in $SCRIPT_DIR!"
    exit 1
fi

# Only root can modify files in this directory to prevent privilege escalation
sudo chown -R root:root $INSTALL_DIR
sudo chmod -R 755 $INSTALL_DIR

# Allow telegraf user to run any python script inside /opt/telegraf-scripts/
echo "telegraf ALL=(root) NOPASSWD: /usr/bin/python3 $INSTALL_DIR/*" | sudo tee /etc/sudoers.d/telegraf_custom_scripts
sudo chmod 0440 /etc/sudoers.d/telegraf_custom_scripts

# Detect nvidia-smi path
echo "Detecting nvidia-smi path..."
NVIDIA_SMI_PATH=$(which nvidia-smi)
echo "Found nvidia-smi at: $NVIDIA_SMI_PATH"

# Create custom telegraf.conf
echo "Creating custom telegraf.conf..."
sudo tee telegraf.conf > /dev/null <<EOF
# Agent Configuration
[agent]
  interval = "5s"
  flush_interval = "5s"
  hostname = "$INFLUX_HOSTNAME"

# Output Plugin for InfluxDB
[[outputs.influxdb_v2]]
  urls = ["$INFLUX_URL"]
  token = "$INFLUX_TOKEN"
  organization = "$INFLUX_ORG"
  bucket = "$INFLUX_BUCKET_VM"
  timeout = "20s"

# Read metrics about cpu usage
[[inputs.cpu]]
  percpu = true
  totalcpu = true
  collect_cpu_time = false
  report_active = false
  core_tags = true

# Read metrics about disk IO by device
[[inputs.diskio]]

# Read metrics about memory usage
[[inputs.mem]]

[[inputs.processes]]

[[inputs.system]]

# Read NVIDIA GPU metrics
[[inputs.nvidia_smi]]
  bin_path = "$NVIDIA_SMI_PATH"
  timeout = "5s"


# Manual GPU process read
[[inputs.exec]]
  commands = ["nvidia-smi --query-compute-apps=pid,process_name,used_memory --format=csv,noheader,nounits"]

  data_format = "csv"
  # csv_trim_space = true
  csv_header_row_count = 0
  csv_column_names = ["pid", "process_name", "used_memory"]

  csv_tag_columns = ["pid", "process_name"]

  name_override = "nvidia_smi_process"

# Telegraf exec the python script to get metrics
[[inputs.exec]]
  commands = ["sudo /usr/bin/python3 $INSTALL_DIR/$TCP_SCRIPT_FILENAME 1 1"]
  timeout = "4s"
  data_format = "influx"
EOF

# Configure telegraf service
echo "Configuring telegraf service..."
sudo tee /etc/systemd/system/telegraf.service > /dev/null <<EOF
[Service]
EnvironmentFile=/root/.env
ExecStart=/usr/bin/telegraf --config /etc/telegraf/telegraf.conf
EOF

# Add permission for Telegraf to reach NVIDIA SMI
sudo usermod -aG video telegraf

# Restart Telegraf service
echo "Restarting Telegraf service..."
sudo systemctl daemon-reload
sudo systemctl restart telegraf
sudo systemctl status telegraf --no-pager -l
echo "Telegraf installation and configuration complete!"

echo "Checking NVIDIA SMI plugin..."
sudo telegraf --config /etc/telegraf/telegraf.conf --input-filter nvidia_smi --test