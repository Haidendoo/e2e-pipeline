#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Stop the services
echo "Stopping eBPF monitoring services..."
sudo systemctl stop tcptop
sudo systemctl stop cpudist

# Disable the services
echo "Disabling eBPF monitoring services..."
sudo systemctl disable tcptop
sudo systemctl disable cpudist

# Remove the systemd service files
echo "Removing systemd service files..."
sudo rm -f /etc/systemd/system/tcptop.service
sudo rm -f /etc/systemd/system/cpudist.service

# Remove the Python scripts from Telegraf directory
echo "Removing Python scripts from Telegraf directory..."
sudo rm -f /etc/telegraf/tcptop.py
sudo rm -f /etc/telegraf/cpudist.py

# Remove the custom bcc-thesis repository
echo "Removing custom bcc-thesis repository..."
sudo rm -rf /root/bcc-thesis

# Uninstall BCC and its dependencies
echo "Removing BCC and related dependencies..."
cd /root/bcc/build
make uninstall

# Remove BCC repository and build directory
echo "Removing BCC repository..."
sudo rm -rf /root/bcc

# Remove installed Python packages
echo "Removing Python packages..."
sudo pip3 uninstall -y python-dotenv influxdb-client

# Remove system dependencies
echo "Removing system dependencies..."
sudo apt-get remove --purge -y zip bison build-essential cmake flex git libedit-dev \
  libllvm14 llvm-14-dev libclang-14-dev python3 zlib1g-dev libelf-dev libfl-dev python3-setuptools \
  liblzma-dev libdebuginfod-dev arping netperf iperf

echo "BCC monitoring services and related packages have been uninstalled successfully!"
