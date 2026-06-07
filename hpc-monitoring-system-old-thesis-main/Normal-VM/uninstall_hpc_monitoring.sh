#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Stop the HPC monitor service
echo "Stopping HPC Monitor service..."
sudo systemctl stop hpc-monitor

# Disable the HPC monitor service
echo "Disabling HPC Monitor service..."
sudo systemctl disable hpc-monitor

# Remove the systemd service file
echo "Removing HPC Monitor systemd service file..."
sudo rm -f /etc/systemd/system/hpc-monitor.service

# Remove the Python script
echo "Removing Python script..."
sudo rm -f /etc/telegraf/hpc-monitor.py

# Remove the log directory if needed
echo "Removing log directory..."
sudo rm -rf /etc/telegraf/logDeploy

# Optionally, clean up unused dependencies (Python packages)
echo "Cleaning up Python packages..."
sudo pip3 uninstall -y psutil requests

echo "HPC Monitor has been uninstalled successfully!"
