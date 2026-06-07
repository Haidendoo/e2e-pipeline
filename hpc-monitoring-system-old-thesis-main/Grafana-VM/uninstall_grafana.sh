#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Stop the Grafana service
echo "Stopping Grafana service..."
sudo service grafana-server stop

# Disable Grafana service
echo "Disabling Grafana service..."
sudo systemctl disable grafana-server

# Remove Grafana package
echo "Removing Grafana package..."
sudo apt-get remove --purge grafana -y

# Clean up Grafana files
echo "Removing Grafana configuration files..."
sudo rm -rf /etc/grafana

# Clean up Grafana logs and systemd files
echo "Removing Grafana logs and systemd files..."
sudo rm -f /etc/systemd/system/grafana-server.service
sudo rm -f /lib/systemd/system/grafana-server.service

# Optionally, clean up dependencies
echo "Cleaning up unused dependencies..."
sudo apt-get autoremove -y
sudo apt-get clean

echo "Grafana has been uninstalled successfully!"
