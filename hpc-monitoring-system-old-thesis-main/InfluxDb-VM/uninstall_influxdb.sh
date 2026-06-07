#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Stop InfluxDB service
echo "Stopping InfluxDB service..."
sudo service influxdb stop

# Disable InfluxDB service
echo "Disabling InfluxDB service..."
sudo systemctl disable influxdb

# Remove InfluxDB package
echo "Removing InfluxDB package..."
sudo apt-get remove --purge influxdb2 -y

# Remove InfluxDB configuration files
echo "Removing InfluxDB configuration files..."
sudo rm -rf /etc/influxdb

# Remove InfluxDB systemd files
echo "Removing InfluxDB systemd files..."
sudo rm -f /etc/systemd/system/influxdb.service

# Optionally, clean up dependencies
echo "Cleaning up unused dependencies..."
sudo apt-get autoremove -y
sudo apt-get clean

echo "InfluxDB has been uninstalled successfully!"
