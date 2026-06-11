#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Download InfluxDB key
echo "Downloading InfluxDB key..."
wget -q https://repos.influxdata.com/influxdata-archive_compat.key

# Verify the InfluxDB key
echo "Verifying InfluxDB key..."
echo '393e8779c89ac8d958f81f942f9ad7fb82a25e133faddaf92e15b16e6ac9ce4c influxdata-archive_compat.key' | sha256sum -c && cat influxdata-archive_compat.key | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/influxdata-archive_compat.gpg > /dev/null

# Add InfluxDB repository to sources list
echo "Adding InfluxDB repository..."
echo 'deb [signed-by=/etc/apt/trusted.gpg.d/influxdata-archive_compat.gpg] https://repos.influxdata.com/debian stable main' | sudo tee /etc/apt/sources.list.d/influxdata.list

# Update and install InfluxDB
echo "Installing InfluxDB..."
sudo apt-get update && sudo apt-get install influxdb2

# Start InfluxDB service
echo "Starting InfluxDB service..."
sudo service influxdb start

# Check the status of InfluxDB service
echo "Checking InfluxDB service status..."
sudo service influxdb status

echo "InfluxDB installation and setup complete!"
