#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Update system packages
echo "Updating package lists..."
sudo apt update

# Install dependencies for Grafana
echo "Installing dependencies..."
sudo apt-get install -y adduser libfontconfig1 musl

# Detect system architecture
echo "Detecting system architecture..."
ARCH=$(uname -m)
echo "Detected architecture: $ARCH"

# Set Grafana package based on architecture
if [ "$ARCH" = "x86_64" ]; then
    GRAFANA_PACKAGE="grafana_11.2.0_amd64.deb"
elif [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
    GRAFANA_PACKAGE="grafana_11.2.0_arm64.deb"
else
    echo "Unsupported architecture: $ARCH"
    exit 1
fi

# Download Grafana .deb package
echo "Downloading Grafana for $ARCH..."
wget https://dl.grafana.com/oss/release/$GRAFANA_PACKAGE

# Install Grafana
echo "Installing Grafana..."
sudo dpkg -i $GRAFANA_PACKAGE

# Start Grafana service
echo "Starting Grafana service..."
sudo service grafana-server start

# Check the status of Grafana service
echo "Checking Grafana service status..."
sudo service grafana-server status

echo "Grafana installation and setup complete!"
