#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Stop the Telegraf service
echo "Stopping Telegraf service..."
sudo service telegraf stop

# Disable Telegraf service
echo "Disabling Telegraf service..."
sudo systemctl disable telegraf

# Remove Telegraf package
echo "Removing Telegraf package..."
sudo apt-get remove --purge telegraf -y

# Remove Telegraf configuration files
echo "Removing Telegraf configuration files..."
sudo rm -rf /etc/telegraf

# Remove Telegraf service file
echo "Removing Telegraf service file..."
sudo rm -f /etc/systemd/system/telegraf.service

echo "Telegraf has been uninstalled successfully!"
