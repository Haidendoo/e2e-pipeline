#!/bin/bash

set -e

# Set non-interactive frontend to bypass prompts
export DEBIAN_FRONTEND=noninteractive

sudo apt update

echo "=== Installing Python packages ==="
pip install python-dotenv influxdb-client

echo "=== Installing system dependencies ==="
sudo apt install -y zip bison build-essential cmake flex git libedit-dev \
  libllvm14 llvm-14-dev libclang-14-dev python3 zlib1g-dev libelf-dev libfl-dev python3-setuptools \
  liblzma-dev libdebuginfod-dev arping netperf iperf

echo "=== Cloning and building BCC ==="
git clone https://github.com/iovisor/bcc.git
mkdir bcc/build; cd bcc/build
cmake ..
make
sudo make install

echo "=== Cloning custom thesis repository ==="
cd /root/
git clone git@github.com:knammm/thesis-project.git

echo "=== Copying custom scripts to BCC directories ==="
cp /root/thesis-project/src/cpudist.py bcc/tools/
cp /root/thesis-project/src/table.py bcc/src/python/bcc/
cp /root/thesis-project/src/tcptop.py bcc/tools/

echo "=== Building BCC Python3 bindings ==="
cd /root/bcc/build
cmake -DPYTHON_CMD=python3 ..
pushd src/python/
make
sudo make install
popd

echo "=== Copying tools to Telegraf directory ==="
sudo cp /root/bcc/tools/cpudist.py /etc/telegraf/
sudo cp /root/bcc/tools/tcptop.py /etc/telegraf/

echo "=== Creating systemd service files ==="
sudo tee /etc/systemd/system/tcptop.service > /dev/null <<EOF
[Unit]
Description=eBPF tcptop Service
After=network.target

[Service]
ExecStart=/usr/bin/python3 /etc/telegraf/tcptop.py -C
User=root
WorkingDirectory=/etc/telegraf
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

sudo tee /etc/systemd/system/cpudist.service > /dev/null <<EOF
[Unit]
Description=eBPF cpudist Service
After=network.target

[Service]
ExecStart=/usr/bin/python3 /etc/telegraf/cpudist.py 5
User=root
WorkingDirectory=/etc/telegraf
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

echo "=== Enabling and starting services ==="
sudo systemctl daemon-reload
sudo systemctl enable tcptop
sudo systemctl start tcptop
sudo systemctl enable cpudist
sudo systemctl start cpudist

echo "=== Done! Monitoring services are now running. ==="
cd /root/
