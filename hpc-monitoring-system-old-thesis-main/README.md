# HPC System Monitoring Tool Deployment Guide

## Head Node Configuration

### Prerequisites

1. Update the system and install essential utilities

```
sudo apt update && sudo apt install -y curl wget git
```

2. (Optional) Verify required packages are installed

```
sudo apt list --installed | grep -E "(python3|pip|curl|wget|git)" | head -10
```

3. Install the dos2unix utility for file format conversion

```
sudo apt install -y dos2unix
```

4. Get to know the TIG stack (Telegraf, InfluxDB, Grafana) so you can understand how the system works better.

### InfluxDB Database Layer Configuration

1. Navigate to the InfluxDB-VM directory

```
cd ./InfluxDb-VM
```

2. Convert the installation script to Unix format

```
dos2unix influxdb.sh
```

3. Grant execution permissions and run the InfluxDB installation

```
chmod +x influxdb.sh && ./influxdb.sh
```

4. Once InfluxDB has started successfully, navigate to http://localhost:8086 to create an account. Securely store the generated authentication token. You need the following information to create an account:

- Username
- Password
- Organization
- Bucket

  **Important:** Keep these login details for later configuration steps.

5. Get the system IP address for later use

```
hostname -I | awk '{print $1}'
```

6. Create the environment configuration file

```
sudo tee /root/.env > /dev/null <<EOF
INFLUX_URL=http://localhost:8086
INFLUX_TOKEN=<replace with your token>
INFLUX_ORG=<replace with your organization>
INFLUX_BUCKET_VM=<replace with your bucket>
INFLUX_HOSTNAME=<replace with your IP address>
EOF
```

7. (Optional) Verify the InfluxDB service status

```
sudo systemctl status influxdb --no-pager -l
```

### Grafana Visualization Layer Configuration

1. Navigate to the Grafana-VM directory

```
cd ../Grafana-VM
```

2. Convert the installation script to Unix format

```
dos2unix grafana.sh
```

3. Grant execution permissions and run the Grafana installation

```
chmod +x grafana.sh && ./grafana.sh
```

4. (Optional) Verify the Grafana service status

```
sudo systemctl status grafana-server --no-pager -l
```

### Telegraf Agent Configuration (Head Node and Compute Nodes)

1. Navigate to the GPU-VM directory

```
cd ../GPU-VM
```

2. Convert the installation script to Unix format

```
dos2unix telegraf.sh
```

3. Grant execution permissions and run the Telegraf installation

```
chmod +x telegraf.sh && ./telegraf.sh
```

4. (Optional) Verify the Telegraf service status

```
sudo systemctl status telegraf --no-pager -l
```


## Multidisciplinary run guide

### To run EdgeX and register

```bash
docker compose -f docker-compose-no-secty.yml up -d
curl -X POST http://localhost:59881/api/v3/deviceprofile/uploadfile   -F "file=@telegraf-profile.yaml"
curl -X POST http://localhost:59881/api/v3/device   -H "Content-Type: application/json"   -d @telegraf-device.json
```

### To run telegraf and Python json flatten

```bash
docker compose -f docker-compose-no-secty.yml up -d
```

### Check if telegraf and python flattener work

```bash
docker logs -f telegraf-edge
docker logs -f telegraf-bridge
```

