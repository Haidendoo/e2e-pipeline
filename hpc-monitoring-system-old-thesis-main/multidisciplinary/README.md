## Multidisciplinary run guide

### To run EdgeX and register

Go to the edgeX repo

```bash
docker compose -f docker-compose-no-secty.yml up -d
cd config
curl -X POST http://localhost:59881/api/v3/deviceprofile/uploadfile   -F "file=@telegraf-profile.yaml"
curl -X POST http://localhost:59881/api/v3/device   -H "Content-Type: application/json"   -d @telegraf-device.json
```

### To run telegraf and Python json flatten

Go to this repo

```bash
cd multidisciplinary
docker compose up -d --build
```

### Check if telegraf and python flattener work

```bash
docker logs -f telegraf-edge
docker logs -f telegraf-bridge
```