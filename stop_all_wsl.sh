#!/bin/bash

echo "🛑 BẮT ĐẦU DỪNG HỆ THỐNG E2E PIPELINE TRÊN WSL"
echo "------------------------------------------------------"

ROOT_DIR=$(pwd)

# Bước 1: Dừng HPC Monitoring
echo -e "\n[1/3] Đang dừng HPC Monitoring..."
cd $ROOT_DIR/hpc-monitoring-system
docker compose down

# Bước 2: Dừng EdgeX Foundry
echo -e "\n[2/3] Đang dừng EdgeX Foundry..."
cd $ROOT_DIR/EdgeX-Foundry-main
docker compose -f docker-compose-no-secty.yml down

# Bước 3: Dừng E2E Data Pipeline & Storage Stack
echo -e "\n[3/3] Đang dừng E2E Data Pipeline & Storage Stack..."
cd $ROOT_DIR/e2e-pipeline-main
docker compose -f storage.docker-compose.yaml down
docker compose down

echo "------------------------------------------------------"
echo "✅ HOÀN TẤT DỪNG HỆ THỐNG!"
