#!/usr/bin/env bash
# ============================================================
# wait_and_register.sh
# Chờ EdgeX core-metadata khởi động xong, sau đó tự động
# đăng ký Device Profile và Device qua Python script.
#
# Dùng trong Linux / WSL / Docker entrypoint
# Usage:
#   chmod +x wait_and_register.sh
#   ./wait_and_register.sh
#   EDGEX_METADATA_HOST=192.168.1.10 ./wait_and_register.sh
# ============================================================

set -euo pipefail

EDGEX_HOST="${EDGEX_METADATA_HOST:-localhost}"
EDGEX_PORT="${EDGEX_METADATA_PORT:-59881}"
MAX_WAIT=120
INTERVAL=3

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "═══════════════════════════════════════════════════"
echo "  EdgeX Auto-Registration"
echo "  Target: http://${EDGEX_HOST}:${EDGEX_PORT}"
echo "═══════════════════════════════════════════════════"

# ─── Chờ EdgeX sẵn sàng ─────────────────────────────────
echo ""
echo "⏳ Chờ EdgeX core-metadata..."
elapsed=0
until curl -sf "http://${EDGEX_HOST}:${EDGEX_PORT}/api/v3/ping" > /dev/null 2>&1; do
    if [ "$elapsed" -ge "$MAX_WAIT" ]; then
        echo "❌ Timeout: EdgeX không phản hồi sau ${MAX_WAIT}s"
        exit 1
    fi
    sleep "$INTERVAL"
    elapsed=$((elapsed + INTERVAL))
    echo "   ... ${elapsed}s"
done
echo "✅ EdgeX core-metadata sẵn sàng!"

# ─── Chờ thêm để core-data và device-rest ổn định ──────
echo "⏳ Chờ thêm 5 giây để các services ổn định..."
sleep 5

# ─── Chạy Python registration script ────────────────────
echo ""
echo "📤 Bắt đầu đăng ký Profile và Device..."
python3 "${SCRIPT_DIR}/register_profile_and_device.py" \
    --host "${EDGEX_HOST}" \
    --port "${EDGEX_PORT}" \
    ${EDGEX_DEVICE_ADDRESS:+--device-address "$EDGEX_DEVICE_ADDRESS"} \
    ${EDGEX_DEVICE_PORT:+--device-port "$EDGEX_DEVICE_PORT"}

echo ""
echo "═══════════════════════════════════════════════════"
echo "  ✅ Hoàn tất! Bạn có thể bắt đầu gửi dữ liệu."
echo "═══════════════════════════════════════════════════"
