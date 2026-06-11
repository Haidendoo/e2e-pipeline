"""
EdgeX Foundry — Auto-Registration Script
=========================================
Tự động đăng ký Device Profile và Device vào EdgeX core-metadata.
Hỗ trợ cả chế độ wait-until-ready để dùng trong Docker init container.

Usage:
    python register_profile_and_device.py
    python register_profile_and_device.py --host 192.168.1.10 --port 59881 --wait

Environment variables:
    EDGEX_METADATA_HOST  — hostname/IP của EdgeX core-metadata (default: localhost)
    EDGEX_METADATA_PORT  — port của EdgeX core-metadata (default: 59881)
    EDGEX_DEVICE_ADDRESS — địa chỉ IP của thiết bị biên (default: 192.168.1.6)
    EDGEX_DEVICE_PORT    — port REST của thiết bị (default: 59986)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import logging
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Config ────────────────────────────────────────────────────────────────────

DEFAULT_HOST = os.getenv("EDGEX_METADATA_HOST", "localhost")
DEFAULT_PORT = int(os.getenv("EDGEX_METADATA_PORT", "59881"))
DEVICE_ADDRESS = os.getenv("EDGEX_DEVICE_ADDRESS", "192.168.1.6")
DEVICE_PORT = int(os.getenv("EDGEX_DEVICE_PORT", "59986"))

# Đường dẫn config (relative to script location)
SCRIPT_DIR = Path(__file__).parent
CONFIG_DIR = SCRIPT_DIR.parent / "config"

PROFILE_FILE = CONFIG_DIR / "rpi-rest-profile-v2.json"
DEVICE_FILE = CONFIG_DIR / "rpi-rest-device-v2.json"

# ─── API Helpers ────────────────────────────────────────────────────────────────

def base_url(host: str, port: int) -> str:
    return f"http://{host}:{port}/api/v3"


def wait_for_edgex(host: str, port: int, timeout: int = 120) -> bool:
    """Chờ EdgeX core-metadata sẵn sàng (health check)."""
    url = f"http://{host}:{port}/api/v3/ping"
    log.info("⏳ Chờ EdgeX core-metadata tại %s:%s ...", host, port)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = requests.get(url, timeout=3)
            if resp.status_code == 200:
                log.info("✅ EdgeX core-metadata sẵn sàng!")
                return True
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(3)
    log.error("❌ Timeout: EdgeX core-metadata không phản hồi sau %ds", timeout)
    return False


def register_device_profile(host: str, port: int) -> bool:
    """Đăng ký Device Profile RPi-REST-Profile-v2."""
    url = f"{base_url(host, port)}/deviceprofile"

    # Đọc profile từ file
    if not PROFILE_FILE.exists():
        log.error("❌ Không tìm thấy profile file: %s", PROFILE_FILE)
        return False

    with open(PROFILE_FILE, encoding="utf-8") as f:
        profiles = json.load(f)

    # Profile file là array, lấy phần tử đầu tiên
    profile_payload = profiles[0] if isinstance(profiles, list) else profiles
    profile_name = profile_payload["profile"]["name"]

    # Kiểm tra xem profile đã tồn tại chưa
    check_url = f"{base_url(host, port)}/deviceprofile/name/{profile_name}"
    try:
        resp = requests.get(check_url, timeout=5)
        if resp.status_code == 200:
            log.info("ℹ️  Profile '%s' đã tồn tại, bỏ qua.", profile_name)
            return True
    except requests.exceptions.RequestException:
        pass

    # POST profile
    log.info("📤 Đang đăng ký Device Profile: %s ...", profile_name)
    try:
        resp = requests.post(url, json=[profile_payload], timeout=10)
        if resp.status_code in (200, 201, 207):
            log.info("✅ Device Profile '%s' đã được đăng ký.", profile_name)
            return True
        elif resp.status_code == 409:
            log.info("ℹ️  Profile '%s' đã tồn tại (409 Conflict).", profile_name)
            return True
        else:
            log.error(
                "❌ Không thể đăng ký profile. Status: %d | Body: %s",
                resp.status_code,
                resp.text[:500],
            )
            return False
    except requests.exceptions.RequestException as e:
        log.error("❌ Lỗi kết nối khi đăng ký profile: %s", e)
        return False


def register_device(host: str, port: int) -> bool:
    """Đăng ký Device RPi4-REST-v2."""
    url = f"{base_url(host, port)}/device"

    if not DEVICE_FILE.exists():
        log.error("❌ Không tìm thấy device file: %s", DEVICE_FILE)
        return False

    with open(DEVICE_FILE, encoding="utf-8") as f:
        devices = json.load(f)

    device_payload = devices[0] if isinstance(devices, list) else devices
    device_name = device_payload["device"]["name"]

    # Override địa chỉ từ environment variable
    device_payload["device"]["protocols"]["rest"]["Address"] = DEVICE_ADDRESS
    device_payload["device"]["protocols"]["rest"]["Port"] = str(DEVICE_PORT)

    # Kiểm tra device đã tồn tại chưa
    check_url = f"{base_url(host, port)}/device/name/{device_name}"
    try:
        resp = requests.get(check_url, timeout=5)
        if resp.status_code == 200:
            log.info("ℹ️  Device '%s' đã tồn tại, bỏ qua.", device_name)
            return True
    except requests.exceptions.RequestException:
        pass

    # POST device
    log.info("📤 Đang đăng ký Device: %s ...", device_name)
    try:
        resp = requests.post(url, json=[device_payload], timeout=10)
        if resp.status_code in (200, 201, 207):
            log.info("✅ Device '%s' đã được đăng ký.", device_name)
            return True
        elif resp.status_code == 409:
            log.info("ℹ️  Device '%s' đã tồn tại (409 Conflict).", device_name)
            return True
        else:
            log.error(
                "❌ Không thể đăng ký device. Status: %d | Body: %s",
                resp.status_code,
                resp.text[:500],
            )
            return False
    except requests.exceptions.RequestException as e:
        log.error("❌ Lỗi kết nối khi đăng ký device: %s", e)
        return False


def verify_registration(host: str, port: int) -> None:
    """In ra danh sách devices và profiles đã đăng ký để xác minh."""
    log.info("\n─── Xác minh đăng ký ──────────────────────")
    try:
        resp = requests.get(f"{base_url(host, port)}/device/all?limit=20", timeout=5)
        if resp.status_code == 200:
            devices = resp.json().get("devices", [])
            log.info("📋 Devices đang có (%d):", len(devices))
            for d in devices:
                log.info("   • %s [profile: %s] [state: %s]",
                         d["name"], d["profileName"], d["adminState"])
    except requests.exceptions.RequestException as e:
        log.warning("⚠️  Không thể lấy danh sách devices: %s", e)

    try:
        resp = requests.get(f"{base_url(host, port)}/deviceprofile/all?limit=20", timeout=5)
        if resp.status_code == 200:
            profiles = resp.json().get("profiles", [])
            log.info("📋 Profiles đang có (%d):", len(profiles))
            for p in profiles:
                resources = len(p.get("deviceResources", []))
                log.info("   • %s [%d resources]", p["name"], resources)
    except requests.exceptions.RequestException as e:
        log.warning("⚠️  Không thể lấy danh sách profiles: %s", e)


# ─── Main ───────────────────────────────────────────────────────────────────────

def main() -> int:
    global DEVICE_ADDRESS, DEVICE_PORT
    parser = argparse.ArgumentParser(description="EdgeX auto-registration script")
    parser.add_argument("--host", default=DEFAULT_HOST, help="EdgeX core-metadata host")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="EdgeX core-metadata port")
    parser.add_argument("--wait", action="store_true", help="Chờ EdgeX sẵn sàng trước khi đăng ký")
    parser.add_argument("--timeout", type=int, default=120, help="Timeout (giây) khi wait mode")
    parser.add_argument("--device-address", default=DEVICE_ADDRESS, help="Địa chỉ IP thiết bị biên")
    parser.add_argument("--device-port", type=int, default=DEVICE_PORT, help="REST port của thiết bị")
    args = parser.parse_args()

    # Override device address từ args
    DEVICE_ADDRESS = args.device_address
    DEVICE_PORT = args.device_port

    log.info("🚀 EdgeX Auto-Registration Tool")
    log.info("   Target: http://%s:%d", args.host, args.port)
    log.info("   Device Address: %s:%d", DEVICE_ADDRESS, DEVICE_PORT)

    if args.wait:
        if not wait_for_edgex(args.host, args.port, args.timeout):
            return 1
    
    # Đăng ký profile
    if not register_device_profile(args.host, args.port):
        log.error("❌ Thất bại khi đăng ký Device Profile")
        return 1

    # Đăng ký device  
    if not register_device(args.host, args.port):
        log.error("❌ Thất bại khi đăng ký Device")
        return 1

    # Xác minh
    verify_registration(args.host, args.port)
    log.info("\n✅ Đăng ký thành công! EdgeX sẵn sàng nhận dữ liệu.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
