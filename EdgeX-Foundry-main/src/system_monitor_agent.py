"""
System Monitor Agent — EdgeX Foundry
======================================
Thu thập đầy đủ 25 system metrics và gửi lên EdgeX device-rest
theo Device Profile: RPi-REST-Profile-v2

Metrics được thu thập:
  CPU:      Temperature, CPUUsage, CPUFreq, ContextSwitches,
            Interrupts, SoftInterrupts, Syscalls
  Memory:   MemUsed, MemFree, SwapIn, SwapOut
  Disk:     DiskRead, DiskWrite
  Network:  NetBytesSent, NetBytesRecv, NetPacketsSent, NetPacketsRecv,
            NetErrorsIn, NetErrorsOut
  System:   LoadAvg1, LoadAvg5, LoadAvg15, ProcessCount, ThreadCount, Uptime

Usage:
    pip install psutil requests
    python system_monitor_agent.py
    python system_monitor_agent.py --host 192.168.1.100 --interval 5
    python system_monitor_agent.py --test-one-shot   # gửi 1 lần rồi thoát

Environment variables:
    EDGEX_REST_HOST    — host EdgeX device-rest (default: localhost)
    EDGEX_REST_PORT    — port EdgeX device-rest (default: 59986)
    EDGEX_DEVICE_NAME  — tên device đã đăng ký (default: RPi4-REST-v2)
    AGENT_INTERVAL     — interval giây giữa các lần gửi (default: 5)
"""

from __future__ import annotations

import argparse
import logging
import os
import platform
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

import psutil
import requests

# ─── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Config ─────────────────────────────────────────────────────────────────────
DEFAULT_HOST = os.getenv("EDGEX_REST_HOST", "localhost")
DEFAULT_PORT = int(os.getenv("EDGEX_REST_PORT", "59986"))
DEFAULT_DEVICE = os.getenv("EDGEX_DEVICE_NAME", "RPi4-REST-v2")
DEFAULT_INTERVAL = int(os.getenv("AGENT_INTERVAL", "5"))

IS_LINUX = platform.system() == "Linux"
IS_RASPBERRY = IS_LINUX and os.path.exists("/sys/class/thermal/thermal_zone0/temp")


# ─── Data Collection ─────────────────────────────────────────────────────────────

@dataclass
class SystemMetrics:
    """Container cho tất cả metrics được thu thập."""
    # CPU
    Temperature: float = 0.0
    CPUUsage: float = 0.0
    CPUFreq: float = 0.0
    ContextSwitches: int = 0
    Interrupts: int = 0
    SoftInterrupts: int = 0
    Syscalls: int = 0
    # Memory
    MemUsed: int = 0
    MemFree: int = 0
    SwapIn: int = 0
    SwapOut: int = 0
    # Disk
    DiskRead: int = 0
    DiskWrite: int = 0
    # Network
    NetBytesSent: int = 0
    NetBytesRecv: int = 0
    NetPacketsSent: int = 0
    NetPacketsRecv: int = 0
    NetErrorsIn: int = 0
    NetErrorsOut: int = 0
    # System
    LoadAvg1: float = 0.0
    LoadAvg5: float = 0.0
    LoadAvg15: float = 0.0
    ProcessCount: int = 0
    ThreadCount: int = 0
    Uptime: int = 0

    def to_edgex_readings(self) -> dict[str, str]:
        """Chuyển metrics thành dict {resourceName: value_string} cho EdgeX."""
        return {
            "Temperature": str(self.Temperature),
            "CPUUsage": str(self.CPUUsage),
            "CPUFreq": str(self.CPUFreq),
            "ContextSwitches": str(self.ContextSwitches),
            "Interrupts": str(self.Interrupts),
            "SoftInterrupts": str(self.SoftInterrupts),
            "Syscalls": str(self.Syscalls),
            "MemUsed": str(self.MemUsed),
            "MemFree": str(self.MemFree),
            "SwapIn": str(self.SwapIn),
            "SwapOut": str(self.SwapOut),
            "DiskRead": str(self.DiskRead),
            "DiskWrite": str(self.DiskWrite),
            "NetBytesSent": str(self.NetBytesSent),
            "NetBytesRecv": str(self.NetBytesRecv),
            "NetPacketsSent": str(self.NetPacketsSent),
            "NetPacketsRecv": str(self.NetPacketsRecv),
            "NetErrorsIn": str(self.NetErrorsIn),
            "NetErrorsOut": str(self.NetErrorsOut),
            "LoadAvg1": str(self.LoadAvg1),
            "LoadAvg5": str(self.LoadAvg5),
            "LoadAvg15": str(self.LoadAvg15),
            "ProcessCount": str(self.ProcessCount),
            "ThreadCount": str(self.ThreadCount),
            "Uptime": str(self.Uptime),
        }


def read_cpu_temperature() -> float:
    """Đọc nhiệt độ CPU. Hỗ trợ Linux thermal zone và psutil sensors."""
    # Raspberry Pi / Linux
    if IS_RASPBERRY:
        try:
            with open("/sys/class/thermal/thermal_zone0/temp") as f:
                return round(float(f.read().strip()) / 1000.0, 2)
        except (OSError, ValueError):
            pass

    # psutil sensors (Linux với hwmon)
    if IS_LINUX:
        try:
            temps = psutil.sensors_temperatures()
            for sensor_name in ("coretemp", "cpu_thermal", "k10temp", "zenpower"):
                if sensor_name in temps:
                    entries = temps[sensor_name]
                    if entries:
                        return round(entries[0].current, 2)
            # Thử sensor đầu tiên có sẵn
            for entries in temps.values():
                if entries:
                    return round(entries[0].current, 2)
        except (AttributeError, Exception):
            pass

    # Windows — trả về 0 (không có thermal API trực tiếp)
    return 0.0


def read_cpu_freq() -> float:
    """Đọc tần số CPU hiện tại (MHz)."""
    try:
        freq = psutil.cpu_freq()
        if freq:
            return round(freq.current, 2)
    except Exception:
        pass
    return 0.0


def collect_metrics() -> SystemMetrics:
    """Thu thập tất cả system metrics."""
    m = SystemMetrics()

    # ── CPU ─────────────────────────────────────────────
    m.Temperature = read_cpu_temperature()
    m.CPUUsage = round(psutil.cpu_percent(interval=1), 2)
    m.CPUFreq = read_cpu_freq()

    try:
        cpu_stats = psutil.cpu_stats()
        m.ContextSwitches = cpu_stats.ctx_switches
        m.Interrupts = cpu_stats.interrupts
        m.SoftInterrupts = cpu_stats.soft_interrupts
        m.Syscalls = getattr(cpu_stats, "syscalls", 0)
    except Exception:
        pass

    # ── Memory ──────────────────────────────────────────
    try:
        vm = psutil.virtual_memory()
        m.MemUsed = vm.used
        m.MemFree = vm.available

        swap = psutil.swap_memory()
        m.SwapIn = swap.sin
        m.SwapOut = swap.sout
    except Exception:
        pass

    # ── Disk I/O ────────────────────────────────────────
    try:
        disk_io = psutil.disk_io_counters()
        if disk_io:
            m.DiskRead = disk_io.read_bytes
            m.DiskWrite = disk_io.write_bytes
    except Exception:
        pass

    # ── Network ─────────────────────────────────────────
    try:
        net_io = psutil.net_io_counters()
        if net_io:
            m.NetBytesSent = net_io.bytes_sent
            m.NetBytesRecv = net_io.bytes_recv
            m.NetPacketsSent = net_io.packets_sent
            m.NetPacketsRecv = net_io.packets_recv
            m.NetErrorsIn = net_io.errin
            m.NetErrorsOut = net_io.errout
    except Exception:
        pass

    # ── System ──────────────────────────────────────────
    try:
        load = psutil.getloadavg()
        m.LoadAvg1 = round(load[0], 3)
        m.LoadAvg5 = round(load[1], 3)
        m.LoadAvg15 = round(load[2], 3)
    except AttributeError:
        # Windows không có getloadavg — dùng CPU percent làm xấp xỉ
        cpu_p = psutil.cpu_percent(interval=None) / 100.0
        m.LoadAvg1 = round(cpu_p, 3)
        m.LoadAvg5 = round(cpu_p, 3)
        m.LoadAvg15 = round(cpu_p, 3)

    try:
        procs = list(psutil.process_iter())
        m.ProcessCount = len(procs)
        m.ThreadCount = sum(getattr(p, "_num_threads", 0) or 0 for p in procs)
        if m.ThreadCount == 0:
            m.ThreadCount = sum(
                p.num_threads() for p in procs
                if not psutil.NoSuchProcess and p.is_running()
            )
    except Exception:
        pass

    try:
        m.Uptime = int(time.time() - psutil.boot_time())
    except Exception:
        pass

    return m


# ─── EdgeX Sender ────────────────────────────────────────────────────────────────

class EdgeXSender:
    """Gửi metrics lên EdgeX device-rest API."""

    def __init__(self, host: str, port: int, device_name: str):
        self.host = host
        self.port = port
        self.device_name = device_name
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        self._base_url = f"http://{host}:{port}/api/v3/resource/{device_name}"
        self._send_count = 0
        self._error_count = 0

    def _send_resource(self, resource_name: str, value: str) -> bool:
        """Gửi một resource reading lên EdgeX."""
        url = f"{self._base_url}/{resource_name}"
        payload = value
        try:
            resp = self.session.post(url, data=payload, timeout=5)
            if resp.status_code in (200, 201):
                return True
            else:
                log.debug("⚠️  %s → %d: %s", resource_name, resp.status_code, resp.text[:100])
                return False
        except requests.exceptions.RequestException as e:
            log.debug("❌ Lỗi gửi %s: %s", resource_name, e)
            return False

    def send_metrics(self, metrics: SystemMetrics) -> tuple[int, int]:
        """Gửi tất cả readings. Returns (success_count, error_count)."""
        readings = metrics.to_edgex_readings()
        success = 0
        errors = 0
        for resource_name, value in readings.items():
            if self._send_resource(resource_name, value):
                success += 1
            else:
                errors += 1
        self._send_count += success
        self._error_count += errors
        return success, errors

    def ping(self) -> bool:
        """Kiểm tra EdgeX device-rest có sẵn sàng không."""
        try:
            resp = self.session.get(
                f"http://{self.host}:{self.port}/api/v3/ping",
                timeout=5,
            )
            return resp.status_code == 200
        except requests.exceptions.RequestException:
            return False


# ─── Main Loop ───────────────────────────────────────────────────────────────────

def run_agent(host: str, port: int, device_name: str, interval: int, one_shot: bool = False) -> None:
    """Vòng lặp chính của agent."""
    sender = EdgeXSender(host, port, device_name)

    log.info("═══════════════════════════════════════════════════════")
    log.info("  System Monitor Agent — EdgeX Foundry")
    log.info("  Device: %s", device_name)
    log.info("  Target: http://%s:%d", host, port)
    log.info("  Interval: %ds | Platform: %s", interval, platform.system())
    log.info("═══════════════════════════════════════════════════════")

    # Kiểm tra kết nối
    log.info("🔍 Kiểm tra kết nối EdgeX...")
    if not sender.ping():
        log.warning(
            "⚠️  EdgeX device-rest không phản hồi tại http://%s:%d/api/v3/ping\n"
            "   Hãy đảm bảo EdgeX đang chạy và device '%s' đã được đăng ký.\n"
            "   Tiếp tục gửi dữ liệu (sẽ retry mỗi lần loop)...",
            host, port, device_name,
        )
    else:
        log.info("✅ EdgeX sẵn sàng!")

    log.info("🚀 Bắt đầu thu thập và gửi dữ liệu...\n")

    iteration = 0
    while True:
        iteration += 1
        try:
            # Thu thập metrics
            metrics = collect_metrics()
            log.info(
                "📊 [%d] CPU: %.1f%% | %.1f°C | Mem: %s MB used | "
                "Net: ↑%s ↓%s bytes",
                iteration,
                metrics.CPUUsage,
                metrics.Temperature,
                metrics.MemUsed // (1024 * 1024),
                metrics.NetBytesSent,
                metrics.NetBytesRecv,
            )

            # Gửi lên EdgeX
            success, errors = sender.send_metrics(metrics)
            log.info(
                "📤 Gửi: %d/%d readings thành công | Tổng lỗi: %d",
                success,
                success + errors,
                sender._error_count,
            )

        except KeyboardInterrupt:
            log.info("\n⛔ Dừng agent theo yêu cầu.")
            log.info(
                "📈 Tổng kết: %d readings gửi thành công, %d lỗi",
                sender._send_count,
                sender._error_count,
            )
            break
        except Exception as e:
            log.error("❌ Lỗi không mong đợi: %s", e, exc_info=True)

        if one_shot:
            log.info("✅ One-shot mode: hoàn tất.")
            break

        time.sleep(interval)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="System Monitor Agent — gửi metrics lên EdgeX Foundry"
    )
    parser.add_argument("--host", default=DEFAULT_HOST, help="EdgeX device-rest host")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="EdgeX device-rest port")
    parser.add_argument("--device", default=DEFAULT_DEVICE, help="Tên device trong EdgeX")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL, help="Interval (giây)")
    parser.add_argument("--test-one-shot", action="store_true", help="Gửi 1 lần rồi thoát")
    args = parser.parse_args()

    run_agent(
        host=args.host,
        port=args.port,
        device_name=args.device,
        interval=args.interval,
        one_shot=args.test_one_shot,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
