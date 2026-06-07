import json
import urllib.request
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

# Dùng host.docker.internal để kết nối ra EdgeX chạy trên máy Host từ bên trong Container.
# Bạn cũng có thể truyền biến môi trường EDGEX_HOST nếu IP của máy bạn khác.
EDGEX_HOST = os.getenv("EDGEX_HOST", "host.docker.internal")
EDGEX_PORT = os.getenv("EDGEX_PORT", "59986")
DEVICE_NAME = os.getenv("DEVICE_NAME", "RPi4-REST-v2")
EDGEX_BASE_URL = f"http://{EDGEX_HOST}:{EDGEX_PORT}/api/v3/resource/{DEVICE_NAME}"

class TelegrafHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(length)
        
        try:
            raw_text = post_data.decode('utf-8').strip()
            parsed_json = json.loads(raw_text)
            data_list = parsed_json.get('metrics', []) if isinstance(parsed_json, dict) else []
            
            # Map Telegraf fields to EdgeX RPi-REST-Profile-v2 standard fields
            metrics_to_send = {}
            
            disk_read_sum = 0
            disk_write_sum = 0
            
            for entry in data_list:
                plugin_name = entry.get('name')
                fields = entry.get('fields', {})
                
                if plugin_name == "cpu":
                    if "usage_idle" in fields:
                        metrics_to_send["CPUUsage"] = str(round(100.0 - float(fields["usage_idle"]), 2))
                
                elif plugin_name == "mem":
                    if "used" in fields:
                        metrics_to_send["MemUsed"] = str(fields["used"])
                    if "available" in fields:
                        metrics_to_send["MemFree"] = str(fields["available"])
                        
                elif plugin_name == "diskio":
                    if "read_bytes" in fields:
                        disk_read_sum += int(fields["read_bytes"])
                    if "write_bytes" in fields:
                        disk_write_sum += int(fields["write_bytes"])
                        
                elif plugin_name == "system":
                    if "load1" in fields:
                        metrics_to_send["LoadAvg1"] = str(fields["load1"])
                        metrics_to_send["LoadAvg5"] = str(fields.get("load5", 0))
                        metrics_to_send["LoadAvg15"] = str(fields.get("load15", 0))
                        metrics_to_send["Uptime"] = str(fields.get("uptime", 0))
                        
                elif plugin_name == "processes":
                    if "proc_total" in fields:
                        metrics_to_send["ProcessCount"] = str(fields["proc_total"])
                        metrics_to_send["ThreadCount"] = str(fields.get("total_threads", 0))

            if disk_read_sum > 0:
                metrics_to_send["DiskRead"] = str(disk_read_sum)
            if disk_write_sum > 0:
                metrics_to_send["DiskWrite"] = str(disk_write_sum)

            # Send metrics
            success = 0
            for res_name, value in metrics_to_send.items():
                val_str = str(value)
                url = f"{EDGEX_BASE_URL}/{res_name}"
                req = urllib.request.Request(url, method='POST')
                req.add_header('Content-Type', 'text/plain')
                try:
                    with urllib.request.urlopen(req, data=val_str.encode('utf-8')) as f:
                        success += 1
                except Exception as e:
                    print(f"Lỗi gửi {res_name}: {e}", flush=True)

            if success > 0:
                print(f"Đã gửi {success} thông số (chuẩn E2E) lên EdgeX ({DEVICE_NAME})", flush=True)

            self.send_response(200)
            self.end_headers()
        except Exception as e:
            print(f"Lỗi: {e}", flush=True)
            self.send_response(500)
            self.end_headers()

if __name__ == '__main__':
    server = HTTPServer(('0.0.0.0', 8080), TelegrafHandler)
    print(f"Bridge đang chạy và sẽ trỏ tới EdgeX tại http://{EDGEX_HOST}:{EDGEX_PORT}...", flush=True)
    server.serve_forever()