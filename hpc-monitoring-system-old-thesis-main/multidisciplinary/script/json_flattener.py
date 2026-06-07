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
            
            # Map Telegraf plugins directly to Telegraf-Full-Node-Profile resources
            metrics_to_send = {}
            
            for entry in data_list:
                plugin_name = entry.get('name')
                fields = entry.get('fields', {})
                
                if not fields:
                    continue
                
                if plugin_name == "cpu":
                    metrics_to_send["cpu_data"] = json.dumps(fields)
                elif plugin_name == "mem":
                    metrics_to_send["mem_data"] = json.dumps(fields)
                elif plugin_name == "diskio":
                    # Co the co nhieu device disk, ta gop lai thanh 1 mang hoac lay cai cuoi
                    # De don gian lay nguyen mang neu co the hoac luu fields
                    metrics_to_send["disk_data"] = json.dumps(fields)
                elif plugin_name == "system":
                    metrics_to_send["sys_data"] = json.dumps(fields)
                elif plugin_name == "processes":
                    metrics_to_send["proc_data"] = json.dumps(fields)
                elif plugin_name == "nvidia_smi":
                    metrics_to_send["gpu_data"] = json.dumps(fields)
                elif plugin_name == "net":
                    metrics_to_send["net_data"] = json.dumps(fields)


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