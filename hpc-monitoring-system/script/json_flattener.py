import json
import urllib.request
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

# host.docker.internal reaches the EdgeX instance on the host from inside the container.
# Set the EDGEX_HOST environment variable if your host IP is different.
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
                    # There can be several disk devices; for simplicity just store
                    # the fields as-is (the last device wins).
                    metrics_to_send["disk_data"] = json.dumps(fields)
                elif plugin_name == "system":
                    metrics_to_send["sys_data"] = json.dumps(fields)
                elif plugin_name == "processes":
                    metrics_to_send["proc_data"] = json.dumps(fields)
                elif plugin_name == "nvidia_smi":
                    metrics_to_send["gpu_data"] = json.dumps(fields)
                elif plugin_name == "net":
                    if "net_data" in metrics_to_send:
                        existing = json.loads(metrics_to_send["net_data"])
                        existing.update(fields)
                        metrics_to_send["net_data"] = json.dumps(existing)
                    else:
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
                    print(f"Failed to send {res_name}: {e}", flush=True)

            if success > 0:
                print(f"Sent {success} metrics (E2E format) to EdgeX ({DEVICE_NAME})", flush=True)

            self.send_response(200)
            self.end_headers()
        except Exception as e:
            print(f"Error: {e}", flush=True)
            self.send_response(500)
            self.end_headers()

if __name__ == '__main__':
    server = HTTPServer(('0.0.0.0', 8080), TelegrafHandler)
    print(f"Bridge is running, forwarding to EdgeX at http://{EDGEX_HOST}:{EDGEX_PORT}...", flush=True)
    server.serve_forever()