echo "Creating custom telegraf.conf..."
sudo tee telegraf.conf > /dev/null <<EOF
[agent]
  interval = "5s" # Lấy mẫu mỗi 5 giây
  flush_interval = "5s"

[[inputs.execd]]
  command = ["/usr/local/bin/NVML-GPM-Collector"]
  signal = "SIGTERM"
  data_format = "influx"

[[outputs.http]]
  ## URL endpoint của EdgeX Device REST service
  ## Cấu trúc chuẩn của EdgeX v3: http://<IP>:59986/api/v3/resource/<Tên_Device>/<Tên_Resource>
  url = "http://<IP_CỦA_EDGEX>:59986/api/v3/resource/gpu-monitor-device/gpm-metrics"
  
  ## Dùng phương thức POST để gửi dữ liệu
  method = "POST"
  
  ## Ép kiểu dữ liệu sang JSON để EdgeX dễ đọc
  data_format = "json"
  
  ## (Tùy chọn) Thêm header nếu EdgeX yêu cầu
  [outputs.http.headers]
    Content-Type = "application/json"
EOF