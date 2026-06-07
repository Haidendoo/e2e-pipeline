#!/bin/bash

# Hàm thực hiện lệnh curl trong một vòng lặp
run_curl() {
  while true; do
    curl http://example.com  # Thay đổi URL theo nhu cầu của bạn
    sleep 1
  done
}

# Tạo 40 thread và chạy mỗi thread thực hiện lệnh curl
for i in {1..40}; do
  run_curl &  # Dấu "&" để chạy mỗi lệnh curl ở chế độ background
done

# Chờ tất cả các thread hoàn thành (trong trường hợp có giới hạn hoặc dừng script)
wait

