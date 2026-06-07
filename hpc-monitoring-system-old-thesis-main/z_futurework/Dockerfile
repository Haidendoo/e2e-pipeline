# ==========================================
# STAGE 1: Môi trường biên dịch (Builder)
# ==========================================
FROM nvidia/cuda:12.2.2-devel-ubuntu22.04 AS builder

# Thêm PPA và cài đặt g++-14 cùng các công cụ cần thiết
RUN apt-get update && apt-get install -y software-properties-common git make && \
    add-apt-repository ppa:ubuntu-toolchain-r/test -y && \
    apt-get update && apt-get install -y g++-14 && \
    update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-14 100

# Clone mã nguồn
WORKDIR /build
RUN git clone https://github.com/RWTH-HPC/NVML-GPM-Collector.git

# Đổi cấu hình link tĩnh thành link động và tiến hành build
WORKDIR /build/NVML-GPM-Collector
RUN sed -i 's/-l:libnvidia-ml.a/-lnvidia-ml/g' Makefile && \
    CPLUS_INCLUDE_PATH=/usr/local/cuda/include \
    LIBRARY_PATH=/usr/local/cuda/lib64/stubs \
    make

# ==========================================
# STAGE 2: Môi trường chạy thực tế (Runtime)
# ==========================================
FROM ubuntu:22.04

# Cài đặt Telegraf từ repository chính thức của InfluxData bằng key mới
RUN apt-get update && apt-get install -y wget gnupg2 ca-certificates && \
    wget -qO- https://repos.influxdata.com/influxdata-archive.key | gpg --dearmor | tee /etc/apt/trusted.gpg.d/influxdata-archive.gpg > /dev/null && \
    echo 'deb [signed-by=/etc/apt/trusted.gpg.d/influxdata-archive.gpg] https://repos.influxdata.com/debian stable main' | tee /etc/apt/sources.list.d/influxdata.list && \
    apt-get update && apt-get install -y telegraf && \
    rm -rf /var/lib/apt/lists/*

# Copy công cụ thu thập GPU đã được build từ Stage 1 sang (đã cập nhật tên file thành main.exe)
COPY --from=builder /build/NVML-GPM-Collector/main.exe /usr/local/bin/NVML-GPM-Collector

# Cấp quyền thực thi cho tool
RUN chmod +x /usr/local/bin/NVML-GPM-Collector

# Mặc định khởi chạy Telegraf
CMD ["telegraf", "--config", "/etc/telegraf/telegraf.conf"]