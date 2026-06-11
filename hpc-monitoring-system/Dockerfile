# Sử dụng base image có hỗ trợ NVIDIA (chọn version Ubuntu phù hợp với host của bạn)
FROM nvidia/cuda:12.2.0-base-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive

# Cài đặt các dependencies, bcc-tools và Telegraf
RUN apt-get update && apt-get install -y \
    wget \
    gawk \
    sudo \
    clang \
    llvm \
    gcc \
    bpfcc-tools \
    python3-bpfcc \
    && wget -q https://dl.influxdata.com/telegraf/releases/telegraf_1.27.1-1_amd64.deb \
    && dpkg -i telegraf_1.27.1-1_amd64.deb \
    && rm telegraf_1.27.1-1_amd64.deb \
    && rm -rf /var/lib/apt/lists/*

# Setup thư mục scripts
RUN mkdir -p /opt/telegraf-scripts

# Copy cấu hình và script vào container
COPY /config/telegraf.conf /etc/telegraf/telegraf.conf
COPY /script/tcptop.py /opt/telegraf-scripts/tcptop.py

# Cấp quyền thực thi
RUN chmod +x /opt/telegraf-scripts/tcptop.py

# Cho phép telegraf chạy script python dưới quyền sudo mà không cần mật khẩu
RUN echo "telegraf ALL=(root) NOPASSWD: /usr/bin/python3 /opt/telegraf-scripts/*" > /etc/sudoers.d/telegraf_custom_scripts \
    && chmod 0440 /etc/sudoers.d/telegraf_custom_scripts

# Lệnh khởi chạy Telegraf
CMD ["telegraf", "--config", "/etc/telegraf/telegraf.conf"]