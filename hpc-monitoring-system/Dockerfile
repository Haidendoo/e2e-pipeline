# NVIDIA-enabled base image (pick the Ubuntu version that matches your host)
FROM nvidia/cuda:12.2.0-base-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies, bcc-tools and Telegraf
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

# Set up the scripts directory
RUN mkdir -p /opt/telegraf-scripts

# Copy the config and scripts into the container
COPY /config/telegraf.conf /etc/telegraf/telegraf.conf
COPY /script/tcptop.py /opt/telegraf-scripts/tcptop.py

# Make the script executable
RUN chmod +x /opt/telegraf-scripts/tcptop.py

# Let telegraf run the python scripts with sudo without being asked for a password
RUN echo "telegraf ALL=(root) NOPASSWD: /usr/bin/python3 /opt/telegraf-scripts/*" > /etc/sudoers.d/telegraf_custom_scripts \
    && chmod 0440 /etc/sudoers.d/telegraf_custom_scripts

# Start Telegraf
CMD ["telegraf", "--config", "/etc/telegraf/telegraf.conf"]