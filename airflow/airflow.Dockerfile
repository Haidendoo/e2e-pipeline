FROM apache/airflow:3.1.6

USER root

# Install OpenJDK (JRE) and build dependencies for C/C++ packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    build-essential \
    librdkafka-dev \
    libffi-dev \
    libssl-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Correct way to set Environment Variables in a Dockerfile
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# You don't need to 'export' it; ENV handles making it available to all processes
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Copy and install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt