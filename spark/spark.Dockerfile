FROM apache/spark:3.4.1

USER root

# Install build dependencies for Python packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    librdkafka-dev \
    libffi-dev \
    libssl-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 1. Setup Environment for Jars
ENV SPARK_HOME=/opt/spark
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262

# 2. Add S3A and Kafka Connectors (The "Secret Sauce" for Minio & Kafka)
# We add these to /opt/spark/jars so they are available to every session automatically
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar ${SPARK_HOME}/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar ${SPARK_HOME}/jars/
# Kafka Connector for Structured Streaming
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.1/spark-sql-kafka-0-10_2.12-3.4.1.jar ${SPARK_HOME}/jars/
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar ${SPARK_HOME}/jars/

# 3. Install Python dependencies
COPY requirements.txt /tmp/spark_requirements.txt
RUN pip install --no-cache-dir -r /tmp/spark_requirements.txt

# Return to spark user for security
USER spark
WORKDIR /opt/spark