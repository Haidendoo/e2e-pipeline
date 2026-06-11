FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir bytewax confluent-kafka influxdb-client requests

COPY bytewax /app/bytewax

WORKDIR /app/bytewax

CMD ["python", "-m", "bytewax.run", "dataflow:flow"]
