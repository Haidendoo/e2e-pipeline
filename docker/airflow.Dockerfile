FROM apache/airflow:3.1.6

# Copy requirements.txt
COPY requirements.txt /requirements.txt

# Install all dependencies from requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt


