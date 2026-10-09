FROM apache/airflow:3.1.8

# Keep Airflow pinned while installing project dependencies, so pip cannot change its version.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==3.1.8" -r /tmp/requirements.txt
