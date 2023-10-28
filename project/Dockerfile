# Image
FROM puckel/docker-airflow:1.10.6

# Create working directory
WORKDIR /app

# Install requirements
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY dags/ /dags
COPY data/ /data
COPY config/ /config
COPY scripts/ /scripts

# Set Apache Airflow environment variables
ENV AIRFLOW_HOME=/usr/local/airflow

ENTRYPOINT ["python", "/dags/etl_pipeline.py"]
