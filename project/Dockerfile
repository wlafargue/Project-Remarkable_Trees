FROM python:3.9

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY dags/ /dags
COPY data/ /data
COPY config/ /config
COPY scripts/ /scripts

ENTRYPOINT ["python", "/dags/etl_pipeline.py"]
