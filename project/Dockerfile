# Image
FROM apache/airflow:2.6.2-python3.9

# Create working directory
WORKDIR /opt/airflow
COPY . ./
COPY ~/.kaggle/kaggle.json /opt/airflow/config

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt
