# Image
FROM apache/airflow:2.6.2-python3.9

# Create working directory
WORKDIR /opt/airflow
COPY . ./
COPY ./config/kaggle.json /root/.kaggle/kaggle.json
COPY ./config/kaggle.json /home/airflow/.kaggle/kaggle.json

# Install dependencies
RUN pip install -r requirements.txt