# Project Remarkable Trees

This projects presents a simple ETL (Extract, Transform and Load) pipeline built using Apache Airflow.

🏗️ Construction in progress...

**Dataset**

<u>URL:</u> https://www.kaggle.com/datasets/volpatto/mpwolke/cusersmarildownloadstreescsv

Dataset is internally downloaded using the Kaggle API (need to be registered).

**How to use it**
1. For enabling Kaggle API use, credentials defined by Kaggle's username and API key need to be set. For that purpose, copy your <code>kaggle.json</code> file into the <code>config</code> directory. \
<u>Command</u>: <code>cp ~/.kaggle/kaggle.json config/</code>
2. Run the database migration and create the first user <code>admin</code> with the related password <code>1234</code>. \
<u>Command</u>: <code>docker-compose up airflow-init</code>
3. Then, run all services. \
<u>Command</u>: <code>docker-compose up</code>

**Tools**
- Python
- PostgreSQL
- Apache Airflow
- Docker
