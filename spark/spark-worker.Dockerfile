FROM bde2020/spark-worker:3.0.1-hadoop3.2

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt