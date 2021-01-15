FROM bde2020/spark-master:3.0.1-hadoop3.2

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt