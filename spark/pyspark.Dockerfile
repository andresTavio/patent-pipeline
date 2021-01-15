FROM bde2020/spark-python-template:3.0.1-hadoop3.2

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

ENV SPARK_APPLICATION_PYTHON_LOCATION=/app/entrypoint.py
