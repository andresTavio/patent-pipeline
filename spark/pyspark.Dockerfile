FROM bde2020/spark-python-template:3.0.1-hadoop3.2

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY db/postgresql-42.2.18.jar /spark/jars/postgresql-42.2.18.jar

ENV SPARK_POSTGRES_DRIVER_LOCATION=/spark/jars/postgresql-42.2.18.jar
ENV SPARK_APPLICATION_PYTHON_LOCATION=/app/scripts/entrypoint.py
