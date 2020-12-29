#!/usr/bin/env bash
POSTGRES_DB="airflow"
AIRFLOW_USER_NAME="admin"
AIRFLOW_FIRST_NAME=AIRFLOW_USER_NAME
AIRFLOW_LAST_NAME=AIRFLOW_USER_NAME

# if psql -lqt | cut -d \| -f 1 | grep -qw $POSTGRES_DB; then
#     echo "Database already initialized"
#     else
#         airflow db init
# fi

airflow db upgrade

if airflow users list | grep -q $AIRFLOW_USER_NAME; then
    echo "User already created"
    else
        airflow users create -e admin@example.org -f $AIRFLOW_FIRST_NAME -l $AIRFLOW_LAST_NAME -p admin -r Admin -u $AIRFLOW_USER_NAME
fi

airflow webserver
