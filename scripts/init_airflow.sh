#!/bin/bash

airflow db init

airflow users create \
    --username $AIRFLOW_USERNAME \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password $AIRFLOW_PASSWORD
exec airflow scheduler &

exec airflow webserver