#!/bin/bash

airflow db init

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin || true

exec airflow scheduler &

exec airflow webserver