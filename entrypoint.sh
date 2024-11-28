#!/bin/bash

airflow db init

airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@airflow.com \
    --password admin

airflow webserver