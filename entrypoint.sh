#!/usr/bin/env bash
airflow initdb
airflow db init
airflow webserver -D
airflow scheduler -D