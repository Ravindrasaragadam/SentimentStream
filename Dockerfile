# Use the base Airflow image
FROM apache/airflow:2.10.2
COPY setup.sh /
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/setup.sh"]

# Copy requirements.txt into the container
COPY ./requirements.txt /requirements.txt

# Copy your project files into the container
COPY ./dags /opt/airflow/dags
COPY ./configs /opt/airflow/configs
