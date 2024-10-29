# Use the base Airflow image
FROM apache/airflow:2.10.2

# Copy requirements.txt into the container
COPY ./requirements.txt /requirements.txt

# Copy your project files into the container
COPY ./dags /opt/airflow/dags
COPY ./configs /opt/airflow/configs
# Copy the entrypoint script
COPY entrypoint.sh /entrypoint.sh

# Make the script executable
RUN chmod +x /entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
EXPOSE 8080

CMD ["airflow", "webserver", "-p", "8080"]