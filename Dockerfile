# Use the base Airflow image
FROM apache/airflow:2.10.2

# Copy requirements.txt into the container
COPY ./requirements.txt /requirements.txt

# Install the Python packages
RUN pip install --no-cache-dir -r /requirements.txt

# Copy your project files into the container
COPY ./dags /opt/airflow/dags
COPY ./configs /opt/airflow/configs
