# Use the base Airflow image
FROM apache/airflow:2.10.2

USER root

# Install Playwright dependencies
RUN apt-get update && apt-get install -y \
    libnss3 \
    libgconf-2-4 \
    libxss1 \
    libx11-xcb1 \
    libxcb1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Playwright and its dependencies
RUN playwright install

# Set the browser path environment variable
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright-browsers
ENV CONFIG_PATH=/opt/airflow/configs/

USER airflow

# Copy requirements.txt into the container
COPY ./requirements.txt /requirements.txt

# Install the Python packages, including Playwright
RUN pip install --no-cache-dir -r /requirements.txt

# Copy your project files into the container
COPY ./dags /opt/airflow/dags
COPY ./configs /opt/airflow/configs
