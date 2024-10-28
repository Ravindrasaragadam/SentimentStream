#!/bin/bash

# Start Docker Compose
echo "Starting Docker containers..."

docker-compose up --build --remove-orphans -d
