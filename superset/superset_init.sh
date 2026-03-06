#!/bin/bash
# Superset initialization script
# This runs on first startup to create the admin user and initialize the database

echo "Initializing Superset..."

# Initialize the database
superset db upgrade

# Create admin user (skip if already exists)
superset fab create-admin \
  --username "${ADMIN_USERNAME:-admin}" \
  --password "${ADMIN_PASSWORD:-admin}" \
  --firstname "Admin" \
  --lastname "User" \
  --email "${ADMIN_EMAIL:-admin@localhost}" || true

# Initialize default roles and permissions
superset init

echo "Superset initialized! Starting server..."

# Start Superset
superset run -h 0.0.0.0 -p 8088 --with-threads
