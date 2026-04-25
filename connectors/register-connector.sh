#!/bin/bash

echo "Waiting for Kafka Connect..."

until curl -s http://localhost:8083/connectors > /dev/null; do
  sleep 5
  echo "   Not ready yet... retrying"
done

echo "Kafka Connect is ready!"

# Delete old connector if exists
curl -s -X DELETE http://localhost:8083/connectors/postgres-connector
sleep 2

# Register the connector
echo "Registering connector..."
curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @/usr/share/confluent-hub-components/custom-connectors/postgres-source.json

# Show status
sleep 5
echo "Connector status:"
curl -s http://localhost:8083/connectors/postgres-connector/status