#!/bin/bash
set -e
# Start the cluster
docker-compose -f rabbit-cluster.yaml up -d

echo "Waiting for RabbitMQ to start..."
sleep 30

# Join rabbitmq2 to the cluster
docker exec rabbitmq2 rabbitmqctl stop_app
docker exec rabbitmq2 rabbitmqctl reset
docker exec rabbitmq2 rabbitmqctl join_cluster rabbit@rabbitmq1
docker exec rabbitmq2 rabbitmqctl start_app
echo "Node 2 joined the cluster"

# Join rabbitmq3 to the cluster
docker exec rabbitmq3 rabbitmqctl stop_app
docker exec rabbitmq3 rabbitmqctl reset
docker exec rabbitmq3 rabbitmqctl join_cluster rabbit@rabbitmq1
docker exec rabbitmq3 rabbitmqctl start_app
echo "Node 3 joined the cluster"

# Check cluster status
docker exec rabbitmq1 rabbitmqctl cluster_status

# Enable Quorum Queues feature flag (already enabled in RabbitMQ 3.11)
echo "Verifying Quorum Queues feature flag..."
docker exec rabbitmq1 rabbitmqctl list_feature_flags