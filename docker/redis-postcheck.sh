#!/bin/bash

# Check cluster info and nodes using any node (using node-0 on port 6379)
echo "Checking cluster info..."
redis-cli -h localhost -p 6379  cluster info

echo "Checking cluster nodes..."
redis-cli -h localhost -p 6379  cluster nodes

# Function to add test keys with different hash tags
add_test_keys() {
  echo "Adding test keys..."
  # We can add keys through any node in the cluster
  for i in {1..100}; do
    redis-cli -h localhost -p 6379   set key:$i value:$i
  done

  # Add keys with different hash tags to see distribution
  for i in {1..20}; do
    redis-cli -h localhost -p 6379   set "{tag1}:key:$i" value:$i
    redis-cli -h localhost -p 6379   set "{tag2}:key:$i" value:$i
    redis-cli -h localhost -p 6379   set "{tag3}:key:$i" value:$i
  done
}

# Check key distribution across all master nodes
check_key_distribution() {
  echo "Checking key count on each master node..."

  # Master nodes are on these ports based on your docker-compose
  masters=(6382 6383 6384)

  for port in "${masters[@]}"; do
    echo "Keys on master port $port:"
    redis-cli -h localhost -p $port   dbsize

    # Sample some keys from each master
    echo "Sample keys on master port $port:"
    redis-cli -h localhost -p $port   --scan --count 5 | head -5
  done
}

# Check cluster slots distribution
check_slots_distribution() {
  echo "Checking slot distribution..."
  redis-cli -h localhost -p 6379   cluster slots
}

# Execute the functions
add_test_keys
check_key_distribution
check_slots_distribution