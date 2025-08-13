#!/bin/bash

# Databricks Cluster Initialization Script
# This script installs required dependencies for the data processing pipeline
# It runs on cluster startup to ensure all nodes have the necessary packages

set -e

echo "Starting data processing pipeline dependency installation..."

# Log file for debugging
LOG_FILE="/tmp/init_script.log"
exec > >(tee -a $LOG_FILE)
exec 2>&1

echo "$(date): Starting dependency installation"

# Update package manager
echo "$(date): Updating package manager..."
sudo apt-get update -y

# Install system dependencies if needed
echo "$(date): Installing system dependencies..."
# Add any system-level dependencies here if required

# Install Python packages
echo "$(date): Installing Python packages..."

# Install Redis client with SSL support
pip install redis>=4.5.0

# Install additional dependencies that might not be in Databricks runtime
pip install typing-extensions>=4.5.0

# Install monitoring and logging dependencies
pip install structlog>=23.0.0

# Verify installations
echo "$(date): Verifying installations..."

python -c "import redis; print(f'Redis version: {redis.__version__}')" || {
    echo "ERROR: Redis installation failed"
    exit 1
}

python -c "import typing_extensions; print('typing_extensions: OK')" || {
    echo "ERROR: typing_extensions installation failed"
    exit 1
}

# Set up environment variables for the cluster
echo "$(date): Setting up environment variables..."

# Create environment file for all users
sudo tee -a /etc/environment > /dev/null << EOF

# Data Processing Pipeline Environment Variables
PIPELINE_ENVIRONMENT=databricks
SPARK_OPTIMIZATIONS_ENABLED=true
EOF

# Set up logging directory
echo "$(date): Setting up logging directory..."
sudo mkdir -p /var/log/data-pipeline
sudo chmod 755 /var/log/data-pipeline
sudo chown spark:spark /var/log/data-pipeline

# Configure log rotation
sudo tee /etc/logrotate.d/data-pipeline > /dev/null << EOF
/var/log/data-pipeline/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 644 spark spark
}
EOF

# Set up monitoring scripts directory
echo "$(date): Setting up monitoring directory..."
sudo mkdir -p /opt/data-pipeline/monitoring
sudo chmod 755 /opt/data-pipeline/monitoring
sudo chown spark:spark /opt/data-pipeline/monitoring

# Create a health check script for the node
sudo tee /opt/data-pipeline/monitoring/node_health.sh > /dev/null << 'EOF'
#!/bin/bash
# Node health check script

echo "=== Node Health Check ==="
echo "Timestamp: $(date)"
echo "Hostname: $(hostname)"
echo "Uptime: $(uptime)"
echo "Disk Usage:"
df -h
echo "Memory Usage:"
free -h
echo "Python packages:"
pip list | grep -E "(redis|pyspark|delta)"
echo "========================="
EOF

sudo chmod +x /opt/data-pipeline/monitoring/node_health.sh

# Configure Spark optimizations at the node level
echo "$(date): Configuring Spark optimizations..."

# Create Spark defaults configuration
sudo mkdir -p /databricks/spark/conf
sudo tee -a /databricks/spark/conf/spark-defaults.conf > /dev/null << EOF

# Data Processing Pipeline Spark Configuration
spark.sql.execution.arrow.pyspark.enabled true
spark.serializer org.apache.spark.serializer.KryoSerializer
spark.sql.adaptive.enabled true
spark.sql.adaptive.coalescePartitions.enabled true
spark.databricks.delta.optimizeWrite.enabled true
spark.databricks.delta.autoCompact.enabled true
EOF

# Set up Redis connection testing utility
echo "$(date): Setting up Redis connection testing utility..."
sudo tee /opt/data-pipeline/monitoring/test_redis.py > /dev/null << 'EOF'
#!/usr/bin/env python3
"""
Redis connection test utility for cluster nodes
"""
import redis
import sys
import os

def test_redis_connection():
    """Test Redis connection with environment variables or defaults"""
    try:
        # Get Redis configuration from environment
        host = os.getenv('REDIS_HOST', 'localhost')
        port = int(os.getenv('REDIS_PORT', '6379'))
        password = os.getenv('REDIS_PASSWORD')
        ssl_enabled = os.getenv('REDIS_SSL', 'false').lower() == 'true'
        
        # Create Redis client
        if ssl_enabled:
            client = redis.Redis(
                host=host,
                port=port,
                password=password,
                ssl=True,
                socket_timeout=10,
                socket_connect_timeout=5
            )
        else:
            client = redis.Redis(
                host=host,
                port=port,
                password=password,
                socket_timeout=10,
                socket_connect_timeout=5
            )
        
        # Test connection
        client.ping()
        print(f"✅ Redis connection successful: {host}:{port}")
        
        # Test TimeSeries if available
        try:
            client.execute_command("TS.INFO", "test_key")
        except redis.exceptions.ResponseError as e:
            if "does not exist" in str(e):
                print("✅ TimeSeries module available")
            else:
                print(f"⚠️  TimeSeries test failed: {e}")
        except Exception as e:
            print(f"⚠️  TimeSeries not available: {e}")
            
        return True
        
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
        return False

if __name__ == "__main__":
    success = test_redis_connection()
    sys.exit(0 if success else 1)
EOF

sudo chmod +x /opt/data-pipeline/monitoring/test_redis.py

# Create a startup validation script
echo "$(date): Creating startup validation script..."
sudo tee /opt/data-pipeline/monitoring/startup_validation.sh > /dev/null << 'EOF'
#!/bin/bash
# Startup validation script

echo "=== Startup Validation ==="
echo "Timestamp: $(date)"

# Check Python packages
echo "Checking Python packages..."
python -c "import redis, pyspark; print('✅ Core packages available')" || {
    echo "❌ Core packages missing"
    exit 1
}

# Check Spark configuration
echo "Checking Spark configuration..."
if [ -f "/databricks/spark/conf/spark-defaults.conf" ]; then
    echo "✅ Spark configuration found"
else
    echo "⚠️  Spark configuration not found"
fi

# Check directories
echo "Checking directories..."
[ -d "/var/log/data-pipeline" ] && echo "✅ Log directory exists" || echo "❌ Log directory missing"
[ -d "/opt/data-pipeline/monitoring" ] && echo "✅ Monitoring directory exists" || echo "❌ Monitoring directory missing"

echo "=== Validation Complete ==="
EOF

sudo chmod +x /opt/data-pipeline/monitoring/startup_validation.sh

# Run startup validation
echo "$(date): Running startup validation..."
/opt/data-pipeline/monitoring/startup_validation.sh

# Clean up temporary files
echo "$(date): Cleaning up temporary files..."
sudo apt-get autoremove -y
sudo apt-get autoclean

# Final status
echo "$(date): Dependency installation completed successfully!"
echo "$(date): Log file location: $LOG_FILE"

# Create a status file to indicate successful initialization
sudo touch /opt/data-pipeline/init_complete
sudo chmod 644 /opt/data-pipeline/init_complete
echo "$(date)" | sudo tee /opt/data-pipeline/init_complete > /dev/null

echo "$(date): Cluster initialization script completed"