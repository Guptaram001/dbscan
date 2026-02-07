#!/usr/bin/env bash
set -e

# Set JAVA_HOME to Java 17 for Spark 4.1.0 compatibility
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"

# Spark cluster configuration
MASTER_HOST=localhost
MASTER_PORT=7077

# Find Spark installation directory
find_spark_home() {
    if [ -n "$SPARK_HOME" ] && [ -d "$SPARK_HOME" ] && [ -f "$SPARK_HOME/sbin/start-master.sh" ]; then
        echo "$SPARK_HOME"
        return
    fi
    
    if command -v spark-submit >/dev/null 2>&1; then
        SPARK_SUBMIT_PATH=$(which spark-submit)
        if [ -n "$SPARK_SUBMIT_PATH" ]; then
            REAL_PATH=$(readlink -f "$SPARK_SUBMIT_PATH" 2>/dev/null || readlink "$SPARK_SUBMIT_PATH" 2>/dev/null || echo "$SPARK_SUBMIT_PATH")
            POSSIBLE_HOME=$(dirname "$(dirname "$REAL_PATH")")
            
            if [ -f "$POSSIBLE_HOME/sbin/start-master.sh" ]; then
                echo "$POSSIBLE_HOME"
                return
            fi
            
            if [ -d "$POSSIBLE_HOME/libexec" ] && [ -f "$POSSIBLE_HOME/libexec/sbin/start-master.sh" ]; then
                echo "$POSSIBLE_HOME/libexec"
                return
            fi
        fi
    fi
    
    if [ -d "/opt/homebrew/opt/apache-spark" ]; then
        BREW_LINK=$(readlink -f "/opt/homebrew/opt/apache-spark" 2>/dev/null || readlink "/opt/homebrew/opt/apache-spark" 2>/dev/null || echo "/opt/homebrew/opt/apache-spark")
        if [ -d "$BREW_LINK/libexec" ] && [ -f "$BREW_LINK/libexec/sbin/start-master.sh" ]; then
            echo "$BREW_LINK/libexec"
            return
        fi
    fi
    
    echo "Error: SPARK_HOME not set and could not find Spark installation" >&2
    exit 1
}

SPARK_HOME=$(find_spark_home)
export SPARK_HOME
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

# Create work directory
mkdir -p /tmp/spark-master-work

# Clean up any existing master
echo "Cleaning up any existing Spark Master..."
pkill -f "org.apache.spark.deploy.master.Master" 2>/dev/null || true
if [ -f "$SPARK_HOME/sbin/stop-master.sh" ]; then
    $SPARK_HOME/sbin/stop-master.sh 2>/dev/null || true
fi
sleep 2

# Start Spark Master
echo "Starting Spark Master on $MASTER_HOST:$MASTER_PORT"
echo "Master UI will be available at: http://127.0.0.1:8080"
echo "Press Ctrl+C to stop the master"
echo ""

$SPARK_HOME/sbin/start-master.sh \
  --host $MASTER_HOST \
  --port $MASTER_PORT \
  --webui-port 8080

# Keep script running and show logs
tail -f /opt/homebrew/Cellar/apache-spark/4.1.0/libexec/logs/spark-*-org.apache.spark.deploy.master.Master-*.out 2>/dev/null || echo "Master started. Check logs in $SPARK_HOME/logs/"
