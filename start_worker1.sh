#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/spark_config.sh"

# Set JAVA_HOME to Java 17 for Spark 4.1.0 compatibility
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"

WORKER_NUM=1

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
mkdir -p /tmp/spark-worker${WORKER_NUM}-work

# Clean up any existing worker
echo "Cleaning up any existing Worker $WORKER_NUM..."
if [ -f /tmp/spark-worker${WORKER_NUM}-work/worker.pid ]; then
    kill $(cat /tmp/spark-worker${WORKER_NUM}-work/worker.pid) 2>/dev/null || true
    rm -f /tmp/spark-worker${WORKER_NUM}-work/worker.pid 2>/dev/null || true
fi
sleep 1

# Start Worker
echo "Starting Worker $WORKER_NUM (memory: $WORKER_MEMORY, cores: $WORKER_CORES)"
echo "Connecting to master: spark://$MASTER_HOST:$MASTER_PORT"
echo "Worker UI will be available at: http://127.0.0.1:808$WORKER_NUM"
echo "Press Ctrl+C to stop the worker"
echo ""

export SPARK_LOCAL_IP=$MASTER_HOST
export SPARK_WORKER_OPTS="-Dspark.worker.bindAddress=0.0.0.0 -Dspark.worker.host=$MASTER_HOST"

nohup $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker \
  --webui-port 808$WORKER_NUM \
  spark://$MASTER_HOST:$MASTER_PORT \
  --memory $WORKER_MEMORY \
  --cores $WORKER_CORES \
  --work-dir /tmp/spark-worker${WORKER_NUM}-work \
  > /tmp/spark-worker${WORKER_NUM}-work/worker.out 2>&1 &

WORKER_PID=$!
echo $WORKER_PID > /tmp/spark-worker${WORKER_NUM}-work/worker.pid
echo "Worker $WORKER_NUM started with PID: $WORKER_PID"

# Show logs
tail -f /tmp/spark-worker${WORKER_NUM}-work/worker.out
