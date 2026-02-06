#!/usr/bin/env bash
set -e

INPUT_FILE=${1:-src/main/resources/densired_2_shrink.csv}

# Set JAVA_HOME to Java 17 for Spark 4.1.0 compatibility
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"

# Spark cluster configuration
# Use localhost instead of 127.0.0.1 for better macOS compatibility
MASTER_HOST=localhost
MASTER_PORT=7077
WORKER_MEMORY=4g
WORKER_CORES=2
DRIVER_MEMORY=2g
DRIVER_CORES=1

# Find Spark installation directory
find_spark_home() {
    # If SPARK_HOME is already set, use it
    if [ -n "$SPARK_HOME" ] && [ -d "$SPARK_HOME" ] && [ -f "$SPARK_HOME/sbin/start-master.sh" ]; then
        echo "$SPARK_HOME"
        return
    fi
    
    # Try to find spark-submit in PATH and derive SPARK_HOME
    if command -v spark-submit >/dev/null 2>&1; then
        SPARK_SUBMIT_PATH=$(which spark-submit)
        # Resolve symlinks to get actual path
        if [ -n "$SPARK_SUBMIT_PATH" ]; then
            REAL_PATH=$(readlink -f "$SPARK_SUBMIT_PATH" 2>/dev/null || readlink "$SPARK_SUBMIT_PATH" 2>/dev/null || echo "$SPARK_SUBMIT_PATH")
            # spark-submit is typically in $SPARK_HOME/bin/spark-submit or $SPARK_HOME/libexec/bin/spark-submit
            POSSIBLE_HOME=$(dirname "$(dirname "$REAL_PATH")")
            
            # Check if this is the SPARK_HOME (has sbin/start-master.sh)
            if [ -f "$POSSIBLE_HOME/sbin/start-master.sh" ]; then
                echo "$POSSIBLE_HOME"
                return
            fi
            
            # For Homebrew, check if libexec subdirectory exists
            if [ -d "$POSSIBLE_HOME/libexec" ] && [ -f "$POSSIBLE_HOME/libexec/sbin/start-master.sh" ]; then
                echo "$POSSIBLE_HOME/libexec"
                return
            fi
        fi
    fi
    
    # Try common installation locations (checking for Homebrew structure)
    if [ -d "/opt/homebrew/opt/apache-spark" ]; then
        # Homebrew symlink - resolve to actual location
        BREW_LINK=$(readlink -f "/opt/homebrew/opt/apache-spark" 2>/dev/null || readlink "/opt/homebrew/opt/apache-spark" 2>/dev/null || echo "/opt/homebrew/opt/apache-spark")
        if [ -d "$BREW_LINK/libexec" ] && [ -f "$BREW_LINK/libexec/sbin/start-master.sh" ]; then
            echo "$BREW_LINK/libexec"
            return
        fi
    fi
    
    # Try other common locations
    if [ -d "/usr/local/spark" ] && [ -f "/usr/local/spark/sbin/start-master.sh" ]; then
        echo "/usr/local/spark"
        return
    fi
    
    if [ -d "$HOME/spark" ] && [ -f "$HOME/spark/sbin/start-master.sh" ]; then
        echo "$HOME/spark"
        return
    fi
    
    echo "Error: SPARK_HOME not set and could not find Spark installation" >&2
    echo "Please set SPARK_HOME environment variable or install Spark" >&2
    exit 1
}

SPARK_HOME=$(find_spark_home)
export SPARK_HOME
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

# Cleanup function
cleanup() {
    echo ""
    echo "Shutting down Spark cluster..."
    # Use Spark's stop scripts if available (more reliable)
    if [ -f "$SPARK_HOME/sbin/stop-worker.sh" ]; then
        $SPARK_HOME/sbin/stop-worker.sh 2>/dev/null || true
    fi
    if [ -f "$SPARK_HOME/sbin/stop-master.sh" ]; then
        $SPARK_HOME/sbin/stop-master.sh 2>/dev/null || true
    fi
    # Stop workers by PID files
    if [ -f /tmp/spark-worker1-work/worker.pid ]; then
        kill $(cat /tmp/spark-worker1-work/worker.pid) 2>/dev/null || true
        rm -f /tmp/spark-worker1-work/worker.pid 2>/dev/null || true
    fi
    if [ -f /tmp/spark-worker2-work/worker.pid ]; then
        kill $(cat /tmp/spark-worker2-work/worker.pid) 2>/dev/null || true
        rm -f /tmp/spark-worker2-work/worker.pid 2>/dev/null || true
    fi
    # Stop master by PID file
    if [ -f /tmp/spark-master-work/master.pid ]; then
        kill $(cat /tmp/spark-master-work/master.pid) 2>/dev/null || true
        rm -f /tmp/spark-master-work/master.pid 2>/dev/null || true
    fi
    # Fallback: kill by process name
    pkill -f "org.apache.spark.deploy.worker.Worker" 2>/dev/null || true
    pkill -f "org.apache.spark.deploy.master.Master" 2>/dev/null || true
    sleep 2
    echo "Cluster shutdown complete."
}

# Set trap to cleanup on exit
trap cleanup EXIT INT TERM

# Clean up any existing Spark processes before starting
echo "Cleaning up any existing Spark processes..."
pkill -f "org.apache.spark.deploy.worker.Worker" 2>/dev/null || true
pkill -f "org.apache.spark.deploy.master.Master" 2>/dev/null || true
# Also try using Spark's stop scripts if they exist
if [ -f "$SPARK_HOME/sbin/stop-worker.sh" ]; then
    $SPARK_HOME/sbin/stop-worker.sh 2>/dev/null || true
fi
if [ -f "$SPARK_HOME/sbin/stop-master.sh" ]; then
    $SPARK_HOME/sbin/stop-master.sh 2>/dev/null || true
fi
sleep 2

echo "Building project"
mvn clean package -DskipTests

# Create results directory if it doesn't exist
mkdir -p results

# Create work directories for master and workers
mkdir -p /tmp/spark-master-work
mkdir -p /tmp/spark-worker1-work
mkdir -p /tmp/spark-worker2-work
mkdir -p /tmp/spark-events

# Check if Spark scripts exist
if [ ! -f "$SPARK_HOME/sbin/start-master.sh" ]; then
    echo "Error: Spark master script not found at $SPARK_HOME/sbin/start-master.sh" >&2
    echo "Please verify SPARK_HOME is set correctly (currently: $SPARK_HOME)" >&2
    exit 1
fi

# Start Spark Master
echo "Starting Spark Master on $MASTER_HOST:$MASTER_PORT"
$SPARK_HOME/sbin/start-master.sh \
  --host $MASTER_HOST \
  --port $MASTER_PORT \
  --webui-port 8080

sleep 5

# Verify master is running
if ! pgrep -f "org.apache.spark.deploy.master.Master" > /dev/null; then
    echo "Error: Spark Master failed to start" >&2
    exit 1
fi

# Start Worker 1
echo "Starting Worker 1 (memory: $WORKER_MEMORY, cores: $WORKER_CORES)"
export SPARK_LOCAL_IP=$MASTER_HOST
export SPARK_WORKER_OPTS="-Dspark.worker.bindAddress=0.0.0.0 -Dspark.worker.host=$MASTER_HOST"
# Start worker directly using spark-class to avoid conflict detection
nohup $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker \
  --webui-port 8081 \
  spark://$MASTER_HOST:$MASTER_PORT \
  --memory $WORKER_MEMORY \
  --cores $WORKER_CORES \
  --work-dir /tmp/spark-worker1-work \
  > /tmp/spark-worker1-work/worker.out 2>&1 &
echo $! > /tmp/spark-worker1-work/worker.pid

sleep 3

# Start Worker 2
echo "Starting Worker 2 (memory: $WORKER_MEMORY, cores: $WORKER_CORES)"
export SPARK_LOCAL_IP=$MASTER_HOST
export SPARK_WORKER_OPTS="-Dspark.worker.bindAddress=0.0.0.0 -Dspark.worker.host=$MASTER_HOST"
# Start worker directly using spark-class to avoid conflict detection
nohup $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker \
  --webui-port 8082 \
  spark://$MASTER_HOST:$MASTER_PORT \
  --memory $WORKER_MEMORY \
  --cores $WORKER_CORES \
  --work-dir /tmp/spark-worker2-work \
  > /tmp/spark-worker2-work/worker.out 2>&1 &
echo $! > /tmp/spark-worker2-work/worker.pid

sleep 5

# Verify workers are running
WORKER_COUNT=$(pgrep -f "org.apache.spark.deploy.worker.Worker" | wc -l | tr -d ' ')
if [ "$WORKER_COUNT" -lt 2 ]; then
    echo "Warning: Expected 2 workers, but only $WORKER_COUNT are running" >&2
fi

echo "Spark cluster is running:"
echo "  Master UI: http://127.0.0.1:8080"
echo "  Worker 1 UI: http://127.0.0.1:8081"
echo "  Worker 2 UI: http://127.0.0.1:8082"
echo ""

# Submit job to cluster
echo "Submitting DBSCAN job to cluster..."
spark-submit \
  --master spark://$MASTER_HOST:$MASTER_PORT \
  --driver-memory $DRIVER_MEMORY \
  --driver-cores $DRIVER_CORES \
  --executor-memory $WORKER_MEMORY \
  --executor-cores $WORKER_CORES \
  --total-executor-cores $((WORKER_CORES * 2)) \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.driver.host=127.0.0.1 \
  --conf spark.ui.enabled=true \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=/tmp/spark-events \
  --conf spark.cores.max=$((WORKER_CORES * 2)) \
  --class spark.EntryPoint \
  target/TemplateSpark-1.0-SNAPSHOT.jar \
  "$INPUT_FILE"

echo ""
echo "Job completed. Check results/results.csv for output."
