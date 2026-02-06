#!/usr/bin/env bash
set -e

INPUT_FILE=${1:-src/main/resources/densired_2_shrink.csv}

# Set JAVA_HOME to Java 17 for Spark 4.1.0 compatibility
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"

# Spark cluster configuration
MASTER_HOST=localhost
MASTER_PORT=7077
WORKER_MEMORY=4g
WORKER_CORES=2
NUM_WORKERS=3
DRIVER_MEMORY=2g
DRIVER_CORES=1

echo "Building project..."
mvn clean package -DskipTests

# Create results directory if it doesn't exist
mkdir -p results

echo ""
echo "Submitting DBSCAN job to cluster at spark://$MASTER_HOST:$MASTER_PORT..."
echo ""

spark-submit \
  --master spark://$MASTER_HOST:$MASTER_PORT \
  --driver-memory $DRIVER_MEMORY \
  --driver-cores $DRIVER_CORES \
  --executor-memory $WORKER_MEMORY \
  --executor-cores $WORKER_CORES \
  --total-executor-cores $((WORKER_CORES * NUM_WORKERS)) \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.driver.host=127.0.0.1 \
  --conf spark.ui.enabled=true \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=/tmp/spark-events \
  --conf spark.cores.max=$((WORKER_CORES * NUM_WORKERS)) \
  --class spark.EntryPoint \
  target/TemplateSpark-1.0-SNAPSHOT.jar \
  "$INPUT_FILE"

echo ""
echo "Job completed. Check results/results.csv for output."
