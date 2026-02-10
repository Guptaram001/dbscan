#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Load shared cluster config (same as workers)
source "$SCRIPT_DIR/spark_config.sh"

MODE=${1:-Serial}
INPUT_FILE=${2:-src/main/resources/densired_2_shrink.csv}
DEBUG=${3:-false}

# Set JAVA_HOME to Java 17 for Spark 4.1.0 compatibility
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"

# Job uses all executor cores from cluster by default (from spark_config.sh)
CORES=${TOTAL_EXECUTOR_CORES:-$((NUM_WORKERS * WORKER_CORES))}

echo "Mode: $MODE"
echo "Input: $INPUT_FILE"
if [ "$MODE" == "Serial" ]; then
  echo "Serial cores: $SERIAL_CORES, memory: ${SERIAL_MEMORY:-$WORKER_MEMORY}"
else
  echo "Cores (from cluster: ${NUM_WORKERS} workers x ${WORKER_CORES} cores): $CORES"
fi

echo "Building project..."
mvn clean package -DskipTests

# Create results and Spark event log
mkdir -p results
mkdir -p /tmp/spark-events


if [ "$MODE" == "Serial" ]; then
  echo ""
  echo "Running SERIAL DBSCAN (pure Java, brute-force neighbor search)"
  echo ""

  _SERIAL_MEMORY=${SERIAL_MEMORY:-$WORKER_MEMORY}

  java \
    -Xmx$_SERIAL_MEMORY \
    -XX:+UseG1GC \
    -Djava.util.concurrent.ForkJoinPool.common.parallelism=$SERIAL_CORES \
    -cp target/TemplateSpark-1.0-SNAPSHOT.jar \
    spark.SerialEntryPoint \
    "$INPUT_FILE" "$MODE" "$_SERIAL_MEMORY" "$SERIAL_CORES" "$NUM_WORKERS" "$DRIVER_MEMORY" "$DRIVER_CORES" "$DEBUG"
fi

if [[ "$MODE" == "UF" || "$MODE" == "GraphX" ]]; then

  echo ""
  echo "Submitting DBSCAN job to cluster at spark://$MASTER_HOST:$MASTER_PORT..."
  echo ""

  spark-submit \
      --master spark://$MASTER_HOST:$MASTER_PORT \
      --driver-memory $DRIVER_MEMORY \
      --driver-cores $DRIVER_CORES \
      --executor-memory $WORKER_MEMORY \
      --executor-cores $WORKER_CORES \
      --total-executor-cores $CORES \
      --conf spark.cores.max=$CORES \
      --conf spark.driver.bindAddress=127.0.0.1 \
      --conf spark.default.parallelism=$((CORES * SPARK_DEFAULT_PARALLELISM_FACTOR)) \
      --conf spark.driver.host=127.0.0.1 \
      --conf spark.ui.enabled=false \
      --conf spark.eventLog.enabled=false \
      --conf spark.eventLog.dir=/tmp/spark-events \
      --class spark.EntryPoint \
      target/TemplateSpark-1.0-SNAPSHOT.jar \
      "$INPUT_FILE" "$MODE" "$WORKER_MEMORY" "$WORKER_CORES" "$NUM_WORKERS" "$DRIVER_MEMORY" "$DRIVER_CORES" "$DEBUG"

fi

echo ""
echo "Job completed."
