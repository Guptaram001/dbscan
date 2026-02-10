#!/usr/bin/env bash
set -e

MODE=${1:-Serial}
INPUT_FILE=${2:-src/main/resources/densired_2_shrink.csv}
DEBUG=${3:-false}

# Set JAVA_HOME to Java 17 for Spark 4.1.0 compatibility
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export PATH="$JAVA_HOME/bin:$PATH"

#Resource
CORES=9
MEMORY=10g

# Spark cluster configuration
MASTER_HOST=localhost
MASTER_PORT=7077
WORKER_MEMORY=3g
WORKER_CORES=2
NUM_WORKERS=3
DRIVER_MEMORY=1g
DRIVER_CORES=1

echo "Mode: $MODE"
echo "Input: $INPUT_FILE"
echo "Cores budget: $CORES"
echo "Memory budget: $MEMORY"

echo "Building project..."
mvn clean package -DskipTests

# Create results and Spark event log
mkdir -p results
mkdir -p /tmp/spark-events


if [ "$MODE" == "Serial"  ]; then
  echo ""
  echo "Running SERIAL DBSCAN (pure Java)"
  echo ""

  java \
    -Xmx$WORKER_MEMORY \
    -XX:+UseG1GC \
    -Djava.util.concurrent.ForkJoinPool.common.parallelism=$WORKER_CORES \
    -cp target/TemplateSpark-1.0-SNAPSHOT.jar \
    spark.SerialEntryPoint \
    "$INPUT_FILE" "$MODE" "$WORKER_MEMORY" "$WORKER_CORES" "$NUM_WORKERS" "$DRIVER_MEMORY" "$DRIVER_CORES" "$DEBUG"
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
