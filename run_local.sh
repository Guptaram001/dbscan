#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/spark_config.sh"

# Arguments:
#   $1 = eps       (optional, default: 0.03)
#   $2 = minPts    (optional, default: 50)
#   $3 = MODE      (optional, default: Serial) – Serial | UF | GraphX
#   $4 = INPUT_FILE (optional, default: src/main/resources/densired_2_shrink.csv)
#   $5 = DEBUG     (optional, default: false)
EPS=${1:-0.03}
MINPTS=${2:-50}
MODE=${3:-Serial}
INPUT_FILE=${4:-src/main/resources/densired_2_shrink.csv}
DEBUG=${5:-false}

# Spark 4.x requires Java 17+. Prefer Java 17 for spark-submit.
JAVA_VERSION=$(java -version 2>&1 | head -1)
if ! echo "$JAVA_VERSION" | grep -qE '"1[789]\.|"2[0-9]\.'; then
  if J17=$(/usr/libexec/java_home -v 17 2>/dev/null); then
    export JAVA_HOME=$J17
  elif [ -d /opt/homebrew/opt/openjdk@17 ]; then
    export JAVA_HOME=/opt/homebrew/opt/openjdk@17
  fi
  [ -n "$JAVA_HOME" ] && echo "Using JAVA_HOME=$JAVA_HOME for Spark (Java 17 required)"
fi

echo "Building project"
mvn clean package -DskipTests

#--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
#--conf spark.kryo.registrator=spark.MyRegistrator \

if [ "$MODE" == "Serial" ]; then
  echo ""
  echo "Running SERIAL DBSCAN (pure Java, brute-force neighbor search)"
  echo ""

_SERIAL_MEMORY=${SERIAL_MEMORY:-$WORKER_MEMORY}

  java \
    -cp target/TemplateSpark-1.0-SNAPSHOT.jar \
    spark.SerialEntryPoint \
    "$INPUT_FILE" "$MODE" "$_SERIAL_MEMORY" "$SERIAL_CORES" "$NUM_WORKERS" "$DRIVER_MEMORY" "$DRIVER_CORES" "$DEBUG" "$EPS" "$MINPTS"
fi

if [[ "$MODE" == "UF" || "$MODE" == "GraphX" ]]; then

  echo ""
  echo "Submitting DBSCAN job to cluster at spark"
  echo ""

  spark-submit \
  --master local[*] \
    --driver-memory 8g \
    --conf spark.driver.bindAddress=127.0.0.1 \
    --conf spark.driver.host=127.0.0.1 \
    --conf spark.ui.enabled=true \
    --conf spark.eventLog.enabled=false \
    --class spark.EntryPoint \
    target/TemplateSpark-1.0-SNAPSHOT.jar \
    "$INPUT_FILE" "$MODE" "$WORKER_MEMORY" "$WORKER_CORES" "$NUM_WORKERS" "$DRIVER_MEMORY" "$DRIVER_CORES" "$DEBUG" "$EPS" "$MINPTS"
fi

echo ""
echo "Job completed."


