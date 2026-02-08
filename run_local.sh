#!/usr/bin/env bash
set -e

INPUT_FILE=${1:-src/main/resources/k10.csv}

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

spark-submit \
  --master local[*] \
  --driver-memory 8g \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.driver.host=127.0.0.1 \
  --conf spark.ui.enabled=false \
  --conf spark.eventLog.enabled=false \
  --class spark.EntryPoint \
  target/TemplateSpark-1.0-SNAPSHOT.jar \
  "$INPUT_FILE"

