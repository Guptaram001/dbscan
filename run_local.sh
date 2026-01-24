#!/usr/bin/env bash
set -e

INPUT_FILE=${1:-src/main/resources/densired_2_shrink.csv}

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

