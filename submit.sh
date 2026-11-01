#!/bin/bash
set -e

# Скрипт запуска Spark-приложения через spark-submit
# Для локального режима используется --master 'local[*]'
# Для запуска на кластере YARN укажите --master yarn

spark-submit \
  --master "local[*]" \
  --packages "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  --driver-memory 8g \
  main.py \
  --master "local[*]" \
  --start-date 2024-11-01 \
  --end-date 2024-11-30 \
  --geo-id "Москва, Россия" \
  --columns usage_platform_ru genre \
  --show-analytics \
  --show-plan \
  "$@"
