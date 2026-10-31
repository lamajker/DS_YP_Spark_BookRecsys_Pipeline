#!/bin/bash
set -e

# Скрипт запуска Spark-приложения
# Для локального режима используется --master 'local[*]'
# Для запуска на кластере YARN укажите --master yarn

python3 main.py \
  --master 'local[*]' \
  --start-date 2024-11-01 \
  --end-date 2024-11-30 \
  --geo-id "Москва, Россия" \
  --columns usage_platform_ru genre \
  --show-analytics \
  --show-plan \
  "$@"
