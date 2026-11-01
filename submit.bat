@echo off
rem Скрипт запуска Spark-приложения через spark-submit на Windows
rem Для локального режима используется --master "local[*]"
rem Для кластера YARN укажите --master yarn

rem Если JAVA_HOME не задан глобально, можно раскомментировать и указать путь к JDK:
rem if "%JAVA_HOME%"=="" set "JAVA_HOME=C:\Program Files\Java\jdk-17"

spark-submit ^
  --master "local[*]" ^
  --packages "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" ^
  --driver-memory 8g ^
  main.py ^
  --master "local[*]" ^
  --start-date 2024-11-01 ^
  --end-date 2024-11-30 ^
  --geo-id "Москва, Россия" ^
  --columns usage_platform_ru genre ^
  --show-analytics ^
  --show-plan ^
  %*
