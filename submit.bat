@echo off
rem Скрипт локального запуска на Windows

rem Если JAVA_HOME не задан глобально, можно указать путь к JDK:
rem if "%JAVA_HOME%"=="" set "JAVA_HOME=C:\Program Files\Java\jdk-17"

python main.py ^
  --master "local[*]" ^
  --start-date 2024-11-01 ^
  --end-date 2024-11-30 ^
  --geo-id "Москва, Россия" ^
  --columns usage_platform_ru genre ^
  --show-analytics ^
  --show-plan ^
  %*
