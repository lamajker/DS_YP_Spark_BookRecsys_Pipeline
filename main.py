import os
import sys
import argparse
import logging
from pathlib import Path
from dotenv import load_dotenv

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("book_recsys")

# грузим переменные из .env если он есть
dotenv_path = Path(__file__).resolve().parent / ".env"
if dotenv_path.is_file():
    load_dotenv(dotenv_path)

S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "https://storage.yandexcloud.net")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY_ID", "")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "")
S3_SOURCE_BUCKET = os.getenv("S3_SOURCE_BUCKET", "s3-ds-source")
S3_OUTPUT_BUCKET = os.getenv("S3_OUTPUT_BUCKET", "")

DEFAULT_AUDITION_PATH = f"s3a://{S3_SOURCE_BUCKET}/audition.parquet"
DEFAULT_CONTENT_PATH = f"s3a://{S3_SOURCE_BUCKET}/content.parquet"
DEFAULT_OUTPUT_PATH = (
    f"s3a://{S3_OUTPUT_BUCKET}/results/S24_project_data"
    if S3_OUTPUT_BUCKET
    else "results/S24_project_data"
)


# инициализация спарк сессии с настройками под s3
def create_spark_session(master="local[*]", app_name="BookRecsysPipeline"):
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        # настройки s3
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # память и ядра для стабильного запуска
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.executor.instances", "2")
        .config("spark.driver.cores", "4")
        .config("spark.driver.memory", "8g")  # 8 гигов по ТЗ
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.network.timeout", "300")
        # отключаем broadcast join, чтобы драйвер не падал по памяти при джойнах
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# парсинг параметров командной строки
def parse_arguments():
    parser = argparse.ArgumentParser(
        description="ETL пайплайн для построения рекомендательных витрин"
    )
    parser.add_argument(
        "--start-date",
        dest="start_date",
        default=None,
        help="Начальная дата (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date", dest="end_date", default=None, help="Конечная дата (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--geo-id",
        dest="geo_id",
        default=None,
        help="Регион, например 'Москва, Россия'",
    )
    parser.add_argument(
        "--columns", nargs="*", default=None, help="Колонки для расчета распределений"
    )
    parser.add_argument(
        "--audition-path",
        dest="audition_path",
        default=DEFAULT_AUDITION_PATH,
        help="Путь к audition.parquet",
    )
    parser.add_argument(
        "--content-path",
        dest="content_path",
        default=DEFAULT_CONTENT_PATH,
        help="Путь к content.parquet",
    )
    parser.add_argument(
        "--output-path",
        dest="output_path",
        default=DEFAULT_OUTPUT_PATH,
        help="Куда сохранять витрины",
    )
    parser.add_argument(
        "--master", default="local[*]", help="Мастер Spark: local[*] или yarn"
    )
    parser.add_argument(
        "--show-plan", action="store_true", help="Вывести план выполнения user_mart"
    )
    parser.add_argument(
        "--show-analytics",
        action="store_true",
        help="Показать распределения по колонкам и дням",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Подробный вывод промежуточных шагов"
    )
    return parser.parse_args()


# переводим день недели в русское название
def get_russian_weekday(date_column):
    d = F.dayofweek(date_column)
    return (
        F.when(d == 1, "воскресенье")
        .when(d == 2, "понедельник")
        .when(d == 3, "вторник")
        .when(d == 4, "среда")
        .when(d == 5, "четверг")
        .when(d == 6, "пятница")
        .when(d == 7, "суббота")
    )


# чистим сессии: дату в тип Date, убираем сессии меньше 0 и больше 24ч, пустые версии заполняем unknown
def clean_auditions(df, verbose=False):
    df = df.withColumn("msk_business_dt_str", F.to_date(F.col("msk_business_dt_str")))

    valid_hours_expr = (
        F.col("hours").isNotNull()
        & F.col("hours_sessions_long").isNotNull()
        & (F.col("hours") >= 0)
        & (F.col("hours") <= 24)
        & (F.col("hours_sessions_long") >= 0)
        & (F.col("hours_sessions_long") <= 24)
    )

    if verbose:
        total_before = df.count()
        df_cleaned = df.filter(valid_hours_expr).fillna({"app_version": "unknown"})
        dropped = total_before - df_cleaned.count()
        logger.info("audition: отфильтровано %d строк с некорректными часами", dropped)
        return df_cleaned

    return df.filter(valid_hours_expr).fillna({"app_version": "unknown"})


# чистим контент: пропуски и нули в длительности заменяем медианой нормальных книг
def clean_contents(df, verbose=False):
    null_or_zero_count = df.filter(
        F.col("main_content_duration_hours").isNull()
        | (F.col("main_content_duration_hours") <= 0)
    ).count()

    median_duration = df.filter(
        F.col("main_content_duration_hours") > 0
    ).approxQuantile("main_content_duration_hours", [0.5], 0.01)[0]

    df_cleaned = df.withColumn(
        "main_content_duration_hours",
        F.when(F.col("main_content_duration_hours") <= 0, F.lit(None)).otherwise(
            F.col("main_content_duration_hours")
        ),
    ).fillna({"main_content_duration_hours": median_duration})

    if verbose:
        logger.info(
            "content: заменено %d аномальных длительностей на медиану = %.7f ч",
            null_or_zero_count,
            median_duration,
        )

    return df_cleaned


# объединяем сессии с контентом по id книги и приводим строку жанров в нормальный массив
def join_and_parse_topics(aud_df, con_df, verbose=False):
    joined = aud_df.join(con_df, on="main_content_id", how="inner")

    # разбиваем строку по запятой и срезаем лишние кавычки с пробелами
    joined = joined.withColumn(
        "published_topic_title_list",
        F.transform(
            F.split(F.col("published_topic_title_list"), ",\\s*"),
            lambda item: F.regexp_replace(F.trim(item), "^'|'$", ""),
        ),
    )

    if verbose:
        logger.info("Джойн audition и content выполнен: %d строк", joined.count())

    return joined


# фильтрация по датам и гео, если параметры были переданы
def filter_dataset(df, start_date=None, end_date=None, geo_id=None):
    filtered = df
    if start_date:
        filtered = filtered.filter(
            F.col("msk_business_dt_str") >= F.to_date(F.lit(start_date))
        )
    if end_date:
        filtered = filtered.filter(
            F.col("msk_business_dt_str") <= F.to_date(F.lit(end_date))
        )
    if geo_id:
        filtered = filtered.filter(F.col("usage_geo_id") == geo_id)
    return filtered


# вспомогательная функция: ищем топовую категорию юзера (любимый жанр, платформа или тип контента)
def get_user_favorite_dimension(df, dimension_col, output_alias):
    w = Window.partitionBy("puid").orderBy(
        F.desc("total_hours_dim"),
        F.desc("sessions_count"),
        F.asc(dimension_col),
    )
    return (
        df.groupBy("puid", dimension_col)
        .agg(
            F.sum("hours").alias("total_hours_dim"),
            F.count("*").alias("sessions_count"),
        )
        .withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") == 1)
        .select("puid", F.col(dimension_col).alias(output_alias))
    )


# 1. витрина: статистика пользователя по каждой книге
def build_user_content_mart(df):
    dt = F.col("msk_business_dt_str")
    return (
        df.groupBy("puid", "main_content_id")
        .agg(
            F.sum("hours").alias("total_hours"),
            F.count("*").alias("sessions_count"),
            F.max(dt).alias("last_listening_date"),
            (F.datediff(F.max(dt), F.min(dt)) + 1).alias("days_spent"),
            F.first("main_content_duration_hours").alias("main_content_duration_hours"),
        )
        .withColumn(
            "finished_percent",
            F.least(
                F.col("total_hours") / F.col("main_content_duration_hours"), F.lit(1.0)
            ),
        )
        .withColumn(
            "is_completed", F.when(F.col("finished_percent") >= 1.0, 1).otherwise(0)
        )
        .withColumn("avg_session_hours", F.col("total_hours") / F.col("sessions_count"))
        .select(
            "puid",
            "main_content_id",
            "total_hours",
            "sessions_count",
            "finished_percent",
            "is_completed",
            "last_listening_date",
            "days_spent",
            "avg_session_hours",
        )
    )


# 2. витрина: интерес пользователя к жанрам с ранжированием по часам
def build_user_genre_mart(df, user_content_mart):
    # разворачиваем массив жанров в отдельные строки
    exploded = (
        df.select(
            "puid",
            "main_content_id",
            "hours",
            "adult_content_flg",
            "kids_content_flg",
            F.explode("published_topic_title_list").alias("genre"),
        )
        .filter(F.col("genre").isNotNull() & (F.col("genre") != ""))
        .join(
            user_content_mart.select("puid", "main_content_id", "is_completed"),
            on=["puid", "main_content_id"],
        )
    )

    w_genre = Window.partitionBy("puid").orderBy(
        F.desc("total_hours"),
        F.desc("sessions_count"),
        F.asc("genre"),
    )

    return (
        exploded.groupBy("puid", "genre")
        .agg(
            F.sum("hours").alias("total_hours"),
            F.count("*").alias("sessions_count"),
            F.countDistinct(
                F.when(F.col("is_completed") == 1, F.col("main_content_id"))
            ).alias("completed_count"),
            F.countDistinct(
                F.when(F.col("adult_content_flg"), F.col("main_content_id"))
            ).alias("adult_count"),
            F.countDistinct(
                F.when(F.col("kids_content_flg"), F.col("main_content_id"))
            ).alias("kids_count"),
        )
        .withColumn("genre_rank", F.row_number().over(w_genre))
        .select(
            "puid",
            "genre",
            "total_hours",
            "sessions_count",
            "completed_count",
            "genre_rank",
            "adult_count",
            "kids_count",
        )
    )


# 3. витрина: итоговый профиль пользователя со всеми предпочтениями
def build_user_mart(df, user_content_mart, user_genre_mart):
    dt = F.col("msk_business_dt_str")

    base_user = df.groupBy("puid").agg(
        F.sum("hours").alias("total_hours"),
        F.countDistinct(dt).alias("active_days"),
        F.countDistinct("main_content_id").alias("unique_content_count"),
        F.max(dt).alias("last_date"),
        F.countDistinct(
            F.when(F.col("adult_content_flg"), F.col("main_content_id"))
        ).alias("adult_content_count"),
        F.countDistinct(
            F.when(F.col("kids_content_flg"), F.col("main_content_id"))
        ).alias("kids_content_count"),
    )

    # считаем уникальные жанры
    unique_genres = (
        df.select("puid", F.explode("published_topic_title_list").alias("genre"))
        .filter(F.col("genre").isNotNull() & (F.col("genre") != ""))
        .groupBy("puid")
        .agg(F.countDistinct("genre").alias("unique_genres_count"))
    )

    # средняя дочитываемость книг
    completion_stats = user_content_mart.groupBy("puid").agg(
        F.avg("finished_percent").alias("completion_rate")
    )

    # любимый жанр: содержательный жанр (не 'Аудио') с минимальным рангом, если других жанров у пользователя нет, берем 'Аудио' как запасной вариант (фолбэк)
    w_fav_genre = Window.partitionBy("puid").orderBy(
        F.when(F.col("genre") != "Аудио", 0).otherwise(1),
        F.col("genre_rank"),
    )
    fav_genre = (
        user_genre_mart.withColumn(
            "fav_priority_rank", F.row_number().over(w_fav_genre)
        )
        .filter(F.col("fav_priority_rank") == 1)
        .select("puid", F.col("genre").alias("favorite_genre"))
    )

    fav_content_type = get_user_favorite_dimension(
        df, "main_content_type", "favorite_content_type"
    )
    fav_platform = get_user_favorite_dimension(
        df, "usage_platform_ru", "favourite_platform"
    )

    return (
        base_user.join(unique_genres, on="puid", how="left")
        .join(completion_stats, on="puid", how="left")
        .join(fav_genre, on="puid", how="left")
        .join(fav_content_type, on="puid", how="left")
        .join(fav_platform, on="puid", how="left")
        .select(
            "puid",
            "total_hours",
            "active_days",
            "unique_content_count",
            "unique_genres_count",
            "completion_rate",
            "favorite_genre",
            "favorite_content_type",
            "favourite_platform",
            "last_date",
            "adult_content_count",
            "kids_content_count",
        )
    )


# 4. витрина: метрики контента и его популярность
def build_content_mart(df, user_content_mart):
    total_users_count = df.select("puid").distinct().count()

    avg_completion = user_content_mart.groupBy("main_content_id").agg(
        (F.avg("finished_percent") * 100).alias("avg_finished_percent")
    )

    return (
        df.groupBy("main_content_id")
        .agg(
            F.countDistinct("puid").alias("unique_users"),
            F.sum("hours").alias("total_hours"),
            F.countDistinct("usage_platform_ru").alias("platforms_count"),
        )
        .join(avg_completion, on="main_content_id", how="left")
        .withColumn("popularity", F.col("unique_users") / F.lit(total_users_count))
        .select(
            "main_content_id",
            "unique_users",
            "total_hours",
            "avg_finished_percent",
            "platforms_count",
            "popularity",
        )
    )


# 5. витрина: активность пользователя по дням
def build_daily_user_mart(df):
    return (
        df.withColumn("session_date", F.col("msk_business_dt_str"))
        .groupBy("puid", "session_date")
        .agg(
            F.countDistinct("main_content_id").alias("content_count"),
            F.sum("hours").alias("hours_spent"),
            F.count("*").alias("sessions_count"),
        )
        .withColumn("day_of_week", get_russian_weekday(F.col("session_date")))
        .select(
            "puid",
            "session_date",
            "day_of_week",
            "content_count",
            "hours_spent",
            "sessions_count",
        )
    )


# собираем все пять витрин в один словарь
def make_all_marts(joined_df, verbose=False):
    logger.info("Считаем user_content_mart...")
    user_content = build_user_content_mart(joined_df)

    logger.info("Считаем user_genre_mart...")
    user_genre = build_user_genre_mart(joined_df, user_content)

    logger.info("Считаем user_mart...")
    user = build_user_mart(joined_df, user_content, user_genre)

    logger.info("Считаем content_mart...")
    content = build_content_mart(joined_df, user_content)

    logger.info("Считаем daily_user_mart...")
    daily_user = build_daily_user_mart(joined_df)

    if verbose:
        for name, mart in [
            ("user_content_mart", user_content),
            ("user_genre_mart", user_genre),
            ("user_mart", user),
            ("content_mart", content),
            ("daily_user_mart", daily_user),
        ]:
            print(f"\n--- Пример: {name} ---")
            mart.show(3, truncate=False)

    return {
        "user_content_mart": user_content,
        "user_genre_mart": user_genre,
        "user_mart": user,
        "content_mart": content,
        "daily_user_mart": daily_user,
    }


# запись витрин в паркет
def save_marts(marts_dict, output_directory):
    base_path = output_directory.rstrip("/")
    for name, df in marts_dict.items():
        target_path = f"{base_path}/{name}.parquet"
        logger.info("Сохраняем %s в %s", name, target_path)
        df.write.mode("overwrite").parquet(target_path)
        logger.info("Сохранен: %s", target_path)


# вывод базовых распределений
def show_analytics(df):
    print("\n--- Типы контента ---")
    (
        df.groupBy("main_content_type")
        .agg(F.countDistinct("puid").alias("users"), F.count("*").alias("sessions"))
        .orderBy(F.desc("users"), F.desc("sessions"))
        .show(truncate=False)
    )

    print("--- Дни недели: длительность и сессии ---")
    (
        df.withColumn("day_of_week", get_russian_weekday(F.col("msk_business_dt_str")))
        .groupBy("day_of_week")
        .agg(
            F.count("*").alias("sessions"),
            F.round(F.avg("hours"), 4).alias("avg_hours"),
            F.round(F.expr("percentile_approx(hours, 0.5)"), 4).alias("median_hours"),
        )
        .orderBy("day_of_week")
        .show(truncate=False)
    )

    print("--- Топ регионов по числу пользователей ---")
    (
        df.filter(F.col("usage_geo_id").isNotNull())
        .groupBy("usage_geo_id")
        .agg(
            F.countDistinct("puid").alias("users"),
            F.count("*").alias("sessions"),
            F.round(F.avg("hours"), 4).alias("avg_hours"),
        )
        .orderBy(F.desc("users"), F.desc("sessions"))
        .show(15, truncate=False)
    )


# распределения по произвольным колонкам из флага --columns
def show_column_distributions(df, columns):
    for col_name in columns:
        print(f"\n--- Распределение: {col_name} (топ-20 по пользователям) ---")
        if col_name == "genre":
            (
                df.select(
                    "puid", F.explode("published_topic_title_list").alias("genre")
                )
                .filter(F.col("genre").isNotNull() & (F.col("genre") != ""))
                .groupBy("genre")
                .agg(
                    F.countDistinct("puid").alias("users"),
                    F.count("*").alias("sessions"),
                )
                .orderBy(F.desc("users"), F.desc("sessions"))
                .show(20, truncate=False)
            )
        elif col_name in df.columns:
            (
                df.filter(F.col(col_name).isNotNull())
                .groupBy(col_name)
                .agg(
                    F.countDistinct("puid").alias("users"),
                    F.count("*").alias("sessions"),
                )
                .orderBy(F.desc("users"), F.desc("sessions"))
                .show(20, truncate=False)
            )
        else:
            logger.warning("Колонки %s нет в датафрейме, пропускаем", col_name)


def main():
    args = parse_arguments()
    logger.info("Запуск Spark (master: %s)", args.master)
    spark = create_spark_session(master=args.master)

    try:
        logger.info("Чтение данных:")
        logger.info("  сессии:  %s", args.audition_path)
        logger.info("  контент: %s", args.content_path)
        raw_audition = spark.read.parquet(args.audition_path)
        raw_content = spark.read.parquet(args.content_path)

        if args.verbose:
            logger.info(
                "Размеры на входе: audition (%d, %d), content (%d, %d)",
                raw_audition.count(),
                len(raw_audition.columns),
                raw_content.count(),
                len(raw_content.columns),
            )
            print("\n--- Схема audition ---")
            raw_audition.printSchema()
            print("\n--- Пример строк: audition (первые 5) ---")
            raw_audition.show(5, truncate=False)

            print("\n--- Схема content ---")
            raw_content.printSchema()
            print("\n--- Пример строк: content (первые 5) ---")
            raw_content.show(5, truncate=False)

            aud_nulls = (
                raw_audition.select(
                    [
                        F.count(F.when(F.col(c).isNull(), c)).alias(c)
                        for c in raw_audition.columns
                    ]
                )
                .collect()[0]
                .asDict()
            )
            logger.info(
                "audition: пропуски по колонкам: %s",
                {k: v for k, v in aud_nulls.items() if v > 0},
            )

            con_nulls = (
                raw_content.select(
                    [
                        F.count(F.when(F.col(c).isNull(), c)).alias(c)
                        for c in raw_content.columns
                    ]
                )
                .collect()[0]
                .asDict()
            )
            logger.info(
                "content: пропуски по колонкам: %s",
                {k: v for k, v in con_nulls.items() if v > 0},
            )

        # очистка
        clean_aud = clean_auditions(raw_audition, verbose=args.verbose)
        clean_con = clean_contents(raw_content, verbose=args.verbose)

        # джойн
        joined_df = join_and_parse_topics(clean_aud, clean_con, verbose=args.verbose)

        if args.verbose:
            dup_sessions = (
                joined_df.groupBy("audition_id")
                .count()
                .filter(F.col("count") > 1)
                .count()
            )
            logger.info(
                "Проверка дубликатов после джойна: дубликатов по audition_id = %d",
                dup_sessions,
            )

        # фильтрация если переданы параметры
        if args.start_date or args.end_date or args.geo_id:
            logger.info(
                "Фильтруем: start_date=%s, end_date=%s, geo_id=%s",
                args.start_date,
                args.end_date,
                args.geo_id,
            )
            joined_df = filter_dataset(
                joined_df,
                start_date=args.start_date,
                end_date=args.end_date,
                geo_id=args.geo_id,
            )
            if args.verbose:
                logger.info("Осталось строк после фильтра: %d", joined_df.count())

        # аналитика если запросили
        if args.columns:
            show_column_distributions(joined_df, args.columns)
        if args.show_analytics:
            show_analytics(joined_df)

        # строим 5 витрин
        marts = make_all_marts(joined_df, verbose=args.verbose)

        # вывод плана выполнения
        if args.show_plan:
            print("\n" + "-" * 50)
            print("ПЛАН ВЫПОЛНЕНИЯ USER_MART (EXPLAIN EXTENDED)")
            print("-" * 50)
            marts["user_mart"].explain(mode="extended")

        # запись результата
        if args.output_path:
            logger.info("Сохранение 5 витрин в %s", args.output_path)
            save_marts(marts, args.output_path)
            logger.info("Все витрины записаны")

    except Exception as exc:
        logger.exception("Ошибка при работе пайплайна: %s", exc)
        sys.exit(1)
    finally:
        logger.info("Останавливаем Spark-сессию")
        spark.stop()
        logger.info("Готово")


if __name__ == "__main__":
    main()
