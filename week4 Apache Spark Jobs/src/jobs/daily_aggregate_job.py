from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, FloatType
from pyspark.sql import functions as F

# Определение функции для объединения массивов
def concat_arrays(arr1, arr2):
    return (arr1 or []) + (arr2 or [])

query = """

WITH daily_aggregate AS
    (SELECT user_id,
            DATE(event_time) AS date,
            COUNT(1)         AS num_site_hits
    FROM events
       WHERE DATE(event_time) = DATE('2023-01-03')
           AND user_id IS NOT NULL
       GROUP BY user_id, DATE(event_time))
SELECT
        user_id,
        DATE_TRUNC('month', date) AS month_start,
        'site_hits' AS metric_name,
        num_site_hits AS num_hits,
        EXTRACT(DAY FROM date) AS day_of_month
    FROM daily_aggregate;
"""

def do_daily_aggregate_transformation(spark, dataframe):
    # Регистрация UDF для объединения массивов
    concat_arrays_udf = F.udf(concat_arrays, ArrayType(FloatType()))

    #print("Input DataFrame:") #потом удалю этот код
    #dataframe.show()

    dataframe.createOrReplaceTempView("daily_aggregate")
    aggregated_df = spark.sql(query)

    # Приведение типа month_start в DATE
    aggregated_df = aggregated_df.withColumn("month_start", F.col("month_start").cast("date"))

    # Использование UDF для объединения массивов
    aggregated_df = aggregated_df.withColumn(
        "metric_array",
        concat_arrays_udf(
            F.array_repeat(F.lit(0.0), F.col("day_of_month") - 1),
            F.array(F.col("num_hits").cast(FloatType()))  # Приведение num_hits к FLOAT
        )
    )

    # Удаление ненужных колонок
    return aggregated_df.drop("num_hits", "day_of_month")


def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("daily_aggregate") .getOrCreate() \
        .getOrCreate()
    output_df = do_daily_aggregate_transformation(spark, spark.table("daily_aggregate"))
    output_df.write.mode("overwrite").insertInto("daily_aggregate")