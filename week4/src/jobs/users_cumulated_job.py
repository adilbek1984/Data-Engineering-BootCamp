from pyspark.sql import SparkSession

query = """

WITH yesterday AS (
    SELECT
        user_id,
        -- Преобразуем строковое значение в массив дат
        CASE 
            WHEN dates_active IS NOT NULL THEN CAST(dates_active AS ARRAY<DATE>)
            ELSE ARRAY() 
        END AS dates_active,
        date
    FROM users_cumulated
    WHERE date = DATE('2023-01-30')
),
today AS (
    SELECT
        CAST(user_id AS STRING) AS user_id,
        DATE(CAST(event_time AS TIMESTAMP)) AS date_active
    FROM events
    WHERE
        DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
        AND user_id IS NOT NULL
)
SELECT
    COALESCE(t.user_id, y.user_id) AS user_id,
    CASE
        WHEN ISNULL(y.dates_active) THEN ARRAY(CAST(t.date_active AS DATE))  
        WHEN ISNULL(t.date_active) THEN y.dates_active 
        ELSE ARRAY_UNION(y.dates_active, ARRAY(CAST(t.date_active AS DATE)))   
    END AS dates_active,
    COALESCE(t.date_active, DATE_ADD(y.date, 1)) AS date
FROM today t
    FULL OUTER JOIN yesterday y
    ON t.user_id = y.user_id;
"""

# Registering the events table in advance
# events.createOrReplaceTempView("events")

def do_users_cumulated_transformation(spark, dataframe):
    #print("Input DataFrame:") #потом удалю этот код
    #dataframe.show()
    dataframe.createOrReplaceTempView("users_cumulated")
    return spark.sql(query) #надо раскомментить потом
    #result_dataframe = spark.sql(query)
    #print("Result DataFrame:") #потом удалю этот код
    #result_dataframe.show() #потом удалю этот код

    #return result_dataframe


def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("users_cumulated") \
        .getOrCreate()
    output_df = do_users_cumulated_transformation(spark, spark.table("users_cumulated"))
    output_df.write.mode("overwrite").insertInto("users_cumulated")