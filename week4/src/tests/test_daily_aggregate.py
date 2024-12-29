from chispa.dataframe_comparer import *
from pyspark.sql.types import ArrayType, FloatType, StringType, StructField, StructType, DateType
from chispa.dataframe_comparer import assert_df_equality
from ..jobs.daily_aggregate_job import do_daily_aggregate_transformation
from datetime import datetime
from collections import namedtuple

EventsAgg = namedtuple('EventsAgg', "user_id month_start metric_name metric_array")
Events = namedtuple('Events', "user_id month_start metric_name metric_array")


def test_daily_aggregate_generation(spark):
    input_data = [
        Events("444502572952128450", "2023-01-03", "site_hits", [1.0])
    ]

    input_dataframe = spark.createDataFrame(input_data)

    events_df = spark.createDataFrame([
        ("444502572952128450", "2023-01-03")], schema=["user_id", "event_time"])

    # Registering the temporary events table
    events_df.createOrReplaceTempView("events")

    actual_df = do_daily_aggregate_transformation(spark, input_dataframe)

    # Check if actual_df has data
    actual_df.show()

    expected_output = [
        EventsAgg(
            user_id="444502572952128450",
            #month_start="2023-01-01",
            month_start=datetime.strptime("2023-01-01", "%Y-%m-%d").date(),
            metric_name="site_hits",
            metric_array=[0.0, 0.0, 1.0]
        )
    ]

    #expected_df = spark.createDataFrame(expected_output)
    # Убедитесь, что в expected_df тип данных для month_start - DateType, а для metric_array - ArrayType(FloatType())
    expected_df = spark.createDataFrame(expected_output, schema=StructType([
        StructField('user_id', StringType(), True),
        StructField('month_start', DateType(), True),
        StructField('metric_name', StringType(), False),
        StructField('metric_array', ArrayType(FloatType(), True), True)
    ]))
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
