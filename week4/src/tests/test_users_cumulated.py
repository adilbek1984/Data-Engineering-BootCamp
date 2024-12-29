from chispa.dataframe_comparer import *

from ..jobs.users_cumulated_job import do_users_cumulated_transformation
from collections import namedtuple
from pyspark.sql import functions as F

UsersCumulated = namedtuple('Users_cumulated', "user_id dates_active date")
Users = namedtuple('Users', "user_id dates_active date")


def test_users_cumulated_generation(spark):
    input_data = [
        Users("444502572952128450", ["2023-01-30"], "2023-01-30")
    ]

    input_dataframe = spark.createDataFrame(input_data)

    events_df = spark.createDataFrame([
        ("444502572952128450", "2023-01-31")], schema=["user_id", "event_time"])

    # Registering the temporary events table
    events_df.createOrReplaceTempView("events")

    actual_df = do_users_cumulated_transformation(spark, input_dataframe)
    # Check if actual_df has data
    actual_df.show()

    expected_output = [
        UsersCumulated(
            user_id="444502572952128450",
            dates_active=["2023-01-30", "2023-01-31"],
            date="2023-01-31"
        )
    ]

    expected_df = spark.createDataFrame(expected_output)

    expected_df = expected_df.withColumn("dates_active", F.col("dates_active").cast("array<date>")) \
        .withColumn("date", F.col("date").cast("date"))

    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
