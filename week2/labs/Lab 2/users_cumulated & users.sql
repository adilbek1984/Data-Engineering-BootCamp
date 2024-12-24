WITH users AS (
    SELECT * FROM users_cumulated
    WHERE date = '2023-01-31'
),
    series AS (
        SELECT *
	    FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') as series_date
    ),
    place_holder_ints AS (
                          SELECT CASE
                                     WHEN
                                         dates_active @> ARRAY [DATE(series_date)]
                                         THEN
                                         CAST(POW(2, 32 - (date - DATE(series_date))) AS BIGINT)
                                         ELSE 0
                                     END as placeholder_int_value,
                                     *
                          FROM users
                                   CROSS JOIN series
--                          WHERE user_id = '439578290726747300'
    )
SELECT
    user_id,
    CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)),
    BIT_COUNT(CAST(CAST (SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0
			AS dim_is_monthly_active,
	BIT_COUNT(CAST ('11111110000000000000000000000000' AS BIT(32)) &  --bitwise AND
	CAST(CAST(CAST (SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS BIT(32))) > 0
			AS dim_is_weekly_active,
	BIT_COUNT(CAST ('10000000000000000000000000000000' AS BIT(32)) &  --bitwise AND
	CAST(CAST(CAST (SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS BIT(32))) > 0
			AS dim_is_daily_active
FROM place_holder_ints
GROUP BY user_id;








