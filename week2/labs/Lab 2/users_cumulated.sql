INSERT INTO users_cumulated
WITH yesterday AS (
    SELECT
        *
    FROM users_cumulated
    WHERE date = DATE('2023-01-30')
),
    today AS (
    SELECT
        CAST(user_id AS TEXT) AS user_id,
        --DATE(:CAST(event_time AS TIMESTAMP)) AS date_active
        DATE(CAST(event_time AS TIMESTAMP)) AS date_active
    FROM events
    WHERE
        --DATE(:CAST(event_time AS TIMESTAMP)) = DATE(:'2023-01-01')
        DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
        AND user_id IS NOT NULL
        GROUP BY user_id, DATE(CAST(event_time AS TIMESTAMP))
        --GROUP BY user_id, DATE(:CAST(event_time AS TIMESTAMP))
    )

SELECT
    COALESCE(t.user_id, y.user_id) AS user_id,
    CASE
        WHEN y.dates_active IS NULL
        THEN ARRAY[t.date_active]
        WHEN t.date_active IS NULL THEN y.dates_active
        ELSE ARRAY[t.date_active] || y.dates_active
    END
    AS dates_active,
    COALESCE(t.date_active, y.date + Interval '1 day') AS date
FROM today t
    FULL OUTER JOIN yesterday y
    ON t.user_id = y.user_id;

-- DROP TABLE users_cumulated;
-- CREATE TABLE users_cumulated (
-- user_id TEXT,
-- -- The list of dates in the past where the user was active
-- dates_active DATE[],
-- -- The current date for user
-- date DATE,
-- PRIMARY KEY (user_id, date)
-- );
select * from users_cumulated;


SELECT * FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day');
