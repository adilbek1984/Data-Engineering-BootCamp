-- CREATE TABLE users_growth_accounting (
--     user_id TEXT,
--     first_active_date DATE,
--     last_active_date DATE,
--     daily_active_state TEXT,
--     weekly_active_state TEXT,
--     dates_active DATE[],
--     date DATE,
--     PRIMARY KEY (user_id, date)
-- );


INSERT INTO users_growth_accounting
WITH yesterday AS (
    SELECT * FROM users_growth_accounting
             WHERE date = DATE('2023-01-10')
),
    today AS (
        SELECT
            CAST(user_id AS TEXT) AS user_id,
            DATE_TRUNC('day', event_time::timestamp) as today_date,
            COUNT(1)
        FROM events
        WHERE DATE_TRUNC('day', event_time::timestamp) = DATE('2023-01-11')
        AND user_id IS NOT NULL
        GROUP BY user_id, DATE_TRUNC('day', event_time::timestamp)
    )
SELECT
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(y.first_active_date, t.today_date) AS first_active_date,
    COALESCE(t.today_date, y.last_active_date) AS last_active_date,
    CASE
        WHEN y.user_id IS NULL AND t.user_id IS NOT NULL THEN 'New'
        WHEN y.last_active_date = t.today_date - Interval '1 day' THEN 'Retained'
        WHEN y.last_active_date < t.today_date - Interval '1 day' THEN 'Resurrected'
        WHEN t.today_date IS NULL AND y.last_active_date = y.date THEN 'Churned'
        ELSE 'Stale'
    END as daily_active_state,
    CASE
        WHEN y.user_id IS NULL AND t.user_id IS NOT NULL THEN 'New'
        WHEN y.last_active_date >= y.date - Interval '7 day' THEN 'Retained'
        WHEN y.last_active_date < t.today_date - Interval '7 day' THEN 'Resurrected'
        WHEN
            t.today_date IS NULL
                AND y.last_active_date = y.date - Interval '7 day' THEN 'Churned'
        ELSE 'Stale'
    END as weekly_active_state,
    COALESCE(y.dates_active,
        ARRAY[]::DATE[])
        || CASE WHEN
            t.user_id IS NOT NULL
            THEN ARRAY[t.today_date]
            ELSE ARRAY[]::DATE[]
        END AS date_list,
    COALESCE(t.today_date, y.date + Interval '1 day') AS date

    FROM today t
    FULL OUTER JOIN yesterday y
    ON t.user_id = y.user_id;


-- Примеры запросов
SELECT date, daily_active_state, COUNT(1)
FROM users_growth_accounting
GROUP BY date, daily_active_state
ORDER BY COUNT(1) DESC;

SELECT *  FROM users_growth_accounting
    WHERE date = DATE('2023-01-13')
      AND last_active_date = DATE('2023-01-01')


SELECT date,
       CAST(COUNT(CASE WHEN daily_active_state
                           IN ('Retained', 'Resurrected', 'New') THEN 1 END) AS REAL)/COUNT(1) as pct_active,
       COUNT(1)  FROM users_growth_accounting
    WHERE first_active_date = DATE('2023-01-01')
GROUP BY date;

-------
SELECT
       date - first_active_date AS days_since_first_active,
       CAST(COUNT(CASE WHEN daily_active_state
                           IN ('Retained', 'Resurrected', 'New') THEN 1 END) AS REAL)/COUNT(1) as pct_active,
       COUNT(1)  FROM users_growth_accounting
GROUP BY date - first_active_date;