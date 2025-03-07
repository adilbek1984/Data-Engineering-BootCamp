--A cumulative query to generate device_activity_datelist from events
INSERT INTO user_devices_cumulated
WITH yesterday AS (
    SELECT
        *
    FROM user_devices_cumulated
    WHERE date = DATE('2023-01-05')
),
    today AS (
    SELECT
        CAST(user_id AS TEXT) AS user_id,
        device_id,
        DATE(CAST(event_time AS TIMESTAMP)) AS date_active
    FROM events
    WHERE
        DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-06')
        AND user_id IS NOT NULL
        GROUP BY user_id, device_id, DATE(CAST(event_time AS TIMESTAMP))
    ),
deduplicated AS
    (SELECT COALESCE(t.user_id, y.user_id) AS user_id,
                    t.device_id,
                    d.browser_type,
                    CASE
                        WHEN y.device_activity_datelist IS NULL
                            THEN ARRAY [t.date_active]
                        WHEN t.date_active IS NULL THEN y.device_activity_datelist
                        ELSE ARRAY [t.date_active] || y.device_activity_datelist
                    END AS device_activity_datelist,
                    COALESCE(t.date_active, y.date + Interval '1 day') AS date,
                    ROW_NUMBER() OVER (PARTITION BY COALESCE(t.user_id, y.user_id),
                        t.device_id, d.browser_type,
                        COALESCE(t.date_active, y.date + INTERVAL '1 day') ORDER BY t.date_active) AS row_num
                 FROM today t
                          FULL OUTER JOIN yesterday y
                                          ON t.user_id = y.user_id
                          JOIN devices d
                               ON t.device_id = d.device_id
                )
SELECT user_id, device_id, browser_type, device_activity_datelist, date
FROM deduplicated
WHERE row_num = 1
ON CONFLICT (user_id, device_id, browser_type, date)
DO UPDATE SET
    device_activity_datelist = EXCLUDED.device_activity_datelist;