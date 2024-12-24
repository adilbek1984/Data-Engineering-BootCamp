-- The incremental query to generate host_activity_datelist
INSERT INTO hosts_cumulated
WITH activity_dates AS (
    SELECT
        host,
        DATE(CAST(event_time AS TIMESTAMP)) AS activity_date -- Convert event_time to date
    FROM events
    WHERE event_time IS NOT NULL
)
SELECT
    host,
    ARRAY_AGG(DISTINCT activity_date ORDER BY activity_date) AS host_activity_datelist,
    MAX(activity_date) AS date
FROM activity_dates
GROUP BY host;