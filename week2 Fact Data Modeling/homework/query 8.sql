--An incremental query that loads host_activity_reduced
INSERT INTO host_activity_reduced
WITH activity_data AS (
    SELECT
        DATE_TRUNC('month', CAST(event_time AS TIMESTAMP)) AS month,  -- month from event_time
        host,
        user_id
    FROM events
    WHERE event_time IS NOT NULL
),
host_months AS (
    -- Generate a list of all possible host-month combinations
    SELECT DISTINCT
        DATE_TRUNC('month', CAST(event_time AS TIMESTAMP)) AS month,
        host
    FROM events
    WHERE event_time IS NOT NULL
),
aggregated_data AS (
    SELECT
        hm.month,
        hm.host,
        COUNT(ad.user_id) AS hit_array,  -- Count total events for the month-host
        ARRAY_AGG(DISTINCT ad.user_id) AS unique_visitors -- Unique visitors for the month-host
    FROM host_months hm
    LEFT JOIN activity_data ad
        ON hm.month = ad.month AND hm.host = ad.host
    GROUP BY hm.month, hm.host
)
SELECT
    month,
    host,
    COALESCE(hit_array, 0) AS hit_array,  -- Default to 0 hits for missing data
    COALESCE(unique_visitors, ARRAY[]::BIGINT[]) AS unique_visitors -- Default to empty array for visitors
FROM aggregated_data;