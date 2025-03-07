INSERT INTO actors_history_scd (
    actor,
    actorid,
    quality_class,
    is_active,
    start_date,
    end_date
)
SELECT
    actor,
    actorid,
    quality_class,
    is_active,
    -- Use a default start date for all records being backfilled
    '1998-01-01'::DATE AS start_date,
    -- Set the end_date to a large future date, indicating the record is current
    '9999-12-31'::DATE AS end_date
FROM actors;