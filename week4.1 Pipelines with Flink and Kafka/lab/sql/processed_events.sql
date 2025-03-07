-- Create processed_events table
-- CREATE TABLE IF NOT EXISTS processed_events (
--     ip VARCHAR,
--     event_timestamp TIMESTAMP(3),
--     referrer VARCHAR,
--     host VARCHAR,
--     url VARCHAR,
--     geodata VARCHAR
-- );

-- CREATE TABLE processed_events_aggregated (
--     event_hour TIMESTAMP(3),
--     host VARCHAR,
--     num_hits BIGINT
-- );
--
-- CREATE TABLE processed_events_aggregated_source (
--     event_hour TIMESTAMP(3),
--     host VARCHAR,
--     referrer VARCHAR,
--     num_hits BIGINT
-- );


SELECT * FROM processed_events WHERE geodata::jsonb ->> 'country' = 'FR';

SELECT * FROM processed_events_aggregated WHERE host IN ('zachwilson.techcreator.io', 'lulu.techcreator.io',
    'dutchengineer.techcreator.io')
LIMIT 100;

SELECT * FROM processed_events_aggregated_source;

SELECT * FROM processed_events_aggregated_source WHERE host IN ('zachwilson.techcreator.io', 'lulu.techcreator.io',
    'dutchengineer.techcreator.io')
LIMIT 100;