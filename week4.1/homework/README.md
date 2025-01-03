Testing Instructions
1. Prerequisites
PostgreSQL installed and running on your system.
Access to the Flink job that writes sessionized data to the sessionized_events table.
SQL client tool installed (e.g., psql, pgAdmin, or a similar database management tool).

2. Setting Up PostgreSQL
Create a Database:

sql

CREATE DATABASE web_traffic_analysis;
Connect to the Database:

bash

psql -U postgres -d web_traffic_analysis
Create the sessionized_events Table: Execute the following SQL script to set up the table:

sql

-- Create the sessionized_events table
CREATE TABLE sessionized_events (
    ip VARCHAR,
    host VARCHAR,
    session_start TIMESTAMP(3),
    session_end TIMESTAMP(3),
    url_list TEXT
);

-- Add an index on the host column for optimized queries
CREATE INDEX idx_host ON sessionized_events (host);

3. Loading Data
Run the Flink Job: Ensure the Flink job is configured to write sessionized data to the sessionized_events table. Use the job script provided earlier to execute the sessionization pipeline.

Verify Data Insertion: Once the Flink job completes, check if data has been inserted:

sql

SELECT * FROM sessionized_events LIMIT 10;

4. Executing the SQL Script
Run the analysis queries to verify the expected outcomes:

Query 1: Average Number of Web Events for Tech Creator

sql

SELECT
    host,
    AVG(array_length(string_to_array(url_list, ','), 1)) AS avg_events_per_session
FROM sessionized_events
WHERE host LIKE '%techcreator.io' -- Filtering by Tech Creator host
GROUP BY host;
Query 2: Compare Results Between Hosts

sql

SELECT
    host,
    AVG(array_length(string_to_array(url_list, ','), 1)) AS avg_events_per_session
FROM sessionized_events
WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
GROUP BY host
ORDER BY avg_events_per_session DESC;

5. Verifying Results
Confirm that results align with your expectations:

Query 1 should return the average web events per session for Tech Creator.
Query 2 should compare averages for the specified hosts, ordered in descending order.
Use EXPLAIN to validate query performance and index utilization:

sql

EXPLAIN ANALYZE
SELECT
    host,
    AVG(array_length(string_to_array(url_list, ','), 1)) AS avg_events_per_session
FROM sessionized_events
WHERE host LIKE '%techcreator.io'
GROUP BY host;

6. Troubleshooting
No Data in Table: Verify the Flink job ran successfully and the JDBC sink settings are correct.
Slow Queries: Check if the idx_host index exists and is being used (verify with EXPLAIN).
Environment Issues: Ensure PostgreSQL service is running, and all required credentials are correct.