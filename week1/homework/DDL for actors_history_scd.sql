-- CREATE TYPE quality_class AS
--     ENUM ('star', 'good', 'average', 'bad');

-- CREATE TYPE is_active AS ENUM ('true', 'false');


CREATE TABLE actors_history_scd (
    actorid TEXT NOT NULL,
    actor TEXT NOT NULL,
    quality_class TEXT NOT NULL,
    is_active BOOLEAN NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE,
    PRIMARY KEY (actorid, start_date)
);

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
    '1998-01-01'::DATE AS start_date,
    '9999-12-31'::DATE AS end_date
FROM actors;