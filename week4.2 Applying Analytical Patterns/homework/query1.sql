-- CREATE TABLE players_growth_accounting (
--     player_name TEXT,
--     first_active_year INTEGER,
--     last_active_year INTEGER,
--     active_state TEXT,
--     years_active INTEGER[],
--     current_season INTEGER,
--     PRIMARY KEY (player_name, current_season)
-- );

INSERT INTO players_growth_accounting
WITH previous_season AS (
    SELECT * FROM players_growth_accounting
             WHERE current_season = 2004
    ),
    current_season AS (
        SELECT
            player_name,
            season,
            COUNT(1)
        FROM player_seasons
        WHERE season = 2005
            AND player_name IS NOT NULL
        GROUP BY player_name, season
    )

    SELECT
    COALESCE(cs.player_name, ps.player_name) AS player_name,
    COALESCE(ps.first_active_year, cs.season) AS first_active_year,
    COALESCE(cs.season, ps.last_active_year) AS last_active_year,
    CASE
        WHEN ps.player_name IS NULL AND cs.player_name IS NOT NULL THEN 'New'
        WHEN ps.player_name IS NOT NULL AND cs.player_name IS NULL THEN 'Retired'
        WHEN ps.player_name IS NOT NULL AND cs.player_name IS NOT NULL THEN 'Continued Playing'
        WHEN ps.player_name IS NOT NULL AND ps.last_active_year < cs.season - 1 THEN 'Returned from Retirement'
        WHEN ps.player_name IS NOT NULL AND cs.player_name IS NULL AND ps.last_active_year < ps.current_season THEN 'Stayed Retired'
        ELSE 'Unknown'
    END as active_state,
    COALESCE(ps.years_active, ARRAY[]::INTEGER[])
        || CASE WHEN
            cs.player_name IS NOT NULL
            THEN ARRAY[cs.season]
            ELSE ARRAY[]::INTEGER[]
        END AS years_active,
    COALESCE(cs.season, ps.current_season + 1) AS current_season
    FROM current_season cs
    FULL OUTER JOIN previous_season ps
    ON cs.player_name = ps.player_name;


SELECT * FROM players_growth_accounting
         ORDER BY current_season DESC