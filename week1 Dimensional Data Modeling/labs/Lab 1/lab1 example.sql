-- select * from player_seasons limit 5;
--
-- CREATE TYPE season_stats AS(
-- 	season INTEGER,
-- 	gp INTEGER,
-- 	pts REAL,
-- 	reb REAL,
-- 	ast REAL
-- 	);

-- CREATE TYPE scoring_class AS
--      ENUM ('bad', 'average', 'good', 'star');

-- CREATE TABLE players (
--      player_name TEXT,
--      height TEXT,
--      college TEXT,
--      country TEXT,
--      draft_year TEXT,
--      draft_round TEXT,
--      draft_number TEXT,
--      season_stats season_stats[],
--      scoring_class scoring_class,
--      years_since_last_season INTEGER,
--      current_season INTEGER,
--      PRIMARY KEY (player_name, current_season)
--  );

-- DROP TABLE players;

INSERT INTO players

-- WITH years AS (
--     SELECT * FROM generate_series(1996, 2022) as season
-- ),
--     p AS (
--         SELECT player_name, MIN(season) as first_season
--         FROM player_seasons
--         GROUP BY player_name
--     ),
--     players_and_seasons AS (
--         SELECT *
--         FROM p
--             JOIN years y ON p.first_season <= y.season
--     ),
--
--     windowed AS (
--         SELECT ps.player_name,
--                ps.season,
--                array_remove(ARRAY_AGG(
--                             CASE
--                                 WHEN p1.season IS NOT NULL THEN ...
--                                 ROW (p1.season, p1.gp, p1.pts, p1.reb, p1.ast)
--                             END)
--                OVER (PARTITION BY ps.player_name))
--     )


WITH yesterday AS (
    SELECT * FROM players
    WHERE current_season = 2021

), today AS (
     SELECT * FROM player_seasons
    WHERE season = 2022
)

SELECT
    COALESCE(t.player_name, y.player_name) AS player_name,
    COALESCE(t.height, y.height) AS height,
    COALESCE(t.college, y.college) AS college,
    COALESCE(t.country, y.country) AS country,
    COALESCE(t.draft_year, y.draft_year) AS draft_year,
    COALESCE(t.draft_round, y.draft_round) AS draft_round,
    COALESCE(t.draft_number, y.draft_number) AS draft_number,
    CASE WHEN y.season_stats IS NULL
	    THEN ARRAY[ROW(
	    t.season,
	    t.gp,
	    t.pts,
	    t.reb,
	    t.ast
        )::season_stats]
    WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[ROW(
        t.season,
	    t.gp,
	    t.pts,
	    t.reb,
	    t.ast
        )::season_stats]
    ELSE y.season_stats
    END as season_stats,
    CASE
        WHEN t.season IS NOT NULL THEN
            CASE
                WHEN t.pts > 20 THEN 'star'
                WHEN t.pts > 15 THEN 'good'
                WHEN t.pts > 10 THEN 'average'
                ELSE 'bad'
            END::scoring_class
            ELSE y.scoring_class
    END as scoring_class,
    CASE WHEN t.season IS NOT NULL THEN 0
        ELSE y.years_since_last_season + 1
    END as years_since_last_season,
    END as is_active,
    COALESCE(t.season, y.current_season + 1) as current_season

    FROM today t FULL OUTER JOIN yesterday y
        ON t.player_name = y.player_name;


SELECT
    player_name,
    (season_stats[CARDINALITY(season_stats)]::season_stats).pts/
    CASE
        WHEN (season_stats[1]::season_stats).pts = 0 THEN 1
        ELSE (season_stats[1]::season_stats).pts
    END

    FROM players WHERE current_season = 2001
    AND scoring_class = 'star';

select * from players order by years_since_last_season;


--Unnest raw
WITH unnested AS (
SELECT player_name,
       UNNEST(season_stats)::season_stats AS season_stats
FROM players
  WHERE current_season = 2001
      AND player_name = 'Michael Jordan'
)
SELECT player_name,
       (season_stats::season_stats).*
FROM unnested;
