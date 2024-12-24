-- DROP TABLE players_scd;

-- CREATE TABLE players_scd (
-- player_name TEXT,
-- scoring_class scoring_class,
-- is_active BOOLEAN,
-- start_season INTEGER,
-- end_season INTEGER,
-- current_season INTEGER,
-- PRIMARY KEY(player_name, start_season)
-- );

INSERT INTO players_scd
WITH with_previous AS (
    SELECT
        player_name,
        current_season,
        scoring_class,
        is_active,
        LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) as previous_scoring_class,
        LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) as previous_is_active
    FROM players
    WHERE current_season <= 2021
    ),

    with_indicators AS (
    SELECT *,
        CASE
            WHEN scoring_class <> previous_scoring_class THEN 1
            WHEN is_active <> previous_is_active THEN 1
            ELSE 0
        END AS  change_indicator
    FROM with_previous

    ),
    with_streaks AS (
    SELECT *, SUM(change_indicator) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
    FROM with_indicators
    )

    SELECT player_name,
           scoring_class,
           --streak_identifier,
           is_active,
           MIN(current_season) as start_season,
           MAX(current_season) as end_season,
           2021 AS current_season
    FROM with_streaks
    GROUP BY player_name, streak_identifier, is_active, scoring_class
    ORDER BY player_name, streak_identifier;

select * from players_scd;