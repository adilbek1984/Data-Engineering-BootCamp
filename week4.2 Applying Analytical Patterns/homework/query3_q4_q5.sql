-- Q4: What is the most games a team has won in a 90 game stretch?
WITH team_results AS (
    SELECT
        gd.game_id,
        gd.team_id,
        gd.team_abbreviation,
        g.game_date_est,
        CASE
            WHEN gd.team_id = g.home_team_id AND g.home_team_wins = 1 THEN 1
            WHEN gd.team_id = g.visitor_team_id AND g.home_team_wins = 0 THEN 1
            ELSE 0
        END AS game_wins
    FROM game_details gd
    JOIN games g ON gd.game_id = g.game_id
),
team_games AS (
    SELECT
        game_id,
        team_id,
        team_abbreviation,
        game_date_est,
        game_wins,
        ROW_NUMBER() OVER
            (PARTITION BY team_id
            ORDER BY game_date_est) AS game_number
    FROM team_results
    GROUP BY game_id, team_id, team_abbreviation, game_date_est, game_wins
),
rolling_wins AS (
    SELECT
        t1.team_id,
        t1.team_abbreviation,
        t1.game_number,
        SUM(t2.game_wins) AS wins_in_90_games
    FROM
        team_games t1
    JOIN
        team_games t2
    ON
        t1.team_id = t2.team_id
        AND t2.game_number BETWEEN t1.game_number AND t1.game_number + 89
    GROUP BY
        t1.team_id, t1.team_abbreviation, t1.game_number
)
SELECT
    team_id,
    team_abbreviation,
    MAX(wins_in_90_games) AS max_wins_in_90_games
FROM
    rolling_wins
GROUP BY
    team_id, team_abbreviation
ORDER BY
    max_wins_in_90_games DESC
LIMIT 1;

-- Q5: How many games in a row did LeBron James score over 10 points a game?
WITH starter AS (
SELECT g.game_date_est,
       player_name,
       team_id,
       team_abbreviation,
       pts,
           CASE
              WHEN pts > 10 THEN 1
              ELSE 0
           END AS scored_over_10
    FROM game_details gd
             JOIN games g on gd.game_id = g.game_id
    WHERE player_name = 'LeBron James'
),
lagged AS (
SELECT
    *,
    LAG(scored_over_10) OVER (PARTITION BY player_name ORDER BY game_date_est) AS scored_over_10_before
FROM starter
),
streak_change AS (
SELECT
    *,
    CASE WHEN scored_over_10 <> scored_over_10_before THEN 1 ELSE 0 END as streak_changed
FROM lagged
),
streak_identified AS (
    SELECT *,
           SUM(streak_changed) OVER (PARTITION BY player_name ORDER BY game_date_est) AS streak_identifier
    FROM streak_change
),
record_counts AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY player_name, streak_identifier ORDER BY game_date_est) AS streak_length
    FROM streak_identified
    WHERE scored_over_10 = 1 -- учитывать только игры с более чем 10 очками
    ),
ranked AS (
SELECT *,
       MAX(streak_length) OVER (PARTITION BY player_name, streak_identifier) AS max_streak_length
FROM record_counts
)
SELECT DISTINCT
    player_name,
    max_streak_length AS longest_streak
FROM ranked
ORDER BY longest_streak DESC
LIMIT 1;
