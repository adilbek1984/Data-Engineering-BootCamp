-- Aggregating data from two tables: games and game_details
CREATE TABLE game_points_dashboard AS
WITH games_augmented AS (
    SELECT player_name,
           team_abbreviation,
           team_city,
           COALESCE(pts, 0) AS pts,
           COALESCE(g.season, 0) AS season,
           CASE
              WHEN gd.team_id = g.home_team_id AND home_team_wins = 1 THEN gd.game_id
              WHEN gd.team_id = g.visitor_team_id AND home_team_wins = 0 THEN gd.game_id
           END AS game_id_won
    FROM game_details gd
             JOIN games g on gd.game_id = g.game_id
)
SELECT
       CASE
           WHEN GROUPING(player_name) = 0
                    AND GROUPING(team_city) = 0
               THEN 'player_name__team_city'
           WHEN GROUPING(player_name) = 0
                    AND GROUPING(season) = 0
               THEN 'player_name__season'
           WHEN GROUPING(team_city) = 0
                    AND GROUPING(team_abbreviation) = 0
               THEN 'team_city__abbreviation'
       END as aggregation_level,
       COALESCE(player_name, '(overall)') as player_name,
       COALESCE(team_abbreviation, '(overall)') as team_abbreviation,
       COALESCE(team_city, '(overall)') as team_city,
       season,
       SUM(pts) AS total_points,
       COUNT(DISTINCT game_id_won) AS total_wins
FROM games_augmented
GROUP BY GROUPING SETS (
	(player_name, team_city),
	(player_name, season),
	(team_city, team_abbreviation)
);

-- DROP TABLE game_points_dashboard;

SELECT * FROM game_points_dashboard;

-- Q1: Who scored the most points playing for one team?
SELECT
    aggregation_level,
    player_name,
    team_city,
    total_points
FROM game_points_dashboard
WHERE aggregation_level = 'player_name__team_city'
ORDER BY total_points DESC
LIMIT 1;

-- Q2: Who scored the most points in one season?
SELECT
    aggregation_level,
    player_name,
    season,
    total_points
FROM game_points_dashboard
WHERE aggregation_level = 'player_name__season'
ORDER BY total_points DESC
LIMIT 1;

-- Q3: Which team has won the most games?
SELECT
    aggregation_level,
    team_abbreviation,
    team_city,
    total_wins
FROM game_points_dashboard
WHERE aggregation_level = 'team_city__abbreviation'
ORDER BY total_wins DESC
LIMIT 1;
