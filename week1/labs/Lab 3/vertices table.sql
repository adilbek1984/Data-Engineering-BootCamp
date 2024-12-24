INSERT INTO vertices
SELECT
    game_id AS identifier,
    'game':: vertex_type AS type,
    json_build_object(
            'pts_home', pts_home,
            'pts_away', pts_away,
            'winning_team', CASE WHEN home_team_wins = 1 THEN home_team_id ELSE visitor_team_id END
    ) AS properties
    FROM games;


select * from games;

select * from vertices;

select * from teams;

WITH players_agg AS (
        SELECT
            player_id                   AS identifier,
            MAX(player_name)            AS player_name,
            COUNT(1)                    AS number_of_games,
            SUM(pts)                    AS total_points,
            ARRAY_AGG(DISTINCT team_id) AS teams
        FROM game_details
        GROUP BY player_id
        )
SELECT identifier, 'player':: vertex_type,
    json_build_object(
            'player_name', player_name,
            'number_of_games', number_of_games,
            'total_points', total_points,
            'teams', teams
    )
FROM players_agg;

INSERT INTO vertices
WITH teams_deduped AS (
	SELECT *, ROW_NUMBER() OVER(PARTITION BY team_id) AS row_num
	FROM teams
)
SELECT
        team_id                AS identifier,
        'team':: vertex_type   AS type,
        json_build_object(
            'abbreviation', abbreviation,
            'nickname', nickname,
            'city', city,
            'arena', arena,
            'year_founded', yearfounded
        )
FROM teams_deduped
WHERE row_num = 1;

select * from vertices;