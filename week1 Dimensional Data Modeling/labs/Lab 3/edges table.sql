-- DROP TABLE edges;

-- CREATE TABLE edges (
--     subject_identifier TEXT,
--     subject_type vertex_type,
--     object_identifier TEXT,
--     object_type vertex_type,
--     edge_type edge_type,
--     properties JSON,
--     PRIMARY KEY (subject_identifier, subject_type, object_identifier, object_type, edge_type)
-- );


-- INSERT INTO edges
-- WITH deduped AS (
--     SELECT *, row_number() over (PARTITION BY player_id, game_id) AS row_num
--     FROM game_details
-- )
-- SELECT
--     player_id AS subject_identifier,
--     'player':: vertex_type AS subject_type,
--     game_id AS object_identifier,
--     'game':: vertex_type AS object_type,
--     'plays_in':: edge_type AS edge_type,
--     json_build_object(
--         'start_position', start_position,
--         'pts', pts,
--         'team_id', team_id,
--         'team_abbreviation', team_abbreviation
--     ) AS properties
-- FROM deduped
-- WHERE row_num = 1;

SELECT * FROM edges;

-- Отображение информации об игроках и их командах, которые играют друг с другом в одной команде и против друг друга за разные команды в рамках одной игры (game_id)
INSERT INTO edges
WITH deduped AS (
    SELECT *, row_number() over (PARTITION BY player_id, game_id) AS row_num
    FROM game_details
),
    filtered AS (
        SELECT * FROM deduped
        WHERE row_num = 1
    ),

    aggregated AS (
        SELECT
    f1.player_id AS subject_player_id,
    f2.player_id AS object_player_id,
    CASE WHEN f1.team_abbreviation = f2.team_abbreviation
        THEN 'shares_team':: edge_type
        ELSE 'plays_against':: edge_type
    END as edge_type,
    MAX(f1.player_name) AS subject_player_name,
    MAX(f2.player_name) AS object_player_name,
    COUNT(1) AS num_games,
    SUM(f1.pts) AS subject_points,
    SUM(f2.pts) AS object_points
 FROM filtered f1
    JOIN filtered f2
        ON f1.game_id = f2.game_id
        AND f1.player_name <> f2.player_name
WHERE f1.player_id > f2.player_id
GROUP BY
    f1.player_id,
    f2.player_id,
    CASE WHEN f1.team_abbreviation = f2.team_abbreviation
        THEN 'shares_team':: edge_type
        ELSE 'plays_against':: edge_type
    END
    )
SELECT
        subject_player_id AS subject_identifier,
        'player':: vertex_type AS subject_type,
        object_player_id AS object_identifier,
        'player':: vertex_type AS object_type,
        edge_type AS edge_type,
        json_build_object(
	    'num_games', num_games,
	    'subject_points', subject_points,
	    'object_points', object_points
        )
    FROM aggregated;

SELECT * FROM edges;

--сравнительный анализ игроков

SELECT
    v.properties->>'player_name',
    e.object_identifier,
    CAST(v.properties->>'number_of_games' as REAL)/
    CASE WHEN CAST(v.properties ->> 'total_points' as REAL) = 0 THEN 1 ELSE
        CAST(v.properties ->> 'total_points' as REAL)
    END,
    e.properties->>'subject_points',
    e.properties->>'num_games'
FROM vertices v JOIN edges e
    ON v.identifier = e.subject_identifier
    AND v.type = e.subject_type
WHERE e.object_type = 'player'::vertex_type;
