--A query to deduplicate game_details
WITH deduped AS (
    SELECT
        gd.*,
        ROW_NUMBER() OVER(PARTITION BY gd.game_id, gd.team_id, gd.player_id ORDER BY gd.game_id) as row_num
    FROM game_details gd
   )
SELECT * FROM deduped
WHERE row_num = 1;