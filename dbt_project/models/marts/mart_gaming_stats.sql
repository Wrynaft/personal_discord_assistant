-- Mart: Gaming Stats
-- Game popularity and play frequency
-- Used for: "Most Played Games" chart in Superset

SELECT
    p.guild_id,
    p.activity_name AS game_name,
    COUNT(*) AS play_sessions,
    COUNT(DISTINCT p.user_id) AS unique_players,
    COUNT(DISTINCT p.activity_date) AS days_played,
    MIN(p.created_at) AS first_played,
    MAX(p.created_at) AS last_played
FROM {{ ref('stg_presence') }} p
WHERE p.activity_type = 'playing'
GROUP BY p.guild_id, p.activity_name
ORDER BY play_sessions DESC
