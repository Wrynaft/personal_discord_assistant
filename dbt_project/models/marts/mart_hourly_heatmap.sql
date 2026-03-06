-- Mart: Hourly Activity Heatmap
-- Message count by day-of-week and hour
-- Used for: activity heatmap visualization in Superset

SELECT
    m.guild_id,
    g.guild_name,
    m.day_of_week,
    CASE m.day_of_week
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_name,
    m.message_hour,
    COUNT(*) AS message_count
FROM {{ ref('stg_messages') }} m
LEFT JOIN {{ source('raw', 'dim_guilds') }} g ON m.guild_id = g.guild_id
GROUP BY m.guild_id, g.guild_name, m.day_of_week, m.message_hour
ORDER BY m.day_of_week, m.message_hour
