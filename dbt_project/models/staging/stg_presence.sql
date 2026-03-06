-- Staging: Presence / Activity
-- Cleans and standardizes raw presence events

SELECT
    id,
    user_id,
    activity_type,
    activity_name,
    guild_id,
    created_at,
    DATE(created_at) AS activity_date
FROM {{ source('raw', 'fact_presence') }}
WHERE activity_name IS NOT NULL
