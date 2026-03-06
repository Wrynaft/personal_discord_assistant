-- Staging: Voice Events
-- Cleans and standardizes raw voice events

SELECT
    id,
    user_id,
    channel_id,
    guild_id,
    event_type,
    created_at,
    DATE(created_at) AS event_date,
    EXTRACT(HOUR FROM created_at) AS event_hour
FROM {{ source('raw', 'fact_voice_events') }}
