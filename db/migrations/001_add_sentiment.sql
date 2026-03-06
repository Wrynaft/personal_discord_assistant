-- Add sentiment_score AND content_preview columns to fact_messages
-- Run on VPS:
-- docker exec -it discord_postgres psql -U discord -d discord_analytics -c "ALTER TABLE fact_messages ADD COLUMN IF NOT EXISTS sentiment_score SMALLINT; ALTER TABLE fact_messages ADD COLUMN IF NOT EXISTS content_preview VARCHAR(200);"

ALTER TABLE fact_messages ADD COLUMN IF NOT EXISTS sentiment_score SMALLINT;
ALTER TABLE fact_messages ADD COLUMN IF NOT EXISTS content_preview VARCHAR(200);

CREATE INDEX IF NOT EXISTS idx_messages_sentiment ON fact_messages(sentiment_score);
