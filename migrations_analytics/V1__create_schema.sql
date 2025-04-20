-- Dimension Time 
CREATE TABLE IF NOT EXISTS time_dimension (
    timestamp TIMESTAMP, 
    year UInt32, 
    month UInt32, 
    day UInt32, 
    weekday UInt32, 
)
ENGINE = MergeTree()
PRIMARY KEY (timestamp);

-- Dim Campaign
CREATE TABLE IF NOT EXISTS campaign_dimension (
    bid Float32,
    budget Float32,
    campaign_id Int32
) 
ENGINE = MergeTree()
ORDER BY (campaign_id)

-- Fact Impressions
CREATE TABLE IF NOT EXISTS fact_impressions (
    advertiser_id Int32,
    campaign_id Int32,
    timestamp DateTime,
    bid Float32,
    budget Float32
)
ENGINE = MergeTree()
PRIMARY KEY (timestamp);

-- Fact Clicks
CREATE TABLE IF NOT EXISTS fact_clicks (
    campaign_id UInt32,
    advertiser_id UInt32,
    timestamp TIMESTAMP
)
ENGINE = MergeTree()
PRIMARY KEY (timestamp);


-- Daily Impressions 
CREATE TABLE daily_impressions (
    count_of_impressions Int32,
    campaign_id Int32,
    created_at Date
) 
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (campaign_id, created_at);

-- Daily Clicks 
CREATE TABLE daily_clicks (
    count_of_clicks Int32,
    campaign_id Int32,
    created_at Date
) 
ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (campaign_id, created_at);
