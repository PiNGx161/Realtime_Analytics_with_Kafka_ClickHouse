
-- CLICKHOUSE INIT SCRIPT - SALES ANALYTICS
CREATE DATABASE IF NOT EXISTS analytics;


-- RAW SALES ORDERS TABLE
CREATE TABLE IF NOT EXISTS analytics.sales_orders
(
    order_id         String,
    customer_id      String,
    customer_name    String,
    customer_email   String,
    product_id       String,
    product_name     String,
    category         String,
    quantity         UInt32,
    unit_price       Float64,
    discount_percent Float64 DEFAULT 0,
    total_amount     Float64,
    payment_method   String,
    region           String,
    sales_rep        String,
    order_status     String,
    order_timestamp  DateTime64(3),
    _ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(order_timestamp)
ORDER BY (order_timestamp, order_id)
TTL order_timestamp + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;



-- AGGREGATED TABLES
CREATE TABLE IF NOT EXISTS analytics.sales_by_category_hourly
(
    hour          DateTime,
    category      String,
    order_count   UInt64,
    total_revenue Float64,
    total_quantity UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, category);


CREATE TABLE IF NOT EXISTS analytics.sales_by_region_daily
(
    date          Date,
    region        String,
    order_count   UInt64,
    total_revenue Float64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, region);



-- MATERIALIZED VIEWS
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_sales_by_category_hourly
TO analytics.sales_by_category_hourly
AS
SELECT
    toStartOfHour(order_timestamp) AS hour,
    category,
    count() AS order_count,
    sum(total_amount) AS total_revenue,
    sum(quantity) AS total_quantity
FROM analytics.sales_orders
WHERE order_status = 'completed'
GROUP BY hour, category;


CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_sales_by_region_daily
TO analytics.sales_by_region_daily
AS
SELECT
    toDate(order_timestamp) AS date,
    region,
    count() AS order_count,
    sum(total_amount) AS total_revenue
FROM analytics.sales_orders
WHERE order_status = 'completed'
GROUP BY date, region;
