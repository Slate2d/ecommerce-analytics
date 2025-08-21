-- Создаем базу данных
CREATE DATABASE IF NOT EXISTS ecommerce;

-- RAW слой - сырые данные
CREATE TABLE IF NOT EXISTS ecommerce.raw_orders
(
    order_id String,
    customer_id String,
    order_date DateTime,
    status String,
    total_amount Decimal(10, 2),
    discount_amount Decimal(10, 2),
    shipping_cost Decimal(10, 2),
    payment_method String,
    shipping_address_city String,
    shipping_address_country String,
    device_type String,
    traffic_source String,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date, order_id);

CREATE TABLE IF NOT EXISTS ecommerce.raw_order_items
(
    order_id String,
    product_id String,
    product_name String,
    category String,
    subcategory String,
    brand String,
    quantity UInt32,
    unit_price Decimal(10, 2),
    total_price Decimal(10, 2),
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (order_id, product_id);

CREATE TABLE IF NOT EXISTS ecommerce.raw_customers
(
    customer_id String,
    email String,
    first_name String,
    last_name String,
    gender String,
    age UInt8,
    city String,
    country String,
    registration_date Date,
    customer_lifetime_value Decimal(10, 2),
    preferred_category String,
    is_premium Boolean,
    created_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (customer_id, created_at);

CREATE TABLE IF NOT EXISTS ecommerce.raw_products
(
    product_id String,
    product_name String,
    category String,
    subcategory String,
    brand String,
    supplier String,
    unit_cost Decimal(10, 2),
    unit_price Decimal(10, 2),
    weight_kg Decimal(5, 2),
    is_active Boolean,
    launch_date Date,
    created_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (product_id, created_at);

-- STAGING слой - очищенные данные
CREATE TABLE IF NOT EXISTS ecommerce.stg_daily_sales
(
    date Date,
    total_orders UInt32,
    total_revenue Decimal(12, 2),
    total_customers UInt32,
    avg_order_value Decimal(10, 2),
    total_items_sold UInt32,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (date, updated_at);

-- DWH слой - витрины данных
CREATE TABLE IF NOT EXISTS ecommerce.dwh_customer_360
(
    customer_id String,
    first_order_date Date,
    last_order_date Date,
    days_since_last_order UInt32,
    total_orders UInt32,
    total_spent Decimal(12, 2),
    avg_order_value Decimal(10, 2),
    favorite_category String,
    customer_segment String,
    rfm_score String,
    churn_probability Float32,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (customer_id, updated_at);

CREATE TABLE IF NOT EXISTS ecommerce.dwh_product_performance
(
    product_id String,
    product_name String,
    category String,
    total_sold_quantity UInt32,
    total_revenue Decimal(12, 2),
    total_orders UInt32,
    avg_rating Float32,
    return_rate Float32,
    abc_category String,
    xyz_category String,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (product_id, updated_at);

-- Таблица для когортного анализа
CREATE TABLE IF NOT EXISTS ecommerce.cohort_retention
(
    cohort_month Date,
    month_number UInt8,
    customers_count UInt32,
    retention_rate Float32,
    avg_revenue_per_user Decimal(10, 2),
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (cohort_month, month_number, updated_at);

-- Real-time метрики
CREATE TABLE IF NOT EXISTS ecommerce.realtime_metrics
(
    timestamp DateTime,
    metric_name String,
    metric_value Float64,
    dimension String
)
ENGINE = MergeTree()
PARTITION BY toDate(timestamp)
ORDER BY (timestamp, metric_name);

-- Таблица для прогноза спроса
CREATE TABLE IF NOT EXISTS ecommerce.dwh_demand_forecast
(
    forecast_date Date,
    predicted_revenue Float64,
    predicted_revenue_lower Float64,
    predicted_revenue_upper Float64,
    created_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (forecast_date, created_at);

-- Таблица для аномальных заказов
CREATE TABLE IF NOT EXISTS ecommerce.dwh_order_anomalies
(
    order_id String,
    order_date DateTime,
    total_amount Decimal(10, 2),
    avg_amount_for_day Decimal(10, 2),
    stddev_amount_for_day Float64,
    z_score Float64,
    detected_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (order_date, order_id);