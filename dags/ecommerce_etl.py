from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta, date
import logging
import random
import pandas as pd
import numpy as np
from faker import Faker
from clickhouse_driver import Client
from prophet import Prophet

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

PRODUCTS_COUNT = 500
CUSTOMERS_COUNT = 10000
ORDERS_PER_DAY = 100  
DAYS_TO_GENERATE = 365  

CATEGORIES = {
    'Electronics': ['Smartphones', 'Laptops', 'Tablets', 'Headphones', 'Cameras'],
    'Clothing': ['T-Shirts', 'Jeans', 'Dresses', 'Shoes', 'Jackets'],
    'Home': ['Furniture', 'Decor', 'Kitchen', 'Bedding', 'Lighting'],
    'Books': ['Fiction', 'Non-Fiction', 'Educational', 'Comics', 'E-books'],
    'Sports': ['Equipment', 'Apparel', 'Footwear', 'Accessories', 'Nutrition']
}

BRANDS = ['Apple', 'Samsung', 'Nike', 'Adidas', 'Sony', 'LG', 'Zara', 'H&M', 'Ikea', 'Amazon Basics']
TRAFFIC_SOURCES = ['organic', 'paid_search', 'social_media', 'email', 'direct', 'referral']
DEVICE_TYPES = ['mobile', 'desktop', 'tablet']
PAYMENT_METHODS = ['credit_card', 'debit_card', 'paypal', 'bank_transfer', 'cash_on_delivery']

CLICKHOUSE_CONNECTION = {
    'host': 'clickhouse',
    'database': 'ecommerce'
}

def generate_products():
    """Генерация каталога товаров"""
    products = []
    for i in range(PRODUCTS_COUNT):
        category = random.choice(list(CATEGORIES.keys()))
        subcategory = random.choice(CATEGORIES[category])
        
        product = {
            'product_id': f'PROD{str(i+1).zfill(5)}',
            'product_name': f'{subcategory} {fake.word().capitalize()} {random.randint(100, 999)}',
            'category': category,
            'subcategory': subcategory,
            'brand': random.choice(BRANDS),
            'supplier': fake.company(),
            'unit_cost': round(random.uniform(10, 500), 2),
            'unit_price': round(random.uniform(20, 1000), 2),
            'weight_kg': round(random.uniform(0.1, 10), 2),
            'is_active': random.choice([True, True, True, False]), 
            'launch_date': fake.date_between(start_date='-2y', end_date='today')
        }
        products.append(product)
    
    return pd.DataFrame(products)

def generate_customers():
    """Генерация клиентской базы"""
    customers = []
    countries = ['USA', 'UK', 'Germany', 'France', 'Canada', 'Australia', 'Japan', 'Brazil']
    
    for i in range(CUSTOMERS_COUNT):
        customer = {
            'customer_id': f'CUST{str(i+1).zfill(6)}',
            'email': fake.email(),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'gender': random.choice(['M', 'F', 'Other']),
            'age': random.randint(18, 70),
            'city': fake.city(),
            'country': random.choice(countries),
            'registration_date': fake.date_between(start_date='-3y', end_date='today'),
            'customer_lifetime_value': round(random.uniform(100, 10000), 2),
            'preferred_category': random.choice(list(CATEGORIES.keys())),
            'is_premium': random.choice([True, False, False, False])  
        }
        customers.append(customer)
    
    return pd.DataFrame(customers)

def generate_orders(execution_date):
    """Генерация заказов за определенную дату"""
    orders = []
    order_items = []
    
    base_orders = ORDERS_PER_DAY
    month = execution_date.month
    day_of_week = execution_date.weekday()
    
    if month in [11, 12]: 
        base_orders = int(base_orders * 2.5)
    elif month in [6, 7]:
        base_orders = int(base_orders * 1.3)
    
    if day_of_week in [5, 6]: 
        base_orders = int(base_orders * 1.2)
    
    num_orders = random.randint(int(base_orders * 0.8), int(base_orders * 1.2))
    
    for i in range(num_orders):
        order_id = f'ORD{execution_date.strftime("%Y%m%d")}{str(i+1).zfill(4)}'
        customer_id = f'CUST{str(random.randint(1, CUSTOMERS_COUNT)).zfill(6)}'
        
        order_time = execution_date.replace(
            hour=random.randint(0, 23),
            minute=random.randint(0, 59),
            second=random.randint(0, 59)
        )
        
        num_items = random.randint(1, 5)
        order_total = 0
        
        for j in range(num_items):
            product_id = f'PROD{str(random.randint(1, PRODUCTS_COUNT)).zfill(5)}'
            quantity = random.randint(1, 3)
            unit_price = round(random.uniform(20, 1000), 2)
            total_price = round(unit_price * quantity, 2)
            order_total += total_price
            
            item = {
                'order_id': order_id,
                'product_id': product_id,
                'product_name': f'Product {product_id}',
                'category': random.choice(list(CATEGORIES.keys())),
                'subcategory': random.choice(['Sub1', 'Sub2', 'Sub3']),
                'brand': random.choice(BRANDS),
                'quantity': quantity,
                'unit_price': unit_price,
                'total_price': total_price
            }
            order_items.append(item)
        
        discount = 0
        if random.random() < 0.2:
            discount = round(order_total * random.uniform(0.05, 0.30), 2)
        
        order = {
            'order_id': order_id,
            'customer_id': customer_id,
            'order_date': order_time,
            'status': random.choice(['completed', 'completed', 'completed', 'processing', 'cancelled']),
            'total_amount': order_total,
            'discount_amount': discount,
            'shipping_cost': round(random.uniform(5, 30), 2),
            'payment_method': random.choice(PAYMENT_METHODS),
            'shipping_address_city': fake.city(),
            'shipping_address_country': random.choice(['USA', 'UK', 'Germany', 'France']),
            'device_type': random.choice(DEVICE_TYPES),
            'traffic_source': random.choice(TRAFFIC_SOURCES)
        }
        orders.append(order)
    
    return pd.DataFrame(orders), pd.DataFrame(order_items)

def load_initial_data(**context):
    """Загрузка начальных данных (продукты и клиенты)"""
    client = Client(host=CLICKHOUSE_CONNECTION['host'])
    client.execute('CREATE DATABASE IF NOT EXISTS ecommerce')
    
    client = Client(**CLICKHOUSE_CONNECTION)    
    products_df = generate_products()
    products_records = products_df.to_dict('records')
    
    client.execute(
        "INSERT INTO raw_products (product_id, product_name, category, subcategory, brand, supplier, unit_cost, unit_price, weight_kg, is_active, launch_date) VALUES",
        [(r['product_id'], r['product_name'], r['category'], r['subcategory'], r['brand'], 
          r['supplier'], float(r['unit_cost']), float(r['unit_price']), float(r['weight_kg']), 
          r['is_active'], r['launch_date']) for r in products_records]
    )
    
    logging.info(f"Loaded {len(products_df)} products")
    
    customers_df = generate_customers()
    customers_records = customers_df.to_dict('records')
    
    client.execute(
        "INSERT INTO raw_customers (customer_id, email, first_name, last_name, gender, age, city, country, registration_date, customer_lifetime_value, preferred_category, is_premium) VALUES",
        [(r['customer_id'], r['email'], r['first_name'], r['last_name'], r['gender'], 
          int(r['age']), r['city'], r['country'], r['registration_date'], 
          float(r['customer_lifetime_value']), r['preferred_category'], r['is_premium']) for r in customers_records]
    )
    
    logging.info(f"Loaded {len(customers_df)} customers")

def load_historical_orders(**context):
    """Загрузка исторических данных заказов за год"""
    client = Client(**CLICKHOUSE_CONNECTION)
    
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=DAYS_TO_GENERATE)
    
    current_date = start_date
    total_orders = 0
    total_items = 0
    
    while current_date <= end_date:
        orders_df, items_df = generate_orders(datetime.combine(current_date, datetime.min.time()))
        
        if not orders_df.empty:
            orders_records = orders_df.to_dict('records')
            client.execute(
                """INSERT INTO raw_orders 
                (order_id, customer_id, order_date, status, total_amount, discount_amount, 
                shipping_cost, payment_method, shipping_address_city, shipping_address_country, 
                device_type, traffic_source) VALUES""",
                [(r['order_id'], r['customer_id'], r['order_date'], r['status'], 
                  float(r['total_amount']), float(r['discount_amount']), float(r['shipping_cost']),
                  r['payment_method'], r['shipping_address_city'], r['shipping_address_country'],
                  r['device_type'], r['traffic_source']) for r in orders_records]
            )
            total_orders += len(orders_df)
        
        if not items_df.empty:
            items_records = items_df.to_dict('records')
            client.execute(
                """INSERT INTO raw_order_items 
                (order_id, product_id, product_name, category, subcategory, brand, 
                quantity, unit_price, total_price) VALUES""",
                [(r['order_id'], r['product_id'], r['product_name'], r['category'], 
                  r['subcategory'], r['brand'], int(r['quantity']), 
                  float(r['unit_price']), float(r['total_price'])) for r in items_records]
            )
            total_items += len(items_df)
        
        if current_date.day == 1: 
            logging.info(f"Processed orders up to {current_date}")
        
        current_date += timedelta(days=1)
    
    logging.info(f"Loaded total: {total_orders} orders with {total_items} items")

def calculate_rfm_segments(**context):
    """Расчет RFM-сегментации клиентов"""
    client = Client(**CLICKHOUSE_CONNECTION)
    
    # Расчет RFM метрик
    client.execute("""
        INSERT INTO dwh_customer_360
        SELECT 
            customer_id,
            min(toDate(order_date)) as first_order_date,
            max(toDate(order_date)) as last_order_date,
            dateDiff('day', max(toDate(order_date)), today()) as days_since_last_order,
            count(distinct order_id) as total_orders,
            sum(total_amount - discount_amount) as total_spent,
            avg(total_amount - discount_amount) as avg_order_value,
            '' as favorite_category,  -- Будет заполнено позже
            multiIf(
                days_since_last_order <= 30 AND total_orders >= 10, 'Champions',
                days_since_last_order <= 30 AND total_orders >= 5, 'Loyal Customers',
                days_since_last_order <= 90 AND total_orders >= 3, 'Potential Loyalists',
                days_since_last_order <= 90 AND total_orders = 1, 'New Customers',
                days_since_last_order <= 180, 'At Risk',
                days_since_last_order <= 365, 'Cant Lose Them',
                'Lost'
            ) as customer_segment,
            concat(
                toString(multiIf(days_since_last_order <= 30, 5, days_since_last_order <= 60, 4, 
                                 days_since_last_order <= 90, 3, days_since_last_order <= 180, 2, 1)),
                toString(multiIf(total_orders >= 20, 5, total_orders >= 10, 4, 
                                total_orders >= 5, 3, total_orders >= 2, 2, 1)),
                toString(multiIf(total_spent >= 5000, 5, total_spent >= 2000, 4, 
                                total_spent >= 1000, 3, total_spent >= 500, 2, 1))
            ) as rfm_score,
            0.0 as churn_probability,  -- Можно добавить ML модель
            now() as updated_at
        FROM raw_orders
        WHERE status = 'completed'
        GROUP BY customer_id
        SETTINGS max_insert_threads = 1
    """)
    
    logging.info("RFM segmentation completed")

def calculate_product_performance(**context):
    """Расчет производительности продуктов и ABC-XYZ анализ"""
    client = Client(**CLICKHOUSE_CONNECTION)
    
    client.execute("TRUNCATE TABLE IF EXISTS ecommerce.dwh_product_performance")

    query = """
    INSERT INTO ecommerce.dwh_product_performance
    WITH 
        product_revenue AS (
            SELECT
                oi.product_id,
                any(oi.product_name) AS product_name,
                any(oi.category) AS category,
                sum(oi.quantity) AS total_sold_quantity,
                sum(oi.total_price) AS total_revenue,
                count(DISTINCT oi.order_id) AS total_orders
            FROM ecommerce.raw_order_items oi
            JOIN ecommerce.raw_orders o ON oi.order_id = o.order_id
            WHERE o.status = 'completed'
            GROUP BY oi.product_id
        ),
        
        product_cumulative AS (
            SELECT
                product_id,
                sum(total_revenue) OVER (ORDER BY total_revenue DESC) AS cumulative_revenue,
                sum(total_revenue) OVER () AS total_revenue_overall
            FROM product_revenue
        ),
        
        product_abc AS (
            SELECT
                product_id,
                multiIf(
                    -- ИЗМЕНЕНИЕ: Приводим cumulative_revenue к Float64
                    toFloat64(cumulative_revenue) <= total_revenue_overall * 0.8, 'A',
                    toFloat64(cumulative_revenue) <= total_revenue_overall * 0.95, 'B',
                    'C'
                ) AS abc_category
            FROM product_cumulative
        ),

        monthly_sales AS (
            SELECT
                product_id,
                toStartOfMonth(o.order_date) AS sale_month,
                sum(oi.quantity) AS monthly_quantity
            FROM ecommerce.raw_order_items oi
            JOIN ecommerce.raw_orders o ON oi.order_id = o.order_id
            WHERE o.status = 'completed'
            GROUP BY product_id, sale_month
        ),

        product_xyz AS (
            SELECT
                product_id,
                if(avg(monthly_quantity) > 0, stddevPop(monthly_quantity) / avg(monthly_quantity), 0) AS variation_coeff,
                multiIf(
                    variation_coeff <= 0.25, 'X',
                    variation_coeff <= 0.5, 'Y',
                    'Z'
                ) AS xyz_category
            FROM monthly_sales
            GROUP BY product_id
        )

    SELECT
        pr.product_id,
        pr.product_name,
        pr.category,
        pr.total_sold_quantity,
        pr.total_revenue,
        pr.total_orders,
        0.0 AS avg_rating,
        0.0 AS return_rate,
        pa.abc_category,
        coalesce(px.xyz_category, 'Z') AS xyz_category,
        now() AS updated_at
    FROM product_revenue pr
    LEFT JOIN product_abc pa ON pr.product_id = pa.product_id
    LEFT JOIN product_xyz px ON pr.product_id = px.product_id
    """
    client.execute(query, settings={'allow_experimental_window_functions': 1})
    logging.info("Product performance and ABC-XYZ analysis completed")



def train_and_predict_demand(**context):
    """Обучение модели Prophet и сохранение прогноза в ClickHouse."""
    client = Client(**CLICKHOUSE_CONNECTION)
    
    query = "SELECT date, total_revenue FROM ecommerce.stg_daily_sales ORDER BY date"
    data = client.execute(query, with_column_types=True)
    
    if not data[0]:
        logging.warning("No data found in stg_daily_sales to train forecast model.")
        return

    df = pd.DataFrame(data[0], columns=[c[0] for c in data[1]])
    df = df.rename(columns={'date': 'ds', 'total_revenue': 'y'})
    
    model = Prophet(daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=True)
    model.fit(df)
    
    future = model.make_future_dataframe(periods=30)
    forecast = model.predict(future)
    
    forecast_to_load = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(30)
    forecast_to_load = forecast_to_load.rename(columns={
        'ds': 'forecast_date',
        'yhat': 'predicted_revenue',
        'yhat_lower': 'predicted_revenue_lower',
        'yhat_upper': 'predicted_revenue_upper'
    })
    
    client.execute("TRUNCATE TABLE ecommerce.dwh_demand_forecast")
    
    records = forecast_to_load.to_dict('records')
    client.execute(
        "INSERT INTO ecommerce.dwh_demand_forecast (forecast_date, predicted_revenue, predicted_revenue_lower, predicted_revenue_upper) VALUES",
        records
    )
    logging.info(f"Saved {len(records)} demand forecast records to ClickHouse.")
def detect_order_anomalies(**context):
    """Поиск аномальных заказов по Z-оценке (Z-score)."""
    client = Client(**CLICKHOUSE_CONNECTION)
    
    client.execute("TRUNCATE TABLE IF EXISTS ecommerce.dwh_order_anomalies")

    z_score_threshold = 3.0
    
    query = f"""
    INSERT INTO ecommerce.dwh_order_anomalies
    WITH daily_stats AS (
        SELECT
            toDate(order_date) AS date,
            avg(total_amount) AS avg_amount,
            stddevPop(total_amount) AS stddev_amount
        FROM ecommerce.raw_orders
        WHERE status = 'completed' AND toDate(order_date) = yesterday()
        GROUP BY date
    )
    SELECT
        o.order_id,
        o.order_date,
        o.total_amount,
        ds.avg_amount AS avg_amount_for_day,
        ds.stddev_amount AS stddev_amount_for_day,
        -- Приводим total_amount к Float64 перед вычитанием
        if(ds.stddev_amount > 0, (toFloat64(o.total_amount) - ds.avg_amount) / ds.stddev_amount, 0) AS z_score,
        now() AS detected_at
    FROM ecommerce.raw_orders o
    JOIN daily_stats ds ON toDate(o.order_date) = ds.date
    WHERE 
        status = 'completed' AND 
        toDate(order_date) = yesterday() AND
        abs(z_score) > {z_score_threshold}
    """
    client.execute(query)
    logging.info("Anomaly detection for yesterday's orders completed.")
    
def calculate_cohort_retention(**context):
    """Расчет когортного анализа"""
    client = Client(**CLICKHOUSE_CONNECTION)

    client.execute("TRUNCATE TABLE IF EXISTS ecommerce.cohort_retention")
    
    query = """
    -- ИЗМЕНЕНИЕ: Явно перечисляем колонки для вставки
    INSERT INTO ecommerce.cohort_retention
    (cohort_month, month_number, customers_count, retention_rate, avg_revenue_per_user)
    WITH 
        customer_activities AS (
            SELECT
                customer_id,
                min(toStartOfMonth(order_date)) OVER (PARTITION BY customer_id) as cohort_month,
                toStartOfMonth(order_date) as activity_month
            FROM raw_orders
            WHERE status = 'completed'
        ),
        cohort_counts AS (
            SELECT
                cohort_month,
                activity_month,
                dateDiff('month', cohort_month, activity_month) as month_number,
                count(DISTINCT customer_id) as customers_count
            FROM customer_activities
            GROUP BY cohort_month, activity_month
        ),
        initial_cohort_sizes AS (
            SELECT
                cohort_month,
                customers_count as initial_customers
            FROM cohort_counts
            WHERE month_number = 0
        )
    SELECT 
        cc.cohort_month,
        cc.month_number,
        cc.customers_count,
        round(cc.customers_count * 100.0 / ics.initial_customers, 2) as retention_rate,
        0.0 as avg_revenue_per_user
    FROM cohort_counts cc
    JOIN initial_cohort_sizes ics ON cc.cohort_month = ics.cohort_month
    WHERE cc.month_number <= 12
    ORDER BY cc.cohort_month, cc.month_number
    """
    client.execute(query, settings={'allow_experimental_window_functions': 1})
    logging.info("Cohort retention analysis completed")

def calculate_daily_metrics(**context):
    """Расчет ежедневных метрик (полный пересчет)"""
    client = Client(**CLICKHOUSE_CONNECTION)
    
    client.execute("TRUNCATE TABLE IF EXISTS ecommerce.stg_daily_sales")

    query_daily_sales = """
        INSERT INTO ecommerce.stg_daily_sales 
        (date, total_orders, total_revenue, total_customers, avg_order_value, total_items_sold)
        SELECT 
            toDate(order_date) as date,
            count(distinct order_id) as total_orders,
            sum(total_amount - discount_amount) as total_revenue,
            count(distinct customer_id) as total_customers,
            avg(total_amount - discount_amount) as avg_order_value,
            sum(oi.quantity) as total_items_sold
        FROM raw_orders o
        JOIN (
            SELECT order_id, sum(quantity) as quantity
            FROM raw_order_items
            GROUP BY order_id
        ) oi ON o.order_id = oi.order_id
        WHERE o.status = 'completed'
        GROUP BY date
    """
    client.execute(query_daily_sales)
    logging.info("Daily metrics calculated for all history")

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ecommerce_analytics_etl',
    default_args=default_args,
    description='E-commerce Analytics ETL Pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['ecommerce', 'analytics', 'rfm', 'cohort']
) as dag:
    
    start = EmptyOperator(task_id='start')

    initial_load_group = EmptyOperator(task_id='initial_data_load')
    
    init_products_customers = PythonOperator(
        task_id='load_initial_data',
        python_callable=load_initial_data,
    )
    
    load_orders = PythonOperator(
        task_id='load_historical_orders',
        python_callable=load_historical_orders,
    )

    analytics_processing_group = EmptyOperator(task_id='daily_analytics_processing')

    daily_metrics = PythonOperator(
        task_id='calculate_daily_metrics',
        python_callable=calculate_daily_metrics,
    )

    rfm_analysis = PythonOperator(
        task_id='calculate_rfm_segments',
        python_callable=calculate_rfm_segments,
    )
    
    product_analysis = PythonOperator(
        task_id='calculate_product_performance',
        python_callable=calculate_product_performance,
    )
    
    cohort_analysis = PythonOperator(
        task_id='calculate_cohort_retention',
        python_callable=calculate_cohort_retention,
    )
    
    demand_forecasting = PythonOperator(
        task_id='train_and_predict_demand',
        python_callable=train_and_predict_demand,
    )
    
    anomaly_detection = PythonOperator(
        task_id='detect_order_anomalies',
        python_callable=detect_order_anomalies,
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> initial_load_group
    initial_load_group >> init_products_customers >> load_orders >> analytics_processing_group
    
    analytics_processing_group >> daily_metrics 
    
    daily_metrics >> [
        rfm_analysis, 
        product_analysis, 
        cohort_analysis, 
        demand_forecasting,
        anomaly_detection
    ] >> end