from pyspark.sql import SparkSession
from pyspark.sql.functions import count , sum
from config import SILVER_PATHs , GOLD_PATHS

spark = SparkSession.builder.appName("BusinessLogicFinal").getOrCreate()

orders_df = spark.read.parquet(SILVER_PATHs['orders'])
customers_df = spark.read.parquet(SILVER_PATHs['customers'])

country_metrics_df = (
    orders_df
    .join(customers_df , on='customer_id' , how='left')
    .groupBy("country")
    .agg(
        count("order_id").alias('total_orders'),
        sum("amount").alias('total_revenue')
    )
)

country_metrics_df.write.mode("overwrite").parquet(
    GOLD_PATHS['country_metrics']
)

orders_df.createOrReplaceTempView("orders")

segmented_orders_df = spark.sql("""
    SELECT
        country,
        CASE
            WHEN amount < 50 THEN 'small'
            WHEN amount BETWEEN 50 AND 200 THEN 'medium'
            ELSE 'large'
        END AS order_size,
        COUNT(order_id) AS orders_count
    FROM orders
    GROUP BY
        country
        CASE
            WHEN amount < 50 THEN 'small'
            WHEN amount BETWEEN 50 AND 200 THEN 'medium'
            ELSE 'large'
        END                      
                                """)

segmented_orders_df.write.mode("overwrite").parquet(
    GOLD_PATHS['order_segments']
)
spark.stop()