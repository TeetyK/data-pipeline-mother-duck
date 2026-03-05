from pyspark.sql import SparkSession
from pyspark.sql.functions import col , trim , lower ,to_date
from config import LANDING_PATH , SILVER_PATH , SCHEME_METADATA

def apply_text_cleanup(df , column):
    return df.withColumn(column , lower(trim(col(column))))
def apply_date_cleanup(df , column):
    return df.withColumn(column , to_date(col(column)))

spark = SparkSession.builder.appName("Cleanup-Main")

for table , schema in SCHEME_METADATA.items():
    df = spark.read.parquet(LANDING_PATH[table])
    
    for column , dtype in schema.items():
        if dtype =="string":
            df = apply_text_cleanup(df , column)
        if column.endswith("_date"):
            df = apply_date_cleanup(df , column)
    df.write.mode('overwrite').parquet(SILVER_PATH[table])

spark.stop()