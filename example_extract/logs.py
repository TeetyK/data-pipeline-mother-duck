import logging

logging.basicConfig(
    Level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger('pipeline')

try:
    logger.info("Pipline data | layer=silver | entity=o")
    
    rows = df.count()
    logger.info(f"Read data | row= {rows}")
    
    df.write.mode('append').parquet('orders')
    logger.info("Write data | target=orders")

except Exception as e:
    logger.errot(f"Pipeline failed | error={e}")
    raise