import logging
from datetime import datetime
from config.config import get_spark_session, DATABASE_CONFIG, PATHS
from src.extract import Extractor
from src.transform import Transformer
from src.load import Loader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/etl_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def run_etl_pipeline():
    """
    Main ETL Pipeline ด้วย PySpark
    """
    logger.info("="*60)
    logger.info("🚀 Starting PySpark ETL Pipeline")
    logger.info("="*60)
    
    # Initialize Spark Session
    spark = get_spark_session("ETL_Pipeline")
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # ========== EXTRACT ==========
        logger.info("\n📥 PHASE 1: EXTRACT")
        logger.info("-"*60)
        
        extractor = Extractor(spark)
        
        # ดึงข้อมูลจาก API
        posts_df = extractor.extract_from_api("/posts")
        users_df = extractor.extract_from_api("/users")
        
        # บันทึก Raw Data (สำรอง)
        posts_df.write.mode("overwrite").parquet(f"{PATHS['raw_data']}/posts")
        users_df.write.mode("overwrite").parquet(f"{PATHS['raw_data']}/users")
        
        # ========== TRANSFORM ==========
        logger.info("\n🔄 PHASE 2: TRANSFORM")
        logger.info("-"*60)
        
        transformer = Transformer(spark)
        
        # Transform Posts
        posts_clean = transformer.clean_text_columns(posts_df, ["title", "body"])
        posts_clean = transformer.validate_email(posts_clean)  # ถ้ามี email
        posts_clean = transformer.add_derived_columns(posts_clean)
        posts_clean = transformer.remove_duplicates(posts_clean, ["id"])
        posts_clean = transformer.add_quality_score(posts_clean)
        
        # Transform Users
        users_clean = transformer.clean_text_columns(users_df, ["name", "email"])
        users_clean = transformer.validate_email(users_clean, "email")
        users_clean = transformer.add_derived_columns(users_clean)
        
        # รัน Data Quality Checks
        transformer.run_data_quality_checks(posts_clean, "posts_clean")
        
        # ========== LOAD ==========
        logger.info("\n📤 PHASE 3: LOAD")
        logger.info("-"*60)
        
        loader = Loader(spark)
        
        # โหลดเป็น Parquet
        loader.load_to_parquet(
            posts_clean, 
            f"{PATHS['output']}/posts_clean",
            mode="overwrite"
        )
        
        loader.load_to_parquet(
            users_clean,
            f"{PATHS['output']}/users_clean",
            mode="overwrite"
        )
        
        # โหลดลง MotherDuck
        loader.load_to_motherduck(
            posts_clean,
            "spark_posts_clean",
            token=DATABASE_CONFIG["motherduck"]["token"]
        )
        
        loader.load_to_motherduck(
            users_clean,
            "spark_users_clean",
            token=DATABASE_CONFIG["motherduck"]["token"]
        )
        
        # ========== AGGREGATION ==========
        logger.info("\n📊 PHASE 4: AGGREGATION")
        logger.info("-"*60)
        
        # สร้าง Summary Table
        posts_summary = transformer.aggregate_data(
            posts_clean,
            group_by=["userId"],
            aggregations={
                "id": "count",
                "body_length": "avg",
                "quality_score": "avg"
            }
        )
        
        loader.load_to_motherduck(
            posts_summary,
            "spark_posts_summary",
            token=DATABASE_CONFIG["motherduck"]["token"]
        )
        
        logger.info("\n" + "="*60)
        logger.info("✅ PySpark ETL Pipeline Completed Successfully!")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"❌ Pipeline Failed: {e}")
        raise
    
    finally:
        # ปิด Spark Session
        spark.stop()
        logger.info("Spark Session stopped")

if __name__ == "__main__":
    run_etl_pipeline()