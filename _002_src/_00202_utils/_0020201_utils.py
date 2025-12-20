import os, sys
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from pyspark.sql import SparkSession
from _00201_config._0020102_database_config import *

def create_spark_session(appName):
    spark = SparkSession.builder \
                .appName(appName) \
                .config("spark.driver.extraClassPath", REDSHIFT_JDBC_JAR_PATH) \
                .getOrCreate()
    return spark

def execute_sql_ddl(spark, sql_query):
    """Thực thi câu lệnh SQL DDL/DML trực tiếp lên Redshift"""
    jvm = spark._jvm

    try:
        DriverManager = jvm.java.sql.DriverManager
        connection = DriverManager.getConnection(
            REDSHIFT_JDBC["url"],
            REDSHIFT_JDBC["properties"]["user"],
            REDSHIFT_JDBC["properties"]["password"]
        )
        statement = connection.createStatement()
        statement.execute(sql_query)
        return True

    except Exception as e:
        print(f"❌ Error executing SQL on Redshift: {e}")
        return False

    finally:
        if 'connection' in locals() and connection:
            connection.close()



def create_spark_session_iceberg(appName):
    """
    SparkSession với Iceberg catalog cho Silver/Gold
    - Metadata lưu trên Glue Catalog
    - Data files lưu trên S3
    - Redshift chỉ dùng COPY từ S3
    """
    
    spark = (
        SparkSession.builder
        .appName(appName)
        # JDBC jar path nếu muốn query Redshift trực tiếp
        .config("spark.driver.extraClassPath", REDSHIFT_JDBC_JAR_PATH)
        
        # Iceberg extensions
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        
        # Catalog kiểu Glue
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        
        # Warehouse trên S3
        .config("spark.sql.catalog.glue_catalog.warehouse", "s3://my-iceberg-warehouse/")  
        
        .getOrCreate()
    )
    
    return spark