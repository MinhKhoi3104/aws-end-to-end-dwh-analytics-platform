import os, sys
current_dir = os.path.dirname(__file__)
config_path = os.path.join(current_dir, '..')
config_path = os.path.abspath(config_path)
sys.path.insert(0, config_path)
from pyspark.sql import SparkSession
from _01_config.data_storage_config import *
from _01_config.jar_paths import *

# Using to create spark session at Bronze Layer
def create_bronze_spark_session(appName):
    """Auto-select credentials based on environment"""
    
    # Detect environment
    is_airflow = os.getenv("AIRFLOW_ENV", "false") == "true"
    
    builder = (
        SparkSession.builder
        .appName(appName)
        .config("spark.jars.packages", 
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.impl", 
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint",
                "s3.ap-southeast-1.amazonaws.com")
    )
    
    if is_airflow:
        print("🚀 Running in AIRFLOW - using IAM Role")
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider"
        )
    else:
        print("💻 Running LOCALLY - using AWS Profile")
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.profile.ProfileCredentialsProvider"
        ).config("spark.hadoop.fs.s3a.profile", "default")
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# Execute SQL DDL/DML directly in Redshift
def execute_sql_ddl(spark, sql_query):
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


# Using to create spark session at Silver Layer
def create_silver_spark_session(appName: str):
    """
    Works for:
    - Local (AWS profile)
    - Airflow (IAM Role)
    """

    is_airflow = os.getenv("AIRFLOW_HOME") is not None

    builder = (
        SparkSession.builder
        .appName(appName)

        # ===== Iceberg =====
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config(
            "spark.sql.catalog.iceberg.warehouse",
            "s3a://data-pipeline-e2e-datalake-98c619f9/iceberg-warehouse"
        )
        .config(
            "spark.sql.catalog.iceberg.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO"
        )

        # ===== S3 =====
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-southeast-1.amazonaws.com")
    )

    if is_airflow:
        print("🚀 Airflow environment → IAM Role")
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider"
        )
    else:
        print("💻 Local environment → AWS profile")
        builder = (
            builder
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.profile.ProfileCredentialsProvider"
            )
            .config("spark.hadoop.fs.s3a.profile", "default")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

# Using to create spark session at Gold Layer
def create_gold_spark_session(appName):
    """Gold: Redshift + Iceberg"""
    spark = (
        SparkSession.builder
        .appName(appName)
        .config("spark.jars", 
                f"{HADOOP_AWS_JAR_PATH},{AWS_JAVA_SDK_BUNDLE_JAR_PATH},"
                f"{ICEBERG_SPARK_RUNTIME_JAR_PATH},{REDSHIFT_JDBC_JAR_PATH},{SPARK_REDSHIFT_JAR_PATH}")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.profile.ProfileCredentialsProvider")
        .config("spark.sql.extensions", 
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.prod", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.prod.type", "hadoop")
        .config("spark.sql.catalog.prod.warehouse", "s3a://data-pipeline-e2e-datalake-98c619f9/iceberg-warehouse")
        .config("spark.sql.catalog.prod.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.databricks.redshift.jdbc.url", REDSHIFT_JDBC['url'])
        .config("spark.databricks.redshift.tempdir", REDSHIFT_JDBC['tempdir'])
        .getOrCreate()
    )

    # set log level WARN to reduce unnessary uotput line
    spark.sparkContext.setLogLevel("WARN")

    return spark

# Create S3 object if not exist
def ensure_s3_prefix(spark,s3_path):
    try:
        if not s3_path.startswith("s3a://"):
            raise ValueError("S3 path must start with s3a://")

        hadoop_conf = spark._jsc.hadoopConfiguration()

        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(s3_path),
            hadoop_conf
        )

        path = spark._jvm.org.apache.hadoop.fs.Path(s3_path)

        if not fs.exists(path):
            fs.mkdirs(path)
            message = f"Created S3 bronze layer: {s3_path}"
        else:
            message = f"S3 bronze layer: {s3_path} is exist"
        
        return print(message)
    except Exception as e:
        return print(f"❌ ERROR: {e}")