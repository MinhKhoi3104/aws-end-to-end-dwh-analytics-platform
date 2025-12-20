# OJDBC JAR File
ORL_LIB_PATH = {"driver_class_path": "/opt/spark/jars/ojdbc17.jar"}

# Path đến Redshift JDBC jar
REDSHIFT_JDBC_JAR_PATH = "/opt/spark/jars/redshift-jdbc42-2.2.1.jar"

# Config kết nối Redshift
REDSHIFT_JDBC = {
    "url": "jdbc:redshift://my-project-e2e-wg.950242545712.ap-southeast-1.redshift-serverless.amazonaws.com:5439/my-project-e2e-dtb",
    "properties": {
        "user": "admin",
        "password": "Devdata123"
    }
}
