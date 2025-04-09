import generico as func
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

    
def teste():
    spark = SparkSession.builder \
            .appName("PySpark Databricks") \
            .enableHiveSupport() \
            .getOrCreate()
    
    #df = spark.sql("USE hive_metastore.joaopereira")
    
    #df = spark.sql("SELECT current_database()")
    df = spark.sql("SHOW SCHEMAS").show()
    df = spark.sql("SHOW TABLES").show()
    #df = spark.sql("SELECT * FROM joaopereira.premier_league_data_2022")
    #df.show()


def main():
    teste()


if __name__ == '__main__':
    main()