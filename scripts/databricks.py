import generico as func
from pyspark.sql import SparkSession
from pyspark.sql.functions import col




def escolha_teste():
    print(f"Qual teste pretende fazer?")

def escolha_ambiente(tecnologia):
    print(f"Você escolheu a tecnologia: {tecnologia}, em qual ambiente pretende realizar os testes? DEV, PRE ou PRO?")
    while True:
        texto_input = input("> ")
        if texto_input.lower() == 'dev':
            ambiente='DEV'
            print(f"Você escolheu o ambiente: {ambiente}")
            ligacao()
        elif texto_input.lower() == 'pre':
            ambiente='PRE'
            print(f"Você escolheu o ambiente: {ambiente}")
        elif texto_input.lower() == 'pro':
            ambiente='PRO'
            print(f"Você escolheu o ambiente: {ambiente}")
        else:
            print(f"Você escolheu um ambiente inválido: {texto_input}")
        
def ligacao():

    spark = SparkSession.builder \
        .appName("DatabricksConnection") \
        .config("spark.databricks.service.address", "https://adb-5510328928640952.12.azuredatabricks.net") \
        .config("spark.databricks.service.token", "dapib0a476763d857aa8a3832fefcc936a85-2") \
        .getOrCreate()
    result = spark.sql("use hive_metastore.joaopereira")
    result1 = spark.sql("SELECT current_schema()")
    #result = spark.sql("SELECT current_database()")
    #result = spark.sql("select * from joaopereira.premier_league_data_2022")
    result.show()
    result1.show()


def start(tecnologia):
    escolha_ambiente(tecnologia)
    escolha_teste()

if __name__ == '__main__':
    start()
















