import generico as func
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import hue as hue
import databricks as databricks

def teste():
    print('Olá, pretendes efetuar testes em qual tecnologia? Opções: Hue , DataBricks')
    while True:
        texto = input("> ")
        if texto.lower() == 'sair':
            break
        elif texto.lower() == 'hue':
            print(f"Você escolheu a tecnologia: {texto}")
            hue.start(texto)
        elif texto.lower() == 'databricks':
            print(f"Você escolheu a tecnologia: {texto}")
            databricks.start(texto)
        else:
            print(f"Você escolheu uma tecnologia inválida: {texto}")

        
        

def start():
    teste()

if __name__ == '__main__':
    start()
