import generico as func
from pyspark.sql import SparkSession
from pyspark.sql.functions import col




def escolhe_teste():
    print(f"Qual teste pretende fazer?")

def escolha_ambiente(tecnologia):
    print(f"Você escolheu a tecnologia: {tecnologia}, em qual ambiente pretende realizar os testes? DEV, PRE ou PRO?")
    while True:
        texto_input = input("> ")
        if texto_input.lower() == 'dev':
            ambiente='DEV'
            print(f"Você escolheu o ambiente: {ambiente}")
            break
        elif texto_input.lower() == 'pre':
            ambiente='PRE'
            print(f"Você escolheu o ambiente: {ambiente}")
            break
        elif texto_input.lower() == 'pro':
            ambiente='PRO'
            print(f"Você escolheu o ambiente: {ambiente}")
            break
        else:
            print(f"Você escolheu um ambiente inválido: {texto_input}")
            break
        
        

def start(tecnologia):
    escolha_ambiente(tecnologia)

if __name__ == '__main__':
    start()