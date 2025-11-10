# Databricks notebook source
from pyspark.sql.functions import col, when, lit
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def test_data_mapping(
    test_id,
    subtype_id,
    src_table,
    dest_table,
    join_key,
    src_select_field,
    dest_select_field,
    src_filter,
    dest_filter,
    src_groupby,
    dest_groupby,
    src_partition_filter_field,
    key_fields
):
    try:
        query = ""
            
        if src_filter == "":
            src_filter = "1=1"
        else:
            src_filter = src_filter

        if dest_filter == "":
            dest_filter = "1=1"
        else:
            dest_filter = dest_filter

        if join_key !="":
            join_key = join_key

        if src_select_field == "":
            query_source = "SELECT * FROM {0} WHERE {1}".format(src_table,  src_filter)
            cols_source = extrair_colunas_cursor_sql(query_source)
        else:
            query_source = "SELECT {0} FROM {1} WHERE {2}".format(src_select_field,src_table,  src_filter)
            cols_source = extrair_colunas_cursor_sql(query_source)

        if dest_select_field == "":
            query_dest = "SELECT * FROM {0} WHERE {1}".format(dest_table, dest_filter)
            cols_dest = extrair_colunas_cursor_sql(query_dest)
        else:
            query_dest = "SELECT {0} FROM {1} WHERE {2}".format(dest_select_field,dest_table,  dest_filter)
            cols_dest = extrair_colunas_cursor_sql(query_source)

        colunas_a = cols_source
        colunas_b = cols_dest

        if src_select_field == "":
            # Define as queries
            query1_str = "SELECT {0} FROM {1} WHERE {2}".format(', '.join  (colunas_a),     src_table, src_filter)
        else:
            query1_str = "SELECT {0} FROM {1} WHERE {2}".format(src_select_field,     src_table, src_filter)
        
        if dest_select_field == "":
            query2_str = "SELECT {0} FROM {1} WHERE {2}".format(', '.join  (colunas_b),  dest_table, dest_filter)
        else:
            query2_str = "SELECT {0}  FROM {1} WHERE {2}".format(dest_select_field,     dest_table, dest_filter)

        # Cria os DataFrames
        df_source = spark.sql(query1_str)
        df_source.show()

        df_dest = spark.sql(query2_str)
        df_dest = df_dest.withColumnRenamed("sum(vitorias)", "vitorias")
        df_dest.show()

        if join_key != "1 = 1":
            left_col, right_col = [s.strip() for s in join_key.split("=")]

            df_join = df_source.alias("src").join(
            df_dest.alias("dest"),
            on=F.col(left_col) == F.col(right_col),
            how="outer"
            )
        else:
            df_join = df_source.alias("src").join(
                df_dest.alias("dest"),
                on=lit(True),
                how="outer"
            )




        # Seleciona colunas com sufixos para comparação
        select_cols = []
        for ca, cb in zip(colunas_a, colunas_b):
            select_cols.append(col(f"src.{ca}").alias(f"{ca}_src"))
            select_cols.append(col(f"dest.{cb}").alias(f"{cb}_dest"))

        df_comparado_val = df_join.select(*select_cols)

        # Gera expressão para identificar diferenças
        condicoes_diferenca = [
            col(f"{ca}_src") != col(f"{cb}_dest") for ca, cb in zip(colunas_a, colunas_b)
        ]
    
        # Adiciona coluna de status
        df_status_val = df_comparado_val.withColumn(
            "status",
            when(reduce(lambda x, y: x | y, condicoes_diferenca), lit("NO MATCH"))
            .when(reduce(lambda x, y: x & y, [col(f"{cb}_dest").isNull() for cb in     colunas_b]),    lit("NO MATCH @ ONLY TABELA A"))
            .when(reduce(lambda x, y: x & y, [col(f"{ca}_src").isNull() for ca in     colunas_a]),    lit("NO MATCH @ ONLY TABELA B"))
            .otherwise(lit("MATCH"))
        )
    
        # Filtra apenas diferenças
        df_diferencas_calc = df_status_val.filter(col("status") != "MATCH")
        df_diferencas_calc.show()

        result_list = [row[f"{key_fields}_src"] for row in df_diferencas_calc.select(f"{key_fields}_src").collect()]

        if result_list != []:
            result = "NOK"
        else:
            result = "OK"
    
        return {
            "TEST_ID": test_id,
            "SUBTYPE_ID": subtype_id,
            "QUERY": query,
            "RESULT": result,
            "SRC_PARTITION_FILTER_FIELD": src_partition_filter_field,
            "SRC_FIELD": src_select_field,
            "DEST_FIELD": dest_select_field,
            "KEY_FIELDS": key_fields,
            "RESULT_DETAILS": result_list

        }
    except Exception as e:
        print(f"[!] Caught exception in test_data_mapping: {e}")

def extrair_colunas_cursor_sql(query, chave_cruzamento=None, limite=None):
    chave_cruzamento = chave_cruzamento or []
    df_query = spark.sql(query)
    colunas = [field.name for field in df_query.schema.fields]
    colunas_validas = [col for col in colunas if col and col != '# col_name']
    colunas_filtradas = [col for col in colunas_validas if col not in chave_cruzamento]
    if limite in colunas_filtradas:
        colunas_filtradas = colunas_filtradas[:colunas_filtradas.index(limite)]
    return colunas_filtradas