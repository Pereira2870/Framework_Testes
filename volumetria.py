# Databricks notebook source
from pyspark.sql.functions import col

def test_data_volume(
    test_id,
    subtype_id,
    src_table,
    dest_table,
    src_where_condition,
    dest_where_condition,
    src_partition_filter_field,
    dest_partition_filter_field
):

    try:
        query_src = f"SELECT COUNT(1) AS COUNT_SRC FROM {src_table} WHERE {src_where_condition}"

        query_dest = f"SELECT COUNT(1) AS COUNT_DEST FROM {dest_table} WHERE {dest_where_condition}"

        df_source = spark.sql(query_src)
        df_dest = spark.sql(query_dest)
        df_join = df_source.crossJoin(df_dest)

        # Verifica se existem diferenças
        diff_count = df_join.filter(col("COUNT_SRC") != col("COUNT_DEST")).count()

        if diff_count == 0:
            result = "OK"
        else:
            result = "NOK"

        query = ""

        return {
            "TEST_ID": test_id,
            "SUBTYPE_ID": subtype_id,
            "QUERY": query,
            "RESULT": result,
            "SRC_TABLE": src_table,
            "DEST_TABLE": dest_table,
            "SRC_PARTITION_FILTER_FIELD": src_partition_filter_field,
            "DEST_PARTITION_FILTER_FIELD": dest_partition_filter_field,
        }

    except Exception as e:
        print(f"[!] Caught exception in test_data_volume: {e}")