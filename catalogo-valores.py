# Databricks notebook source
def test_data_catalog(
    param_id,
    test_id,
    test_type,
    src_table,
    src_filter,
):
    try:
        query = f"""
            SELECT
                CASE
                    WHEN SRC.COUNT_SRC = 0 THEN 'OK'
                    ELSE 'NOT OK'
                END AS OUTPUT
            FROM ( SELECT COUNT(1) AS COUNT FROM {src_table} WHERE {src_filter} ) SRC
        """

        result = spark.sql(query).collect()[0]['OUTPUT']

        return {
            "PARAM_ID": param_id,
            "QUERY": query,
            "RESULT": result,
        }
    except Exception as e:
        print(f"[!] Caught exception in test_data_catalog: {e}")