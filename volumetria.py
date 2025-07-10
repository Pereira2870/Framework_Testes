# Databricks notebook source
def test_data_volume(
    param_id,
    test_id,
    test_type,
    src_table,
    dest_table,
    src_where_condition,
    dest_where_condition
):
    try:
        query = f"""
            SELECT
                CASE
                    WHEN SRC.COUNT_SRC = DEST.COUNT_DEST THEN 'OK'
                    ELSE 'NOT OK'
                END AS OUTPUT
            FROM ( SELECT COUNT(1) AS COUNT_SRC FROM {src_table} WHERE {src_where_condition} ) SRC
            LEFT JOIN ( SELECT COUNT(1) AS COUNT_DEST FROM {dest_table} WHERE {dest_where_condition} ) DEST
            ON
                1 = 1
        """

        result = spark.sql(query).collect()[0]['OUTPUT']
        
        return {
            "PARAM_ID": param_id,
            "QUERY": query,
            "RESULT": result,
        }
    except Exception as e:
        print(f"[!] Caught exception in test_data_volume: {e}")