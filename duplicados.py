# Databricks notebook source
def test_duplicates(
    param_id,
    test_id,
    test_type,
    src_table,
    src_where_condition,
    src_groupby,
    src_select_field
):
    try:
        query = f"""
            SELECT
                CASE
                    WHEN SRC.COUNT_SRC = 0 THEN 'OK'
                    ELSE 'NOT OK'
                END AS OUTPUT
            FROM (
                SELECT
                    COUNT(1) AS COUNT_SRC
                FROM (
                    SELECT {src_select_field}, COUNT(1) AS COUNT_DUPLICATES
                    FROM {src_table}
                    WHERE {src_where_condition}
                    GROUP BY {src_groupby}
                    HAVING
                        COUNT_DUPLICATES > 1
                ) AS DUPLICATES
            ) SRC
        """

        result = spark.sql(query).collect()[0]['OUTPUT']
        
        return {
            "PARAM_ID": param_id,
            "QUERY": query,
            "RESULT": result,
        }
    except Exception as e:
        print(f"[!] Caught exception in test_duplicates: {e}")