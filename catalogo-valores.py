# Databricks notebook source
def test_data_catalog(
    test_id,
    subtype_id,
    src_table,
    src_filter,
    src_partition_filter_field,
    key_fields
):
    try:
        query = f"""
            SELECT
                CASE
                    WHEN SRC.COUNT = 0 THEN 'OK'
                    ELSE 'NOT OK'
                END AS OUTPUT,
                {key_fields} as key_fields
            FROM ( SELECT COUNT(1) AS COUNT, {key_fields} FROM {src_table} WHERE {src_filter} group by {key_fields} ) SRC
        """
        result_rows = spark.sql(query).collect()

        if result_rows:
            key_fields = [
                {   
                    "key_fields": row[1]
                }
                for row in result_rows
            ]
            result = "NOT OK"
        else:
            key_fields = []
            result = "OK"

        return {
            "TEST_ID": test_id,
            "SUBTYPE_ID": subtype_id,
            "QUERY": query,
            "RESULT": result,
            "SRC_PARTITION_FILTER_FIELD": src_partition_filter_field,
            "SRC_TABLE": src_table,
            "SRC_FILTER": src_filter,
            "KEY_FIELDS": key_fields
        }
    except Exception as e:
        print(f"[!] Caught exception in test_data_catalog: {e}")