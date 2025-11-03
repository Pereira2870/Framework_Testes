# Databricks notebook source
def test_data_volume(
    test_id,
    subtype_id,
    src_table,
    dest_table,
    src_where_condition,
    dest_where_condition,
    src_partition_filter_field
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
            "TEST_ID": test_id,
            "SUBTYPE_ID": subtype_id,
            "QUERY": query,
            "RESULT": result,
            "SRC_TABLE": src_table,
            "DEST_TABLE": dest_table,
            "SRC_PARTITION_FILTER_FIELD": src_partition_filter_field
        }

    except Exception as e:
        print(f"[!] Caught exception in test_data_volume: {e}")