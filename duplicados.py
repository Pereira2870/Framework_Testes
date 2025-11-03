# Databricks notebook source
def test_duplicates(
    test_id,
    subtype_id,
    src_table,
    src_filter,
    src_groupby,
    src_select_field,
    src_partition_filter_field,
    key_fields

):
    try:
        query = f"""
            WITH duplicates AS (
                SELECT
                    {src_select_field},
                    {key_fields},
                    COUNT(*) AS count_duplicates
                FROM {src_table}
                WHERE {src_filter}
                GROUP BY {src_groupby},{key_fields}
                HAVING COUNT(*) > 1
            )
            SELECT
                CASE
                    WHEN EXISTS (SELECT 1 FROM duplicates) THEN 'NOT OK'
                    ELSE 'OK'
                END AS result,
                {src_select_field} as src_select_field,
                {key_fields} as key_fields,
                count_duplicates
            FROM duplicates
        """
        result_rows = spark.sql(query).collect()

        if result_rows:
            key_fields = [
                {   
                    "source_field" : row[1],
                    "key_fields": row[2]
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
            "KEY_FIELDS": key_fields,
            "SRC_TABLE": src_table,
            "SRC_PARTITION_FILTER_FIELD": src_partition_filter_field
        }
    except Exception as e:
        print(f"[!] Caught exception in test_duplicates: {e}")