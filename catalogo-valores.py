def test_data_catalog(
    test_id,
    subtype_id,
    src_table,
    src_filter,
    src_partition_filter_field,
    key_fields
):
    try:
        query = ""
        query_src = f"SELECT {key_fields} FROM {src_table} WHERE {src_filter} group by {key_fields}"
        df_source = spark.sql(query_src)

        if df_source.count() > 0:
            result = 'NOK'
        else:
            result = 'OK'

        if key_fields != "":
            key_fields_split = [k.strip() for k in key_fields.split(",")]
            key_fields_list = [
                ", ".join([f"{k}='{str(row[k])}'" for k in key_fields_split])
                for row in df_source.collect()
            ]
        else:
            key_fields_list = []

        return {
            "TEST_ID": test_id,
            "SUBTYPE_ID": subtype_id,
            "QUERY": query,
            "RESULT": result,
            "KEY_FIELDS_LIST": key_fields_list,
            "SRC_PARTITION_FILTER_FIELD": src_partition_filter_field
        }
    except Exception as e:
        print(f"[!] Caught exception in test_data_catalog: {e}")