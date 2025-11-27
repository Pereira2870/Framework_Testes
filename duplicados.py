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
        query = ""
        # Seleciona apenas as colunas necessárias antes de qualquer operação
        select_cols = [c.strip() for c in (src_select_field + ',' + key_fields).split(',') if c.strip()]
        df = spark.table(src_table).select(*select_cols)

        # Aplica filtro se houver
        if src_filter:
            df = df.filter(src_filter)

        # Agrupa e conta duplicados, só aplica src_groupby se for diferente de '1 = 1'
        if src_groupby and src_groupby.strip() != '1 = 1':
            groupby_cols = [c.strip() for c in (src_groupby + ',' + key_fields).split(',') if c.strip()]
        else:
            groupby_cols = [c.strip() for c in key_fields.split(',') if c.strip()]
        df_grouped = df.groupBy(*groupby_cols).count()

        # Filtra apenas os grupos com mais de 1 ocorrência
        df_duplicates = df_grouped.filter("count > 1")

        # Reparticiona para otimizar processamento em grandes volumes
        df_duplicates = df_duplicates.repartition(128)  # Ajuste o número conforme seu cluster

        result_rows = df_duplicates.collect()

        if result_rows:
            key_fields_list = [
                ", ".join([f"{k}='{row[k]}'" for k in groupby_cols])
                for row in result_rows
            ]
            result = "NOT OK"
        else:
            key_fields_list = []
            result = "OK"

        return {
            "TEST_ID": test_id,
            "SUBTYPE_ID": subtype_id,
            "QUERY": query,
            "RESULT": result,
            "KEY_FIELDS_LIST": key_fields_list,
            "SRC_TABLE": src_table,
            "SRC_PARTITION_FILTER_FIELD": src_partition_filter_field
        }
    except Exception as e:
        print(f"[!] Caught exception in test_duplicates: {e}")