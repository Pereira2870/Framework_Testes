from pyspark.sql import Row
import datetime
def insert_results(result):
    try:
        if not result:
            raise Exception("No result was sent for logging!")

        #VARIABLES
        test_id = result["TEST_ID"]
        query = result["QUERY"].replace("'", "''")
        results = result["RESULT"]
        src_partition_filter_field = result["SRC_PARTITION_FILTER_FIELD"]
        result_id = spark.sql("SELECT COALESCE(MAX(RESULT_ID), 0) + 1 FROM workbench_reportinghub.test_out_results").collect()[0][0]
        subtype_id = result["SUBTYPE_ID"]

        if result['RESULT'] !='OK':
            details_id = spark.sql("SELECT COALESCE(MAX(DETAILS_ID), 0) + 1 FROM workbench_reportinghub.test_out_results_details").collect()[0][0]
        else:
            details_id = None

        details_id = f"{details_id}" if details_id is not None else "NULL"
        

        #INSERT test_out_results_details
        now = datetime.datetime.now()
        results_row = [Row(
            RESULT_ID=result_id,
            TEST_ID=test_id,
            QUERY=query,
            RESULT=results,
            PARTITION_FILTER_VALUE=src_partition_filter_field,
            TIMESTAMP=now
        )]

        df_results = spark.createDataFrame(results_row).distinct()
        df_results.write.insertInto("workbench_reportinghub.test_out_results", overwrite=False)

        #INSERT test_out_results_details
        if result['RESULT'] !='OK' and subtype_id !='VOL':
            key_fields = result["KEY_FIELDS_LIST"]
            if key_fields != []:
                # Cria uma lista de Rows para o DataFrame, serializando KEY_FIELDS se necessário
                now = datetime.datetime.now()
                details_rows = [Row(
                    DETAILS_ID=int(details_id)+i,
                    RESULT_ID=result_id,
                    KEY_FIELDS=key,
                    TIMESTAMP=now
                ) for i, key in enumerate(key_fields)]
                df_details = spark.createDataFrame(details_rows)
                df_details = df_details.dropDuplicates(["KEY_FIELDS"])
                df_details = df_details.repartition(32)
                df_details.write.insertInto("workbench_reportinghub.test_out_results_details", overwrite=False)
                details_id = int(details_id) + len(key_fields)
            else:
                details_row = [Row(
                    DETAILS_ID=int(details_id),
                    RESULT_ID=result_id,
                    KEY_FIELDS="",
                    TIMESTAMP=now
                )]
                df_details = spark.createDataFrame(details_row)
                df_details = df_details.dropDuplicates(["KEY_FIELDS"])
                df_details.write.insertInto("workbench_reportinghub.test_out_results_details", overwrite=False)
                details_id = int(details_id)  + 1

    except Exception as e:
        print(f"[!] Caught exception in insert_results: {e}")
    return