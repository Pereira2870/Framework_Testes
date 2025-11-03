# Databricks notebook source
def insert_results(result):
    try:
        if not result:
            raise Exception("No result was sent for logging!")

        #VARIABLES
        test_id = result["TEST_ID"]
        query = result["QUERY"].replace("'", "''")
        results = result["RESULT"]
        src_partition_filter_field = result["SRC_PARTITION_FILTER_FIELD"]
        result_id = spark.sql("SELECT COALESCE(MAX(RESULT_ID), 0) + 1 FROM framework_testes.test_out_results").collect()[0][0]
        subtype_id = result["SUBTYPE_ID"]

        if result['RESULT'] !='OK':
            details_id = spark.sql("SELECT COALESCE(MAX(DETAILS_ID), 0) + 1 FROM framework_testes.test_out_results_details").collect()[0][0]
        else:
            details_id = None

        details_id = f"{details_id}" if details_id is not None else "NULL"
        
        #INSERT test_out_results
        spark.sql(
            f"""
                INSERT INTO framework_testes.test_out_results
                (RESULT_ID, TEST_ID, QUERY, RESULT, PARTITION_FILTER_VALUE, TIMESTAMP)
                VALUES (
                    {result_id},
                    {test_id},
                    "{query}",
                    "{results}",
                    "{src_partition_filter_field}",
                    current_timestamp()
                )
            """
        )

        #INSERT test_out_results_details
        if result['RESULT'] !='OK' and subtype_id !='VOL':
            key_fields = result["KEY_FIELDS"]
            print(f"key_fields marados {key_fields}")
            for teste in key_fields:
                key_field = teste["key_fields"]
                print(f"key_field {key_field}")
                spark.sql(
                    f"""
                        INSERT INTO framework_testes.test_out_results_details
                        (DETAILS_ID, RESULT_ID, KEY_FIELDS, TIMESTAMP)
                        VALUES (
                            {details_id},
                            {result_id},
                            "{key_field}",
                            current_timestamp()
                        )
                    """
                )
                details_id = int(details_id)  + 1

    except Exception as e:
        print(f"[!] Caught exception in insert_results: {e}")
    return 