# Databricks notebook source
# MAGIC %run /Workspace/framework-testes/duplicados

# COMMAND ----------

# MAGIC %run /Workspace/framework-testes/catalogo-valores

# COMMAND ----------

# MAGIC %run /Workspace/framework-testes/mapeamentos

# COMMAND ----------

# MAGIC %run /Workspace/framework-testes/volumetria

# COMMAND ----------

# MAGIC %run /Workspace/framework-testes/logger

# COMMAND ----------

def parse_config_table():
    return spark.sql(
        f"""
            SELECT
                configs.PARAM_ID AS PARAM_ID,
                configs.TEST_ID AS TEST_ID,
                tests.TYPE AS TEST_TYPE,
                configs.SOURCE_TABLE AS SOURCE_TABLE,
                configs.DEST_TABLE AS DEST_TABLE,
                configs.JOIN_KEY AS JOIN_KEY,
                configs.SOURCE_SELECT_FIELD AS SOURCE_SELECT_FIELD,
                configs.DEST_SELECT_FIELD AS DEST_SELECT_FIELD,
                configs.SOURCE_FILTER AS SOURCE_FILTER,
                configs.DEST_FILTER AS DEST_FILTER,
                configs.SOURCE_GROUPBY AS SOURCE_GROUPBY,
                configs.DEST_GROUPBY AS DEST_GROUPBY
            FROM framework_testes.test_parameters configs
            LEFT JOIN framework_testes.tests tests
            ON
                configs.TEST_ID = tests.TEST_ID
            ORDER BY
                PARAM_ID
        """
    ).collect()

def parse_conditions(test_params_list):
    params_list = []

    for test_params in test_params_list:
        param_id = test_params['PARAM_ID']
        test_id = test_params['TEST_ID']
        test_type = test_params['TEST_TYPE']

        src_select_field = test_params['SOURCE_SELECT_FIELD']
        dest_select_field = test_params['DEST_SELECT_FIELD']

        if not src_select_field:
            src_select_field = '*'

        if not dest_select_field:
            dest_select_field = '*'

        source_table = test_params['SOURCE_TABLE']
        dest_table = test_params['DEST_TABLE']

        join_key = test_params['JOIN_KEY']
        
        if not join_key:
            join_key = '1 = 1'
            
        src_groupby = test_params['SOURCE_GROUPBY']
        dest_groupby = test_params['DEST_GROUPBY']

        src_filter = test_params['SOURCE_FILTER']
        dest_filter = test_params['DEST_FILTER']

        if not src_filter:
            src_filter = '1 = 1'

        if not dest_filter:
            dest_filter = '1 = 1'

        params_list.append({
            'PARAM_ID': param_id,
            'TEST_ID': test_id,
            'TEST_TYPE': test_type,
            'SOURCE_TABLE': source_table,
            'DEST_TABLE': dest_table,
            'JOIN_KEY': join_key,
            'SOURCE_SELECT_FIELD': src_select_field,
            'DEST_SELECT_FIELD': dest_select_field,
            'SOURCE_FILTER': src_filter,
            'DEST_FILTER': dest_filter,
            'SRC_GROUPBY': src_groupby,
            'DEST_GROUPBY': dest_groupby,
        })
                    
    return params_list

def run_framework():
    try:
        
        test_params_list = parse_config_table()
        tests = parse_conditions(test_params_list)
    
        if not tests:
            raise Exception("Test configuration table is empty!")

        for test in tests:
            param_id = test['PARAM_ID']
            test_id = test['TEST_ID']
            test_type = test['TEST_TYPE']
            src_table = test['SOURCE_TABLE']
            dest_table = test['DEST_TABLE']
            join_key = test['JOIN_KEY']
            src_select_field = test['SOURCE_SELECT_FIELD']
            dest_select_field = test['DEST_SELECT_FIELD']
            src_filter = test['SOURCE_FILTER']
            dest_filter = test['DEST_FILTER']
            src_groupby = test['SRC_GROUPBY']
            dest_groupby = test['DEST_GROUPBY']

            if test_id == 1:
                print("[!] Calling data volume workload...")
                result = test_data_volume(
                    param_id,
                    test_id,
                    test_type,
                    src_table,
                    dest_table,
                    src_filter,
                    dest_filter
                )
                print(f"[!] Finished data volume workload... - {result['RESULT']}")
            elif test_id == 2:
                print("[!] Calling data mapping workload...")
                result = test_data_mapping(
                    param_id,
                    test_id,
                    test_type,
                    src_table,
                    dest_table,
                    src_filter,
                    dest_filter,
                    join_key,
                    src_groupby,
                    dest_groupby,
                    src_select_field,
                    dest_select_field
                )
                print(f"[!] Finished data mapping workload... - {result['RESULT']}")
            elif test_id == 3:
                print("[!] Calling data catalog workload...")
                result = test_data_catalog(
                    param_id,
                    test_id,
                    test_type,
                    src_table,
                    src_filter
                )
                print(f"[!] Finished data catalog workload... - {result['RESULT']}")
            elif test_id == 4:
                print("[!] Calling data format workload...")
                result = test_data_catalog(
                    param_id,
                    test_id,
                    test_type,
                    src_table,
                    src_filter
                )
                print(f"[!] Finished data format workload... - {result['RESULT']}")
            elif test_id == 5:
                print("[!] Calling referential integrity workload...")
                result = test_data_mapping(
                    param_id,
                    test_id,
                    test_type,
                    src_table,
                    dest_table,
                    src_filter,
                    dest_filter,
                    join_key,
                    src_groupby,
                    dest_groupby,
                    src_select_field,
                    dest_select_field
                )
                print(f"[!] Finished referential integrity workload... - {result['RESULT']}")
            elif test_id == 6:
                print("[!] Calling mandatory fill workload...")
                result = test_data_catalog(
                    param_id,
                    test_id,
                    test_type,
                    src_table,
                    src_filter
                )
                print(f"[!] Finished mandatory fill workload... - {result['RESULT']}")
            elif test_id == 7:
                print("[!] Calling duplicates workload...")
                result = test_duplicates(
                    param_id,
                    test_id,
                    test_type,
                    src_table,
                    src_filter,
                    src_groupby,
                    src_select_field
                )
                print(f"[!] Finished duplicates workload... - {result['RESULT']}")
            else:
                raise Exception("Caught unknown test type!")

            insert_log(result)
    except Exception as e:
        print(f"[!] Caught exception in run_framework: {e}")

run_framework()