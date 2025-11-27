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

def parse_config_table(test_config_set_list):
   
    if not test_config_set_list: #Se a lista de set's estiver vazia, não é aplicado nenhum filtro.
        test_config_set_filter = '1=1'
    else: #Se a lista de set's estiver prrenchida, são executados os test ids parametrizados.
        test_config_set_values = [f"'{test_config_set_value.TEST_ID}'" for test_config_set_value in test_config_set_list]
        test_config_set_filter = f"test_config_params.TEST_ID IN ({', '.join(test_config_set_values)})"
        
    return spark.sql(
        f"""
            SELECT
                test_config_params.TEST_ID AS TEST_ID,
                test_config_params.TYPE_ID AS TYPE_ID,
                test_config_params.SUBTYPE_ID AS SUBTYPE_ID,
                test_config_params.SOURCE_TABLE AS SOURCE_TABLE,
                test_config_params.DEST_TABLE AS DEST_TABLE,
                test_config_params.JOIN_KEY AS JOIN_KEY,
                test_config_params.SOURCE_SELECT_FIELD AS SOURCE_SELECT_FIELD,
                test_config_params.DEST_SELECT_FIELD AS DEST_SELECT_FIELD,
                test_config_params.SOURCE_FILTER AS SOURCE_FILTER,
                test_config_params.DEST_FILTER AS DEST_FILTER,
                test_config_params.SOURCE_GROUPBY AS SOURCE_GROUPBY,
                test_config_params.DEST_GROUPBY AS DEST_GROUPBY,
                test_config_params.SOURCE_FLAG_PARTITION_FILTER AS SOURCE_FLAG_PARTITION_FILTER,
                test_config_params.DEST_FLAG_PARTITION_FILTER AS DEST_FLAG_PARTITION_FILTER,
                test_config_params.SOURCE_PARTITION_FILTER_FIELD AS SOURCE_PARTITION_FILTER_FIELD,
                test_config_params.DEST_PARTITION_FILTER_FIELD AS DEST_PARTITION_FILTER_FIELD,
                test_config_params.key_fields AS KEY_FIELDS
            FROM workbench_reportinghub.test_config_parameters test_config_params
            WHERE {test_config_set_filter}
            ORDER BY
                TEST_ID
        """
    ).collect()

def parse_conditions(test_config_params_list):
    params_list = []

    for test_config_params in test_config_params_list:
        test_id = test_config_params['TEST_ID']
        subtype_id  = test_config_params['SUBTYPE_ID']


        src_select_field = test_config_params['SOURCE_SELECT_FIELD']
        dest_select_field = test_config_params['DEST_SELECT_FIELD']

        if not src_select_field:
            src_select_field = '*'

        if not dest_select_field:
            dest_select_field = '*'

        source_table = test_config_params['SOURCE_TABLE']
        dest_table = test_config_params['DEST_TABLE']

        join_key = test_config_params['JOIN_KEY']
        
        if not join_key:
            join_key = '1 = 1'
            
        src_filter = test_config_params['SOURCE_FILTER']
        dest_filter = test_config_params['DEST_FILTER']

        src_groupby = test_config_params['SOURCE_GROUPBY']

        if not src_groupby:
            src_groupby = '1 = 1'

        dest_groupby = test_config_params['DEST_GROUPBY']

        if not dest_groupby:
            dest_groupby = '1 = 1'

        if not src_filter:
            src_filter = '1 = 1'

        if not dest_filter:
            dest_filter = '1 = 1'

        src_flag_partition_filter = test_config_params['SOURCE_FLAG_PARTITION_FILTER']
        dest_flag_partition_filter = test_config_params['DEST_FLAG_PARTITION_FILTER']

        src_partition_filter_field = test_config_params['SOURCE_PARTITION_FILTER_FIELD']

        if not src_partition_filter_field:
            src_partition_filter_field = ''


        dest_partition_filter_field = test_config_params['DEST_PARTITION_FILTER_FIELD']

        if not dest_partition_filter_field:
            dest_partition_filter_field = ''

        key_fields = test_config_params['KEY_FIELDS']

        params_list.append({
            'TEST_ID': test_id,
            'SUBTYPE_ID': subtype_id,
            'SOURCE_TABLE': source_table,
            'DEST_TABLE': dest_table,
            'JOIN_KEY': join_key,
            'SOURCE_SELECT_FIELD': src_select_field,
            'DEST_SELECT_FIELD': dest_select_field,
            'SOURCE_FILTER': src_filter,
            'DEST_FILTER': dest_filter,
            'SRC_GROUPBY': src_groupby,
            'DEST_GROUPBY': dest_groupby,
            'SOURCE_FLAG_PARTITION_FILTER': src_flag_partition_filter,
            'DEST_FLAG_PARTITION_FILTER': dest_flag_partition_filter,
            'SOURCE_PARTITION_FILTER_FIELD': src_partition_filter_field,
            'DEST_PARTITION_FILTER_FIELD': dest_partition_filter_field,
            'KEY_FIELDS': key_fields
        })
                    
    return params_list

def parse_config_set ():
    return spark.sql(
        f"""
            SELECT
                test_rel_set_parameter.TEST_ID AS TEST_ID
            FROM workbench_reportinghub.test_rel_set_parameter test_rel_set_parameter
            ORDER BY
                TEST_ID
        """
    ).collect()

def run_framework(set_list=[]):
    try:
        test_config_set_list = parse_config_set()
        test_config_params_list = parse_config_table(test_config_set_list)
        tests = parse_conditions(test_config_params_list)
    
        if not tests:
            raise Exception("Test configuration table is empty!")

        for test in tests:
            test_id = test['TEST_ID']
            subtype_id = test['SUBTYPE_ID']
            src_table = test['SOURCE_TABLE']
            dest_table = test['DEST_TABLE']
            join_key = test['JOIN_KEY']
            src_select_field = test['SOURCE_SELECT_FIELD']
            dest_select_field = test['DEST_SELECT_FIELD']
            src_filter = test['SOURCE_FILTER']
            dest_filter = test['DEST_FILTER']
            src_groupby = test['SRC_GROUPBY']
            dest_groupby = test['DEST_GROUPBY']
            src_flag_partition_filter = test['SOURCE_FLAG_PARTITION_FILTER']
            dst_flag_partition_filter = test['DEST_FLAG_PARTITION_FILTER']
            src_partition_filter_field = test['SOURCE_PARTITION_FILTER_FIELD']
            dest_partition_filter_field = test['DEST_PARTITION_FILTER_FIELD']
            key_fields = test['KEY_FIELDS']

            if subtype_id == 'VOL':
                print("[!] Calling data volume workload...")
                print(f"[!] TEST ID - {test_id}")
                result = test_data_volume(
                    test_id,
                    subtype_id,
                    src_table,
                    dest_table,
                    src_filter,
                    dest_filter,
                    src_partition_filter_field,
                    dest_partition_filter_field
                )
                print(f"[!] Finished data volume workload... - {result['RESULT']}")
            elif subtype_id == 'CON':
                print("[!] Calling data mapping workload...")
                print(f"[!] TEST ID - {test_id}")
                result = test_data_mapping(
                    test_id,
                    subtype_id,
                    src_table,
                    dest_table,
                    join_key,
                    src_select_field,
                    dest_select_field,
                    src_filter,
                    dest_filter,
                    src_groupby,
                    dest_groupby,
                    src_partition_filter_field,
                    dest_partition_filter_field,
                    key_fields
                )
                print(f"[!] Finished data mapping workload... - {result['RESULT']}")
            elif subtype_id == 'CAT':
                print("[!] Calling data catalog workload...")
                print(f"[!] TEST ID - {test_id}")
                result = test_data_catalog(
                    test_id,
                    subtype_id,
                    src_table,
                    src_filter,
                    src_partition_filter_field,
                    key_fields
                )
                print(f"[!] Finished data catalog workload... - {result['RESULT']}")
            elif subtype_id == 'FORM':
                print("[!] Calling data format workload...")
                print(f"[!] TEST ID - {test_id}")
                result = test_data_catalog(
                    test_id,
                    subtype_id,
                    src_table,
                    src_filter,
                    src_partition_filter_field,
                    key_fields
                )
                print(f"[!] Finished data format workload... - {result['RESULT']}")
            elif subtype_id == 'REF':
                print("[!] Calling referential integrity workload...")
                print(f"[!] TEST ID - {test_id}")
                result = test_data_mapping(
                    test_id,
                    subtype_id,
                    src_table,
                    dest_table,
                    join_key,
                    src_select_field,
                    dest_select_field,
                    src_filter,
                    dest_filter,
                    src_groupby,
                    dest_groupby,
                    src_partition_filter_field,
                    dest_partition_filter_field,
                    key_fields
                )
                print(f"[!] Finished referential integrity workload... - {result['RESULT']}")
            elif subtype_id == 'OBG':
                print("[!] Calling mandatory fill workload...")
                print(f"[!] TEST ID - {test_id}")
                result = test_data_catalog(
                    test_id,
                    subtype_id,
                    src_table,
                    src_filter,
                    src_partition_filter_field,
                    key_fields
                )
                print(f"[!] Finished mandatory fill workload... - {result['RESULT']}")
            elif subtype_id == 'DUP':
                print("[!] Calling duplicates workload...")
                print(f"[!] TEST ID - {test_id}")
                result = test_duplicates(
                    test_id,
                    subtype_id,
                    src_table,
                    src_filter,
                    src_groupby,
                    src_select_field,
                    src_partition_filter_field,
                    key_fields
                )
                print(f"[!] Finished duplicates workload... - {result['RESULT']}")
            else:
                raise Exception("Caught unknown test type!")
            
            insert_results(result)
        print(f"[!] Framwork run with success.")
    except Exception as e:
        print(f"[!] Caught exception in run_framework: {e}")

run_framework()