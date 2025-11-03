# Databricks notebook source
def query_table_fields(table):
    select_fields = ""

    columns = spark.sql(f"SHOW COLUMNS FROM {table}").collect()

    for col in columns:
        select_fields += f"{col[0]}, "

    return select_fields[:-2]

def format_select_fields(table, select_field):
    
    if select_field == "*":
        select_field = query_table_fields(table)
    
    fields = select_field.split(",")

    select_fields = ""
    outer_select = ""
    counter = 1

    field_dict = {}
    field_dict_opposite = {}

    for field in fields:
        if counter == 1:
            select_fields += f"{field.strip()} AS FIELD_{counter}"
            outer_select += f"""
                MIN(CASE
                    WHEN SRC.FIELD_{counter} = DEST.FIELD_{counter} THEN 'OK'
                    ELSE 'NOT OK'
                END) AS FIELD_{counter}
            """
        else:
            select_fields += f", {field.strip()} AS FIELD_{counter}"
            outer_select += f""",
                MIN(CASE
                    WHEN SRC.FIELD_{counter} = DEST.FIELD_{counter} THEN 'OK'
                    ELSE 'NOT OK'
                END) AS FIELD_{counter}
            """
            
        field_dict[field.strip()] = f"FIELD_{counter}" 
        field_dict_opposite[f"FIELD_{counter}"] = field.strip() 
        
        counter += 1

    return select_fields, outer_select, field_dict, field_dict_opposite

def format_join_key(join_key, field_dict):

    for field in field_dict:
        join_key = join_key.replace(field, field_dict[field])

    return join_key.upper()

def parse_output(output, src_field_dict_opposite, dst_field_dict_opposite):

    result = 'OK'
    dest_value = ''
    print(f'teste jpjp {enumerate(output)}')
    for index, col in enumerate(output):
        print(f'index {index}')
        print(f'col {col}')
        #if output['teste'] is None:
        #    if index == 0:
        #        dest_value += f'{src_field_dict_opposite[f"FIELD_{index+1}"]} = {dst_field_dict_opposite[f"FIELD_{index+1}"]}'
        #    else:
        #        dest_value += f'; {src_field_dict_opposite[f"FIELD_{index+1}"]} = {dst_field_dict_opposite[f"FIELD_{index+1}"]}'

        #    if col == 'NOT OK':
        #        result = 'NOT OK'
        #        dest_value += ' NOT OK'
        #    else:
        #        dest_value += ' OK'

    return result, dest_value

def test_data_mapping(
    test_id,
    subtype_id,
    src_table,
    dest_table,
    src_filter,
    dest_filter,
    join_key,
    src_groupby,
    dest_groupby,
    src_select_field,
    dest_select_field,
    src_partition_filter_field,
    key_fields
):
    try:
        # print(f'test_id: {test_id}')
        full_field_dict = {}
        
        src_fields, outer_select, field_dict, src_field_dict_opposite = format_select_fields(src_table, src_select_field)
        full_field_dict.update(field_dict)
        src_field = ", ".join([field.strip() for field in src_select_field.split(',') if field.strip() not in join_key])
        # print(f'src_field: {src_field}')

        dest_fields, outer_select, field_dict, dst_field_dict_opposite = format_select_fields(dest_table, dest_select_field)
        full_field_dict.update(field_dict)
        dest_field = ", ".join([field.strip() for field in dest_select_field.split(',') if field.strip() not in join_key])
        # print(f'dest_field: {dest_field}')

        # print(f'full_field_dict: {full_field_dict}')
        print(f'src_field_dict_opposite: {src_field_dict_opposite}')
        print(f'dst_field_dict_opposite: {dst_field_dict_opposite}')
        # print(f'src_field: {", ".join(src_field)}')
        # print(f'dest_field: {", ".join(dest_field)}')
        # print(f'src_select_field: {src_select_field}')
        # print(f'dest_select_field: {dest_select_field}')

        formatted_join_key = format_join_key(join_key, full_field_dict)

        src_groupby_exp = f"GROUP BY {src_groupby}" if src_groupby else ""
        dest_groupby_exp = f"GROUP BY {dest_groupby}" if dest_groupby else ""

        query = f"""
            SELECT {outer_select}
            FROM ( 
                SELECT {src_fields}
                FROM {src_table}
                WHERE
                    {src_filter}
                {src_groupby_exp}
            ) SRC
            LEFT JOIN (
                SELECT {dest_fields}
                FROM {dest_table}
                WHERE
                    {dest_filter}
                {dest_groupby_exp}
            ) DEST
            ON
                {formatted_join_key}
        """
        output = spark.sql(query).collect()[0]
        print(f"passou {output}")
        result, dest_value = parse_output(output, src_field_dict_opposite, dst_field_dict_opposite)
    
        #query = f"""
        #    SELECT {outer_select},SRC.{key_fields}
        #    FROM ( 
        #        SELECT {src_fields},{key_fields} 
        #        FROM {src_table}
        #        WHERE
        #            {src_filter}
        #        {src_groupby_exp}
        #    ) SRC
        #    LEFT JOIN (
        #        SELECT {dest_fields}
        #        FROM {dest_table}
        #        WHERE
        #            {dest_filter}
        #        {dest_groupby_exp}
        #    ) DEST
        #    ON
        #        {formatted_join_key}
        #    GROUP BY SRC.{key_fields}
        #"""
        
        #result_rows = spark.sql(query).collect()

        #if result_rows:
        #    key_fields = [
        #        {   
        #            "key_fields": row[1]
        #        }
        #        for row in result_rows
        #    ]

        return {
            "TEST_ID": test_id,
            "SUBTYPE_ID": subtype_id,
            "QUERY": query,
            "RESULT": result,
            "RESULT_DETAILS": dest_value,
            "SRC_PARTITION_FILTER_FIELD": src_partition_filter_field,
            "SRC_FIELD": src_field,
            "DEST_FIELD": dest_field,
            "KEY_FIELDS": key_fields

        }
    except Exception as e:
        print(f"[!] Caught exception in test_data_mapping: {e}")