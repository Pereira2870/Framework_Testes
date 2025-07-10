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
        
        counter += 1

    return select_fields, outer_select, field_dict

def format_join_key(join_key, field_dict):
    for field in field_dict:
        join_key = join_key.replace(field, field_dict[field])

    return join_key.upper()

def parse_output(output):
    for col in output:
        if col == 'NOT OK':
            return 'NOT OK'
    
    return 'OK'

def test_data_mapping(
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
):
    try:
        full_field_dict = {}
        
        src_fields, outer_select, field_dict = format_select_fields(src_table, src_select_field)
        full_field_dict.update(field_dict)

        dest_fields, outer_select, field_dict = format_select_fields(dest_table, dest_select_field)
        full_field_dict.update(field_dict)

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
                
        print(f"Generated query: {query}")

        output = spark.sql(query).collect()[0]
        result = parse_output(output)

        return {
            "PARAM_ID": param_id,
            "QUERY": query,
            "RESULT": result,
        }
    except Exception as e:
        print(f"[!] Caught exception in test_data_mapping: {e}")