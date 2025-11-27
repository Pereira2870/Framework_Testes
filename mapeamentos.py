from pyspark.sql.functions import col, when, lit
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def test_data_mapping(
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
):
    try:
        query = ""
        colunas_a = []
        colunas_b = []
        cols_source = ""
        cols_dest = ""

        if src_select_field == "*":
            query_source = "SELECT * FROM {0} WHERE {1}".format(src_table,  src_filter)
            cols_source = extrair_colunas_cursor_sql(query_source)
        else:
            cols_source = src_select_field

        if dest_select_field == "*":
            query_dest = "SELECT * FROM {0} WHERE {1}".format(dest_table, dest_filter)
            cols_dest = extrair_colunas_cursor_sql(query_dest)
        else:
            cols_dest = dest_select_field

        if key_fields is None:
            key_fields = ""
        else:
            key_fields = key_fields
        
        # Corrige para garantir que colunas_a e colunas_b sejam listas de nomes de colunas
        if isinstance(cols_source, str):
            colunas_a.extend([c.strip() for c in cols_source.split(",")])
        else:
            colunas_a.extend(cols_source)

        if isinstance(cols_dest, str):
            colunas_b.extend([c.strip() for c in cols_dest.split(",")])
        else:
            colunas_b.extend(cols_dest)

        if src_select_field == "*":
            if src_groupby == "1 = 1":
                query1_str = "SELECT {0} FROM {1} WHERE {2}".format(', '.join  (colunas_a),src_table, src_filter)
            else:
                query1_str = "SELECT {0} FROM {1} WHERE {2} GROUP BY {3}".format(', '.join  (colunas_a),src_table, src_filter, src_groupby)
        else:
            if src_groupby == "1 = 1":
                query1_str = "SELECT {0} FROM {1} WHERE {2}".format(src_select_field,src_table, src_filter)
            else:
                query1_str = "SELECT {0} FROM {1} WHERE {2} GROUP BY {3}".format(src_select_field,src_table, src_filter, src_groupby)
        
        if dest_select_field == "*":
            if dest_groupby == "1 = 1":
                query2_str = "SELECT {0} FROM {1} WHERE {2}".format(', '.join  (colunas_b),dest_table, dest_filter)
            else:
                query2_str = "SELECT {0} FROM {1} WHERE {2} GROUP BY {3}".format(', '.join  (colunas_b),dest_table, dest_filter, dest_groupby)
        else:
            if dest_groupby == "1 = 1":
                query2_str = "SELECT {0} FROM {1} WHERE {2}".format(dest_select_field,dest_table, dest_filter)
            else:
                query2_str = "SELECT {0} FROM {1} WHERE {2} GROUP BY {3}".format(dest_select_field,dest_table, dest_filter, dest_groupby  )

        # Cria os DataFrames
        df_source = spark.sql(query1_str)
        df_dest = spark.sql(query2_str)

        if join_key != "1 = 1":
            # Suporta múltiplas condições de junção: "colA = colA and colB = colB ..."
            try:
                join_segments = [seg.strip() for seg in join_key.split("and") if seg.strip()]
                if not join_segments:
                    raise ValueError("join_key vazio após processamento.")
                conditions = []
                for seg in join_segments:
                    if "=" not in seg:
                        raise ValueError(f"Segmento inválido na cláusula de junção: '{seg}'")
                    left_col, right_col = [s.strip() for s in seg.split("=", 1)]
                    # Usa aliases src/dest para evitar ambiguidade
                    conditions.append(col(f"src.{left_col}") == col(f"dest.{right_col}"))
                # Combina condições com AND lógico
                join_condition = reduce(lambda a, b: a & b, conditions)
                df_join = df_source.alias("src").join(
                    df_dest.alias("dest"),
                    on=join_condition,
                    how="outer"
                )
            except Exception as je:
                raise Exception(f"Erro ao processar join_key '{join_key}': {je}")
        else:
            df_join = df_source.crossJoin(df_dest)

        # Seleciona colunas com sufixos para comparação
        select_cols = []
        if join_key == "1 = 1":
            for ca, cb in zip(colunas_a, colunas_b):
                select_cols.append(col(f"{ca}").alias(f"{ca}_src"))
                select_cols.append(col(f"{cb}").alias(f"{cb}_dest"))
        else:
            for ca, cb in zip(colunas_a, colunas_b):
                select_cols.append(col(f"src.{ca}").alias(f"{ca}_src"))
                select_cols.append(col(f"dest.{cb}").alias(f"{cb}_dest"))
        
        df_comparado_val = df_join.select(*select_cols)

        # Gera expressão para identificar diferenças
        condicoes_diferenca = [
            col(f"{ca}_src") != col(f"{cb}_dest") for ca, cb in zip(colunas_a, colunas_b)
        ]
        
        # Adiciona coluna de status
        df_status_val = df_comparado_val.withColumn(
            "status",
            when(reduce(lambda x, y: x | y, condicoes_diferenca), lit("NO MATCH"))
            .when(reduce(lambda x, y: x & y, [col(f"{cb}_dest").isNull() for cb in colunas_b]), lit("NO MATCH @ ONLY TABELA A"))
            .when(reduce(lambda x, y: x & y, [col(f"{ca}_src").isNull() for ca in colunas_a]), lit("NO MATCH @ ONLY TABELA B"))
            .otherwise(lit("MATCH"))
        )
    
        # Filtra apenas diferenças
        df_diferencas_calc = df_status_val.filter(col("status") != "MATCH")
        df_diferencas_calc.show()

        if key_fields != "":
            # Suporta múltiplas chaves separadas por vírgula: "c1,c2,c3"
            keys = [k.strip() for k in key_fields.split(",") if k.strip()]
            if keys:
                # Define a função auxiliar para montar o predicado
                def build_predicate(row):
                    parts = []
                    for k in keys:
                        val = row[f"{k}_src"]
                        if val is None:
                            parts.append(f"{k} IS NULL")
                        else:
                            sval = str(val).replace("'", "''")
                            parts.append(f"{k}='{sval}'")
                    return " , ".join(parts)

                if subtype_id == 'REF':
                    # Para REF: somente chaves que não existem no destino
                    # key_fields pode ter nomes diferentes dos usados no join_key.
                    # Construímos um mapa src->dest a partir do join_key para localizar o nome correto da coluna no destino.
                    try:
                        join_segments = [seg.strip() for seg in str(join_key).split("and") if seg.strip()]
                        pairs = []
                        for seg in join_segments:
                            if "=" in seg:
                                left_col, right_col = [s.strip() for s in seg.split("=", 1)]
                                pairs.append((left_col, right_col))
                        # Mapa de coluna origem para coluna destino
                        src_to_dest = {src: dest for src, dest in pairs}
                    except Exception:
                        src_to_dest = {}

                    # Para cada chave fornecida, procura a coluna correspondente no destino; se não houver, usa o próprio nome
                    dest_key_cols = [src_to_dest.get(k, k) for k in keys]
                    only_src_mask = reduce(lambda x, y: x & y, [col(f"{dk}_dest").isNull() for dk in dest_key_cols])
                    df_keys = df_diferencas_calc.filter(only_src_mask)
                else:
                    # Outros subtipos: manter comportamento anterior (todas as diferenças)
                    df_keys = df_diferencas_calc

                select_cols_keys = [f"{k}_src" for k in keys]
                rows = df_keys.select(*select_cols_keys).collect()
                key_fields_list = [build_predicate(r) for r in rows]
            else:
                key_fields_list = []
        else:
            key_fields_list = []
            
        if df_diferencas_calc.count() >= 1:
            result = "NOK"
        else:
            result = "OK"
    
        return {
            "TEST_ID": test_id,
            "SUBTYPE_ID": subtype_id,
            "QUERY": query,
            "RESULT": result,
            "SRC_FIELD": src_select_field,
            "DEST_FIELD": dest_select_field,
            "KEY_FIELDS_LIST": key_fields_list,
            "SRC_PARTITION_FILTER_FIELD": src_partition_filter_field,
            "DEST_PARTITION_FILTER_FIELD": dest_partition_filter_field
        }
    except Exception as e:
        print(f"[!] Caught exception in test_data_mapping: {e}")

def extrair_colunas_cursor_sql(query, chave_cruzamento=None, limite=None):
    chave_cruzamento = chave_cruzamento or []
    df_query = spark.sql(query)
    colunas = [field.name for field in df_query.schema.fields]
    colunas_validas = [col for col in colunas if col and col != '# col_name']
    colunas_filtradas = [col for col in colunas_validas if col not in chave_cruzamento]
    if limite in colunas_filtradas:
        colunas_filtradas = colunas_filtradas[:colunas_filtradas.index(limite)]
    return colunas_filtradas