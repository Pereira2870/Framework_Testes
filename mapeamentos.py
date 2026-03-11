from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

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
    key_fields,
    user_input
):
    """
    Compara dados entre duas tabelas (source e destination) e identifica diferenças.
    
    Parâmetros:
        test_id: Identificador do teste
        subtype_id: Tipo de teste (REF para referência, CON para conteúdo)
        src_table: Tabela de origem
        dest_table: Tabela de destino
        join_key: Chave de junção entre as tabelas ("1 = 1" para CROSS JOIN)
        src_select_field: Campos a selecionar da tabela origem ("*" para todos)
        dest_select_field: Campos a selecionar da tabela destino ("*" para todos)
        src_filter: Filtro WHERE para tabela origem
        dest_filter: Filtro WHERE para tabela destino
        src_groupby: GROUP BY para tabela origem ("1 = 1" se não houver agrupamento)
        dest_groupby: GROUP BY para tabela destino ("1 = 1" se não houver agrupamento)
        key_fields: Campos chave para identificar registros com diferenças
        user_input: Parâmetros de execução fornecidos pelo usuário (substitui ? nos filtros)
    
    Retorna:
        Dicionário com resultados do teste incluindo:
        - TEST_ID: ID do teste
        - SUBTYPE_ID: Tipo do teste
        - QUERY: Query SQL executada
        - RESULT: "OK" se sem diferenças, "NOK" se houver diferenças
        - SRC_FIELD: Campos source usados
        - DEST_FIELD: Campos destination usados
        - KEY_FIELDS_LIST: Lista de chaves dos registros com diferenças
        - EXECUTION_PARAMETER: Parâmetros aplicados
        
        Retorna None em caso de erro.
    """
    try:
        # Processa o user_input para extrair o campo e valor dos parâmetros dinâmicos
        # Substitui placeholders (?) nos filtros pelos valores reais fornecidos pelo usuário
        execution_parameter = []
        if user_input:
            import re
            partitions = re.findall(r'\(([^)]+)\)|([^,]+)', user_input)
            partitions = [p[0] if p[0] else p[1] for p in partitions]
            partitions = [p.strip() for p in partitions if p.strip()]
            
            for partition in partitions:
                # Extrai o nome do campo e seu valor
                match = re.search(r"(\w+)\s*=\s*(.+)", partition)
                if match:
                    field_name = match.group(1).strip()
                    field_value = match.group(2).strip()
                    # Cria padrão regex para encontrar o placeholder campo=?
                    pattern = rf'\b{re.escape(field_name)}\s*=\s*\?'
                    
                    # Substitui o placeholder no filtro source, se existir
                    if re.search(pattern, src_filter, re.IGNORECASE):
                        src_filter = re.sub(pattern, f"{field_name}={field_value}", src_filter, flags=re.IGNORECASE)
                        execution_parameter.append(f"{field_name}={field_value}")
                    
                    # Substitui o placeholder no filtro destination, se existir
                    if re.search(pattern, dest_filter, re.IGNORECASE):
                        dest_filter = re.sub(pattern, f"{field_name}={field_value}", dest_filter, flags=re.IGNORECASE)
                        # Evita duplicação na lista de parâmetros
                        if f"{field_name}={field_value}" not in execution_parameter:
                            execution_parameter.append(f"{field_name}={field_value}")
        
        # Converte lista de parâmetros executados em string para retorno
        execution_parameter_str = " | ".join(execution_parameter) if execution_parameter else "N/A"
        
        # Extrai colunas das tabelas source e destination
        colunas_a = []  # Colunas da tabela source
        colunas_b = []  # Colunas da tabela destination

        # Se o campo source for "*", extrai todas as colunas da tabela via schema
        if src_select_field == "*":
            query_source = f"SELECT * FROM {src_table} WHERE {src_filter} LIMIT 0"
            cols_source = extrair_colunas_cursor_sql(query_source)
        else:
            cols_source = src_select_field

        # Se o campo destination for "*", extrai todas as colunas da tabela via schema
        if dest_select_field == "*":
            query_dest = f"SELECT * FROM {dest_table} WHERE {dest_filter} LIMIT 0"
            cols_dest = extrair_colunas_cursor_sql(query_dest)
        else:
            cols_dest = dest_select_field

        # Garante que key_fields seja sempre uma string
        if key_fields is None:
            key_fields = ""
        
        # Converte colunas source em lista (pode vir como string CSV ou lista)
        if isinstance(cols_source, str):
            colunas_a.extend([c.strip() for c in cols_source.split(",")])
        else:
            colunas_a.extend(cols_source)

        # Converte colunas destination em lista (pode vir como string CSV ou lista)
        if isinstance(cols_dest, str):
            colunas_b.extend([c.strip() for c in cols_dest.split(",")])
        else:
            colunas_b.extend(cols_dest)

        # Monta as queries source e dest com aliases para funções de agregação
        # Funções como COUNT(*), SUM(campo) precisam de alias para serem referenciadas
        # Processa colunas source adicionando aliases onde necessário
        src_cols_parts = []
        for ca in colunas_a:
            if '(' in ca:
                # É uma função de agregação (ex: COUNT(*), SUM(valor)) - adiciona alias
                ca_alias = ca.replace('(', '_').replace(')', '').replace(',', '_').replace(' ', '')
                src_cols_parts.append(f"{ca} as {ca_alias}")
            else:
                src_cols_parts.append(ca)
        src_cols = ', '.join(src_cols_parts)
        
        # Processa colunas destination da mesma forma
        dest_cols_parts = []
        for cb in colunas_b:
            if '(' in cb:
                # É uma função de agregação - adiciona alias
                cb_alias = cb.replace('(', '_').replace(')', '').replace(',', '_').replace(' ', '')
                dest_cols_parts.append(f"{cb} as {cb_alias}")
            else:
                dest_cols_parts.append(cb)
        dest_cols = ', '.join(dest_cols_parts)
        
        # Monta query source - adiciona GROUP BY apenas se especificado
        # "1 = 1" indica que não há agrupamento
        if src_groupby == "1 = 1":
            query_src = f"SELECT {src_cols} FROM {src_table} WHERE {src_filter}"
        else:
            query_src = f"SELECT {src_cols} FROM {src_table} WHERE {src_filter} GROUP BY {src_groupby}"
        
        # Monta query destination - adiciona GROUP BY apenas se especificado
        if dest_groupby == "1 = 1":
            query_dest = f"SELECT {dest_cols} FROM {dest_table} WHERE {dest_filter}"
        else:
            query_dest = f"SELECT {dest_cols} FROM {dest_table} WHERE {dest_filter} GROUP BY {dest_groupby}"
        
        # Monta a query de comparação entre source e destination
        # Duas estratégias: FULL OUTER JOIN (se há chave) ou CROSS JOIN (sem chave)
        if join_key != "1 = 1":
            # Há chave de junção definida - usa FULL OUTER JOIN
            # Processa join_key (formato: "col1=col2 and col3=col4") para SQL
            join_segments = [seg.strip() for seg in join_key.split("and") if seg.strip()]
            join_conditions = []
            for seg in join_segments:
                if "=" in seg:
                    left_col, right_col = [s.strip() for s in seg.split("=", 1)]
                    join_conditions.append(f"src.{left_col} = dest.{right_col}")
            join_clause = " AND ".join(join_conditions)
            
            # Seleciona colunas com sufixos _src e _dest para distinguir origem
            # Usa os aliases criados anteriormente para funções de agregação
            select_parts = []
            comparison_parts = []  # Condições para verificar se valores diferem
            for ca, cb in zip(colunas_a, colunas_b):
                # Determina o alias usado na subquery
                if '(' in ca:
                    ca_alias = ca.replace('(', '_').replace(')', '').replace(',', '_').replace(' ', '')
                    select_parts.append(f"src.{ca_alias} as {ca_alias}_src")
                    ca_ref = ca_alias
                else:
                    select_parts.append(f"src.{ca} as {ca}_src")
                    ca_ref = ca
                
                if '(' in cb:
                    cb_alias = cb.replace('(', '_').replace(')', '').replace(',', '_').replace(' ', '')
                    select_parts.append(f"dest.{cb_alias} as {cb_alias}_dest")
                    cb_ref = cb_alias
                else:
                    select_parts.append(f"dest.{cb} as {cb}_dest")
                    cb_ref = cb
                
                comparison_parts.append(f"{ca_ref}_src != {cb_ref}_dest OR ({ca_ref}_src IS NULL AND {cb_ref}_dest IS NOT NULL) OR ({ca_ref}_src IS NOT NULL AND {cb_ref}_dest IS NULL)")
            
            select_clause = ", ".join(select_parts)
            comparison_clause = ' OR '.join(comparison_parts)
            
            # Gera as cláusulas IS NULL para determinar o status de cada registro
            # Status possíveis: MATCH, NO MATCH, NO MATCH @ ONLY TABELA A, NO MATCH @ ONLY TABELA B
            dest_null_checks = []  # Verifica se registro existe só em source
            src_null_checks = []   # Verifica se registro existe só em destination
            for cb in colunas_b:
                if '(' in cb:
                    cb_alias = cb.replace('(', '_').replace(')', '').replace(',', '_').replace(' ', '')
                    dest_null_checks.append(f"{cb_alias}_dest IS NULL")
                else:
                    dest_null_checks.append(f"{cb}_dest IS NULL")
            
            for ca in colunas_a:
                if '(' in ca:
                    ca_alias = ca.replace('(', '_').replace(')', '').replace(',', '_').replace(' ', '')
                    src_null_checks.append(f"{ca_alias}_src IS NULL")
                else:
                    src_null_checks.append(f"{ca}_src IS NULL")
            
            # Query com FULL OUTER JOIN para capturar registros de ambas as tabelas
            if subtype_id == 'REF':
                # Para REF (Referencial): verifica apenas se o registro EXISTE no destino
                # Não compara valores das colunas, apenas a presença do registro
                # Usado para validar integridade referencial entre tabelas
                query = f"""
            SELECT {select_clause},
                CASE
                    WHEN {' AND '.join(dest_null_checks)}
                    THEN 'NO MATCH @ ONLY TABELA A'
                    WHEN {' AND '.join(src_null_checks)}
                    THEN 'NO MATCH @ ONLY TABELA B'
                    ELSE 'MATCH'
                END as status
            FROM ({query_src}) src
            FULL OUTER JOIN ({query_dest}) dest
            ON {join_clause}
            """
            else:
                # Para CON (Conteúdo) e outros: compara VALORES das colunas
                # Identifica diferenças nos dados, não apenas na existência
                query = f"""
            SELECT {select_clause},
                CASE
                    WHEN {comparison_clause}
                    THEN 'NO MATCH'
                    WHEN {' AND '.join(dest_null_checks)}
                    THEN 'NO MATCH @ ONLY TABELA A'
                    WHEN {' AND '.join(src_null_checks)}
                    THEN 'NO MATCH @ ONLY TABELA B'
                    ELSE 'MATCH'
                END as status
            FROM ({query_src}) src
            FULL OUTER JOIN ({query_dest}) dest
            ON {join_clause}
            """
        else:
            # Não há chave de junção (join_key = "1 = 1") - usa CROSS JOIN
            # Usado tipicamente para comparar valores agregados ou totalizadores
            # Exemplo: total de registros, soma de valores, etc.
            select_parts = []
            comparison_parts = []
            # Processa colunas para CROSS JOIN (geralmente funções de agregação)
            for ca, cb in zip(colunas_a, colunas_b):
                # Se a coluna contém parênteses, é uma função de agregação
                if '(' in ca:
                    ca_alias = ca.replace('(', '_').replace(')', '').replace(',', '_').replace(' ', '')
                    select_parts.append(f"src.{ca_alias} as {ca_alias}_src")
                    ca_ref = ca_alias
                else:
                    select_parts.append(f"src.{ca} as {ca}_src")
                    ca_ref = ca
                
                if '(' in cb:
                    cb_alias = cb.replace('(', '_').replace(')', '').replace(',', '_').replace(' ', '')
                    select_parts.append(f"dest.{cb_alias} as {cb_alias}_dest")
                    cb_ref = cb_alias
                else:
                    select_parts.append(f"dest.{cb} as {cb}_dest")
                    cb_ref = cb
                
                comparison_parts.append(f"{ca_ref}_src != {cb_ref}_dest OR ({ca_ref}_src IS NULL AND {cb_ref}_dest IS NOT NULL) OR ({ca_ref}_src IS NOT NULL AND {cb_ref}_dest IS NULL)")
            
            select_clause = ", ".join(select_parts)
            comparison_clause = ' OR '.join(comparison_parts)
            
            # Query com CROSS JOIN - compara uma linha de cada tabela
            # Usado quando não há chave de relacionamento (ex: comparar contadores)
            query = f"""
            SELECT {select_clause},
                CASE
                    WHEN {comparison_clause}
                    THEN 'NO MATCH'
                    ELSE 'MATCH'
                END as status
            FROM ({query_src}) src
            CROSS JOIN ({query_dest}) dest
            """
        
        # Executa a query de comparação no Spark
        df_result = spark.sql(query)
        # Filtra apenas registros com diferenças (status != MATCH)
        df_diferencas = df_result.filter("status != 'MATCH'")
        
        # Constrói lista de chaves dos registros com diferenças para análise detalhada
        # Permite identificar exatamente quais registros falharam no teste
        key_fields_list = []
        if key_fields != "":
            # Extrai lista de campos chave (separados por vírgula)
            keys = [k.strip() for k in key_fields.split(",") if k.strip()]
            if keys:
                if subtype_id == 'REF':
                    # Para REF: captura somente chaves de registros que NÃO existem no destino
                    # Monta filtro para identificar registros apenas em source
                    try:
                        # Cria mapeamento entre colunas source e destination baseado no join_key
                        join_segments = [seg.strip() for seg in str(join_key).split("and") if seg.strip()]
                        src_to_dest = {}
                        for seg in join_segments:
                            if "=" in seg:
                                left_col, right_col = [s.strip() for s in seg.split("=", 1)]
                                src_to_dest[left_col] = right_col
                    except Exception:
                        src_to_dest = {}
                    
                    # Mapeia key fields para nomes destination e filtra onde dest IS NULL
                    dest_key_cols = [src_to_dest.get(k, k) for k in keys]
                    filter_cond = " AND ".join([f"{dk}_dest IS NULL" for dk in dest_key_cols])
                    df_keys = df_diferencas.filter(filter_cond)
                else:
                    # Para outros tipos (CON, etc): considera todas as diferenças
                    df_keys = df_diferencas
                
                # Seleciona apenas as colunas chave dos registros com diferenças
                select_cols_keys = [f"{k}_src" for k in keys]
                rows = df_keys.select(*select_cols_keys).collect()
                
                # Formata cada registro como string "campo1=valor1 , campo2=valor2"
                for row in rows:
                    parts = []
                    for k in keys:
                        val = row[f"{k}_src"]
                        if val is None:
                            # Valor NULL - usa sintaxe IS NULL
                            parts.append(f"{k} IS NULL")
                        else:
                            # Escapa aspas simples para evitar erros SQL
                            sval = str(val).replace("'", "''")
                            parts.append(f"{k}='{sval}'")
                    key_fields_list.append(" , ".join(parts))
        
        # Determina resultado final do teste
        # NOK se houver pelo menos uma diferença, OK se todas as comparações forem iguais
        if df_diferencas.count() >= 1:
            result = "NOK"  # Teste falhou - há diferenças
        else:
            result = "OK"   # Teste passou - sem diferenças
    
        # Retorna dicionário com todos os detalhes do teste executado
        return {
            "TEST_ID": test_id,
            "SUBTYPE_ID": subtype_id,
            "QUERY": query,
            "RESULT": result,
            "SRC_FIELD": src_select_field,
            "DEST_FIELD": dest_select_field,
            "KEY_FIELDS_LIST": key_fields_list,
            "EXECUTION_PARAMETER": execution_parameter_str
        }
    except Exception as e:
        # Em caso de erro, tenta identificar se faltam parâmetros no user_input
        import re
        missing_params = []
        pattern = r'(\w+)\s*=\s*\?'  # Procura por placeholders campo=?
        src_missing = re.findall(pattern, src_filter, re.IGNORECASE)
        dest_missing = re.findall(pattern, dest_filter, re.IGNORECASE)
        missing_params.extend(src_missing)
        missing_params.extend(dest_missing)
        
        # Exibe parâmetros faltantes, se houver
        if missing_params:
            print(f"[!] Parâmetros em falta no user_input: {' | '.join(set(missing_params))}")
        
        # Exibe apenas a primeira linha da mensagem de erro para clareza
        error_msg = str(e).split('\n')[0] if str(e) else "Unknown error"
        print(f"[!] Caught exception in test_data_mapping: {error_msg}")
        return None  # Retorna None indicando falha na execução

def extrair_colunas_cursor_sql(query, chave_cruzamento=None, limite=None):
    """
    Extrai os nomes das colunas de uma query SQL executada via Spark.
    Útil para descobrir colunas dinamicamente quando se usa SELECT *.
    
    Parâmetros:
        query: Query SQL para extrair colunas (geralmente com LIMIT 0 para performance)
        chave_cruzamento: Lista de colunas a excluir do resultado (opcional)
        limite: Nome de coluna que serve como limite - retorna apenas colunas até esse ponto (opcional)
    
    Retorna:
        Lista com os nomes das colunas válidas e filtradas
    """
    chave_cruzamento = chave_cruzamento or []
    # Executa a query e obtém o schema para extrair nomes das colunas
    df_query = spark.sql(query)
    colunas = [field.name for field in df_query.schema.fields]
    # Remove colunas vazias ou metadados do Spark (como '# col_name')
    colunas_validas = [col for col in colunas if col and col != '# col_name']
    # Remove colunas que estão na lista de chaves de cruzamento (para evitar duplicação)
    colunas_filtradas = [col for col in colunas_validas if col not in chave_cruzamento]
    # Se há coluna limite definida, retorna apenas colunas até esse ponto
    if limite in colunas_filtradas:
        colunas_filtradas = colunas_filtradas[:colunas_filtradas.index(limite)]
    return colunas_filtradas