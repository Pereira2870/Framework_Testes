from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

def test_duplicates(
    test_id,
    subtype_id,
    src_table,
    src_filter,
    src_groupby,
    src_select_field,
    key_fields,
    execution_parameter_input
    ):
    try:
        # Processa o execution_parameter_input para extrair campos e valores dos parâmetros
        # Função para validar duplicados - identifica registros duplicados baseado em key_fields
        # Exemplo: "REF_DATE='2025-12-31'" ou "(REF_DATE='2025-12-31'),(DATA_DATE_PART='2025-12-31')" 
        execution_parameter = []  # Lista que irá armazenar os parâmetros processados
        
        if execution_parameter_input:
            import re
            
            # Extrai todas as partições entre parênteses ou sem parênteses
            # O regex captura: (conteúdo) OU conteúdo_sem_parênteses separado por vírgula
            # Exemplo: "(A=1),(B=2)" -> captura "A=1" e "B=2"
            # Exemplo: "A=1,B=2" -> captura "A=1" e "B=2"
            partitions = re.findall(r'\(([^)]+)\)|([^,]+)', execution_parameter_input)
            
            # Flatten e limpa a lista de tuplas retornada pelo findall
            # O regex retorna tuplas (grupo1, grupo2), escolhemos o grupo não-vazio
            partitions = [p[0] if p[0] else p[1] for p in partitions]
            
            # Remove espaços em branco e filtra strings vazias
            partitions = [p.strip() for p in partitions if p.strip()]
            
            # Itera sobre cada partição extraída para processar os parâmetros
            for partition in partitions:
                # Procura por padrão "CAMPO = VALOR" na partição
                # Exemplo: "REF_DATE='2025-12-31'" -> field_name="REF_DATE", field_value="'2025-12-31'"
                match = re.search(r"(\w+)\s*=\s*(.+)", partition)
                
                if match:
                    field_name = match.group(1).strip()   # Nome do campo (ex: REF_DATE)
                    field_value = match.group(2).strip()  # Valor do campo (ex: '2025-12-31')
                    
                    # Substitui ? pelo valor nas condições que usam o campo (case-insensitive)
                    # Cria pattern que procura "campo = ?" no filtro
                    # \b garante word boundary, re.escape previne problemas com caracteres especiais
                    pattern = rf'\b{re.escape(field_name)}\s*=\s*\?'
                    
                    # Verifica se o campo aparece no filtro da tabela source
                    if re.search(pattern, src_filter, re.IGNORECASE):
                        # Substitui o placeholder "?" pelo valor real no filtro
                        # Exemplo: "ref_date = ?" -> "ref_date='2025-12-31'"
                        src_filter = re.sub(pattern, f"{field_name}={field_value}", src_filter, flags=re.IGNORECASE)
                        execution_parameter.append(f"{field_name}={field_value}")
        
        # Converte lista de parâmetros em string para retorno
        # Usa ", " como separador
        execution_parameter_str = " | ".join(execution_parameter) if execution_parameter else "N/A"
        
        # Define as colunas de agrupamento combinando src_groupby e key_fields
        # Se src_groupby estiver vazio ou for condição dummy '1 = 1', usa apenas key_fields
        if src_groupby and src_groupby.strip() != '1 = 1':
            # Combina src_groupby com key_fields e remove duplicatas/espaços
            groupby_cols = [c.strip() for c in (src_groupby + ',' + key_fields).split(',') if c.strip()]
        else:
            # Usa apenas key_fields quando src_groupby não é relevante
            groupby_cols = [c.strip() for c in key_fields.split(',') if c.strip()]
        
        # Cria string com as colunas separadas por vírgula para usar no GROUP BY
        groupby_clause = ", ".join(groupby_cols)
        
        # Monta a query SQL para identificar registros duplicados
        # A cláusula HAVING COUNT(*) > 1 filtra apenas grupos com mais de um registro
        # Isso identifica as combinações de valores que aparecem duplicadas
        query = f"""
        SELECT {groupby_clause}, COUNT(*) as count
        FROM {src_table}
        WHERE {src_filter}
        GROUP BY {groupby_clause}
        HAVING COUNT(*) > 1
        """
        
        # Executa a query no Spark e obtém os resultados
        result_rows = spark.sql(query).collect()

        # Determina o resultado do teste:
        # Se encontrou linhas, há duplicados -> NOT OK
        # Se não encontrou linhas, não há duplicados -> OK
        if result_rows:
            # Constrói lista com os valores dos campos que estão duplicados
            # Para cada linha duplicada, cria string "campo1='valor1', campo2='valor2'"
            # Exemplo: ["REF_DATE='2025-12-31', COD_VISAO='1'"]
            key_fields_list = [
                ", ".join([f"{k}='{row[k]}'" for k in groupby_cols])
                for row in result_rows
            ]
            result = "NOT OK"  # Teste falhou - existem duplicados
        else:
            key_fields_list = []  # Lista vazia quando não há duplicados
            result = "OK"  # Teste passou - não existem duplicados  # Teste passou - não existem duplicados

        # Retorna dicionário com todos os dados do teste
        return {
            "TEST_ID": test_id,
            "SUBTYPE_ID": subtype_id,
            "QUERY": query,
            "RESULT": result,
            "KEY_FIELDS_LIST": key_fields_list,  # Lista de registos duplicados
            "SRC_TABLE": src_table,
            "EXECUTION_PARAMETER": execution_parameter_str
        }
        
    except Exception as e:
        # Bloco de tratamento de exceções - identifica parâmetros não substituídos
        import re
        
        # Procura por padrões "campo = ?" que não foram substituídos no filtro
        # Isso indica que faltam parâmetros no execution_parameter_input
        missing_params = re.findall(r'(\w+)\s*=\s*\?', src_filter)
        
        # Informa ao utilizador quais parâmetros estão em falta
        if missing_params:
            print(f"[!] Parâmetros em falta no execution_parameter_input: {', '.join(missing_params)}")
        
        # Extrai apenas a primeira linha do erro para evitar logs enormes
        # Útil para erros do Spark que costumam ter stack traces extensos
        error_msg = str(e).split('\n')[0] if str(e) else "Unknown error"
        print(f"[!] Caught exception in test_duplicates: {error_msg}")