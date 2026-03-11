def test_data_catalog(
    test_id,
    subtype_id,
    src_table,
    src_filter,
    key_fields,
    execution_parameter_input

):
    try:
        # Processa o execution_parameter_input para extrair campos e valores dos parâmetros
        # Função para validar catálogo de valores - identifica duplicatas nos key_fields
        execution_parameter = []  # Lista que irá armazenar os parâmetros processados
        
        if execution_parameter_input:
            import re
            
            # Extrai todas as partições entre parênteses ou sem parênteses
            # O regex captura: (conteúdo) OU conteúdo_sem_parênteses separado por vírgula
            # Exemplo: "(REF_DATE='2025-12-31')" ou "REF_DATE='2025-12-31',COD_VISAO=1"
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
        # Usa " | " como separador para melhor visualização
        execution_parameter_str = " | ".join(execution_parameter) if execution_parameter else "N/A"
        
        # Monta a query SQL para identificar duplicatas nos key_fields
        # Se encontrar registros com COUNT(*) > 1, significa que há duplicatas
        query = f"SELECT {key_fields}, COUNT(*) as count FROM {src_table} WHERE {src_filter} GROUP BY {key_fields}"
        
        # Executa a query no Spark e obtém os resultados
        result_rows = spark.sql(query).collect()

        # Determina o resultado do teste:
        # Se encontrou linhas (len > 0), há duplicatas -> NOK
        # Se não encontrou linhas, não há duplicatas -> OK
        if len(result_rows) > 0:
            result = 'NOK'  # Teste falhou - existem duplicatas
        else:
            result = 'OK'   # Teste passou - não existem duplicatas

        # Constrói lista com os valores dos key_fields que estão duplicados
        # Útil para identificar quais registros específicos têm problema
        if key_fields != "":
            # Divide os key_fields por vírgula e remove espaços
            key_fields_split = [k.strip() for k in key_fields.split(",")]
            
            # Para cada linha duplicada encontrada, cria string "campo1='valor1', campo2='valor2'"
            # Exemplo: ["REF_DATE='2025-12-31', COD_VISAO='1'"]
            key_fields_list = [
                ", ".join([f"{k}='{str(row[k])}'" for k in key_fields_split])
                for row in result_rows
            ]
        else:
            # Se não há key_fields definidos, retorna lista vazia
            key_fields_list = []

        # Retorna dicionário com todos os dados do teste
        return {
            "TEST_ID": test_id,
            "SUBTYPE_ID": subtype_id,
            "QUERY": query,
            "RESULT": result,
            "KEY_FIELDS_LIST": key_fields_list,  # Lista de registos duplicados
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
            print(f"[!] Parâmetros em falta no execution_parameter_input: {' | '.join(missing_params)}")
        
        # Extrai apenas a primeira linha do erro para evitar logs enormes
        # Útil para erros do Spark que costumam ter stack traces extensos
        error_msg = str(e).split('\n')[0] if str(e) else "Unknown error"
        print(f"[!] Caught exception in test_data_catalog: {error_msg}")