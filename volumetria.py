from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

def test_data_volume(
    test_id,
    subtype_id,
    src_table,
    dest_table,
    src_where_condition,
    dest_where_condition,
    execution_parameter_input
):

    try:
        # Processa o execution_parameter_input para extrair o campo e valor
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
                    # Cria pattern que procura "campo = ?" nas WHERE conditions
                    # \b garante word boundary, re.escape previne problemas com caracteres especiais
                    pattern = rf'\b{re.escape(field_name)}\s*=\s*\?'
                    
                    # Verifica se o campo aparece na condição WHERE da tabela source
                    if re.search(pattern, src_where_condition, re.IGNORECASE):
                        # Substitui o placeholder "?" pelo valor real
                        # Exemplo: "ref_date = ?" -> "ref_date='2025-12-31'"
                        src_where_condition = re.sub(pattern, f"{field_name}={field_value}", src_where_condition, flags=re.IGNORECASE)
                        execution_parameter.append(f"{field_name}={field_value}")
                    
                    # Verifica se o campo aparece na condição WHERE da tabela destino
                    if re.search(pattern, dest_where_condition, re.IGNORECASE):
                        # Substitui o placeholder "?" pelo valor real
                        dest_where_condition = re.sub(pattern, f"{field_name}={field_value}", dest_where_condition, flags=re.IGNORECASE)
                        # Evita duplicatas na lista de parâmetros
                        if f"{field_name}={field_value}" not in execution_parameter:
                            execution_parameter.append(f"{field_name}={field_value}")
        
        # Converte lista de parâmetros em string para retorno
        # Usa " | " como separador para melhor visualização
        execution_parameter_str = " | ".join(execution_parameter) if execution_parameter else "N/A"
        
        # Constrói query SQL que compara volumetria entre source e destino
        # Retorna contagens de ambas as tabelas e 'OK'/'NOK' conforme igualdade
        query = f"""
        SELECT 
            src.SRC_COUNT,
            dest.DEST_COUNT,
            CASE 
                WHEN src.SRC_COUNT = dest.DEST_COUNT THEN "OK"
                ELSE "NOK"
            END AS RESULT
        FROM
            (SELECT COUNT(1) AS SRC_COUNT FROM {src_table} WHERE {src_where_condition}) src,
            (SELECT COUNT(1) AS DEST_COUNT FROM {dest_table} WHERE {dest_where_condition}) dest
        """

        # Executa a query no Spark e obtém o resultado
        result = spark.sql(query).collect()[0]["RESULT"]

        return {
            "TEST_ID": test_id,
            "SUBTYPE_ID": subtype_id,
            "QUERY": query,
            "RESULT": result,
            "SRC_TABLE": src_table,
            "DEST_TABLE": dest_table,
            "EXECUTION_PARAMETER": execution_parameter_str
        }

    except Exception as e:
        # Bloco de tratamento de exceções - identifica parâmetros não substituídos
        import re
        missing_params = []
        
        # Procura por padrões "campo = ?" que não foram substituídos nas condições WHERE
        # Isso indica que faltam parâmetros no execution_parameter_input
        pattern = r'(\w+)\s*=\s*\?'
        src_missing = re.findall(pattern, src_where_condition, re.IGNORECASE)
        dest_missing = re.findall(pattern, dest_where_condition, re.IGNORECASE)
        
        # Consolida lista de parâmetros em falta (sem duplicatas)
        missing_params.extend(src_missing)
        missing_params.extend([m for m in dest_missing if m not in missing_params])
        
        # Informa ao utilizador quais parâmetros estão em falta
        if missing_params:
            print(f"[!] Parâmetros em falta no execution_parameter: {' | '.join(missing_params)}")
        
        # Extrai apenas a primeira linha do erro para evitar logs enormes
        # Útil para erros do Spark que costumam ter stack traces extensos
        error_msg = str(e).split('\n')[0] if str(e) else "Unknown error"
        print(f"[!] Caught exception in test_data_volume: {error_msg}")
