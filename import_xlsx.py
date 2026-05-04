import glob
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType, IntegerType
)

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
CSV_DIR = "/Workspace/Framework-Testes-V1.0"
TARGET_TABLE = "framework_testes.test_config_parameters"
WRITE_MODE = "overwrite"  # "overwrite" substitui a tabela; "append" adiciona linhas

# ---------------------------------------------------------------------------
# Schema da tabela destino
# ---------------------------------------------------------------------------
TABLE_SCHEMA = StructType([
    StructField("TEST_ID",              IntegerType(),   True),
    StructField("TYPE_ID",              StringType(), True),
    StructField("SUBTYPE_ID",           StringType(), True),
    StructField("SOURCE_TABLE",         StringType(), True),
    StructField("DEST_TABLE",           StringType(), True),
    StructField("JOIN_KEY",             StringType(), True),
    StructField("SOURCE_SELECT_FIELD",  StringType(), True),
    StructField("DEST_SELECT_FIELD",    StringType(), True),
    StructField("SOURCE_FILTER",        StringType(), True),
    StructField("DEST_FILTER",          StringType(), True),
    StructField("SOURCE_GROUPBY",       StringType(), True),
    StructField("DEST_GROUPBY",         StringType(), True),
    StructField("KEY_FIELDS",           StringType(), True),
])

EXPECTED_COLUMNS = [f.name for f in TABLE_SCHEMA.fields]

# ---------------------------------------------------------------------------
# Localizar ficheiro .csv
# ---------------------------------------------------------------------------
csv_files = glob.glob(f"{CSV_DIR}/*.csv")

if not csv_files:
    raise FileNotFoundError(
        f"Nenhum ficheiro .csv encontrado em {CSV_DIR}. "
        "Coloque o ficheiro CSV na pasta e volte a executar."
    )

if len(csv_files) > 1:
    print(f"[!] Encontrados {len(csv_files)} ficheiros .csv:")
    for f in csv_files:
        print(f"    - {f}")
    print(f"[!] A utilizar o primeiro: {csv_files[0]}")

csv_path = csv_files[0]
print(f"[!] A ler ficheiro: {csv_path}")

# ---------------------------------------------------------------------------
# Ler CSV com pandas (tenta separadores comuns: ; e ,)
# ---------------------------------------------------------------------------
try:
    pdf = pd.read_csv(csv_path, sep=";", encoding="utf-8")
    if len(pdf.columns) <= 1:  # separador errado, tentar com vírgula
        pdf = pd.read_csv(csv_path, sep=",", encoding="utf-8")
except Exception:
    pdf = pd.read_csv(csv_path, sep=",", encoding="utf-8")

print(f"[!] Linhas lidas do CSV: {len(pdf)}")
print(f"[!] Colunas do CSV: {list(pdf.columns)}")

# Normalizar nomes de colunas (remover espaços, uppercase)
pdf.columns = [c.strip().upper() for c in pdf.columns]

# Validar que as colunas esperadas existem
missing = [c for c in EXPECTED_COLUMNS if c not in pdf.columns]
if missing:
    raise ValueError(
        f"As seguintes colunas estão em falta no ficheiro CSV: {missing}. "
        f"Colunas encontradas: {list(pdf.columns)}"
    )

# Seleccionar e ordenar colunas conforme a tabela destino
pdf = pdf[EXPECTED_COLUMNS]

# Converter TEST_ID para inteiro (pandas pode ler como float se houver NaN)
pdf["TEST_ID"] = pd.to_numeric(pdf["TEST_ID"], errors="coerce").astype("Int64")

# Converter restantes colunas para string (substituir NaN por None)
for col in EXPECTED_COLUMNS[1:]:
    pdf[col] = pdf[col].where(pdf[col].notna(), None)
    pdf[col] = pdf[col].astype(object).where(pdf[col].notna(), None)

# ---------------------------------------------------------------------------
# Converter para Spark DataFrame e escrever na tabela
# ---------------------------------------------------------------------------
df = spark.createDataFrame(pdf.astype(object).where(pdf.notna(), None), schema=TABLE_SCHEMA)

print(f"[!] Preview dos dados:")
df.show(5, truncate=False)

df.write.mode(WRITE_MODE).saveAsTable(TARGET_TABLE)
print(f"[!] Dados escritos com sucesso na tabela {TARGET_TABLE} (mode={WRITE_MODE}).")
print(f"[!] Total de linhas na tabela: {spark.table(TARGET_TABLE).count()}")
