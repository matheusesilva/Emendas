import pytest
from pyspark.sql import SparkSession
from src.silver_transformation import clean_column_names

@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[*]").appName("Tests").getOrCreate()
    yield session
    session.stop()

def test_clean_column_names_advanced(spark):
    dirty_cols = [
        "Identificação Usuário", # Espaço e acento
        "Preço (R$)",            # Parênteses e símbolo
        "Ação_&_Status!!",       # E comercial e exclamação
        "Data-Nascimento",       # Hífen
        "Endereço.Residencial"   # Cedilha e ponto
    ]
    data = [(1, 2, 3, 4, 5)]
    df = spark.createDataFrame(data, dirty_cols)
    
    df_cleaned = clean_column_names(df)
    
    expected_cols = [
        "identificacao_usuario",
        "preco_r",
        "acao_status",
        "data_nascimento",
        "endereco_residencial"
    ]
    
    assert df_cleaned.columns == expected_cols

    