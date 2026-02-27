# Emendas Parlamentares - Pipeline de Dados

Este repositório contém um pipeline de ingestão, processamento e transformação de dados de emendas parlamentares do Portal da Transparência, utilizando PySpark e arquitetura de dados em camadas (raw, bronze, silver, gold).

## Visão Geral
O objetivo deste projeto é automatizar o download, padronização, enriquecimento e disponibilização de dados públicos sobre emendas parlamentares, facilitando análises e visualizações futuras.

## Estrutura do Projeto
```
├── config.yaml                # Configuração central do pipeline
├── main.py                    # Ponto de entrada do pipeline
├── src/                       # Código-fonte principal
│   ├── utils.py               # Utilitários, configuração, logging, schemas
│   ├── bronze_ingestion.py    # Ingestão e particionamento dos dados brutos
│   ├── silver_transformation.py # Transformações e limpezas para camada silver
│   └── ...
├── data/
│   ├── 00_raw/                # Dados brutos baixados
│   ├── 01_bronze/             # Dados padronizados e particionados
│   ├── 02_silver/             # Dados tratados e prontos para análise
│   └── ...
├── logs/                      # Logs estruturados do pipeline
├── notebooks/                 # Notebooks de EDA e testes
└── tests/                     # Testes automatizados
```

## Etapas do Pipeline

### 1. Ingestão (Bronze)
- [x] Download automático do arquivo zip de emendas parlamentares do Portal da Transparência
- [x] Armazenamento do zip em `data/00_raw` com particionamento por data (formato Hive)
- [x] Extração dos arquivos CSV do zip para pasta temporária
- [x] Leitura de cada CSV conforme schema definido em `config.yaml`
- [x] Adição de colunas técnicas (`arquivo_fonte`, `timestamp_ingestao`, `ingestion_date`)
- [x] Escrita dos dados em Parquet, particionados por data, na camada `01_bronze` (um diretório por tabela)

### 2. Transformação (Silver)
- [x] Implementação das regras de limpeza, padronização e enriquecimento dos dados
- [x] Escrita dos dados tratados na camada `02_silver`

### 3. Camada Gold e Visualização
- [ ] Modelagem de dados para consumo analítico
- [ ] Integração com ferramentas de BI ou notebooks de análise

## Como Executar
1. Execute o pipeline pelo `main.py`.
2. Os dados processados estarão disponíveis dentro da pasta `data`.

## Requisitos
- Python 3.10
- PySpark
- requests

## Licença
MIT

---

> Projeto em desenvolvimento.
