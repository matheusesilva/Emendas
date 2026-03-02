# Emendas Parlamentares - Pipeline de Dados

Este repositório contém um pipeline completo para ingestão, processamento, transformação e disponibilização de dados de emendas parlamentares do Portal da Transparência, utilizando PySpark, DuckDB e Streamlit.

## Visão Geral
O objetivo deste projeto é automatizar o download, padronização, enriquecimento e disponibilização de dados públicos sobre emendas parlamentares, facilitando análises, relatórios e visualizações avançadas.

## Estrutura do Projeto
```
├── config.yaml                  # Configuração central do pipeline
├── main.py                      # Ponto de entrada do pipeline
├── src/                         # Código-fonte principal
│   ├── utils.py                 # Utilitários, configuração, logging, schemas
│   ├── bronze_ingestion.py      # Ingestão e particionamento dos dados brutos
│   ├── silver_transformation.py # Transformações e limpezas para camada silver
│   ├── gold_delivery.py         # Modelagem e consultas com DuckDB para camada gold
│   └── dashboard.py             # Aplicação Streamlit para visualização dos dados gold
├── data/
│   ├── 00_raw/                  # Dados brutos baixados (zip)
│   ├── 01_bronze/               # Dados padronizados e particionados por tabela
│   ├── 02_silver/               # Dados tratados e prontos para análise
│   └── 03_gold/                 # Dados agregados/modelados para consumo analítico 
├── logs/                        # Logs estruturados do pipeline (JSON)
└── notebooks/                   # Notebooks de EDA, testes e visualização
```

## Ferramentas Utilizadas
- **Python 3.8+**: Linguagem principal do pipeline
- **PySpark**: Processamento distribuído, leitura e escrita de dados, transformação
- **DuckDB**: Consultas SQL rápidas e modelagem analítica na camada gold
- **Streamlit**: Criação de dashboards interativos para visualização dos dados gold
- **requests**: Download de arquivos via HTTP
- **logging**: Logging estruturado em JSON
- **Jupyter Notebook** (opcional): Exploração e visualização

## Decisões técnicas
- **Arquivo de Configuração**: Devido a natureza pública dos dados e a possível eventual volatilidade dos schemas, arquivos, links foi adatado um arquivo de configuração em `YAML` para o pipeline contendo informações dos dados que podem ser alteradas conforme a necessidade. Contribuido para a maior manutenabilidade do pipeline.
- **Ferramenta de Transformação**: Para esse pipeline foi adotado o `Pyspark` para realizar as principais transformações dentro do pipeline, a escolha foi estritamente didática dado a volume baixo de dados. Outras ferramentas poderiam serem utilizadas sem comprometimento da performance, como Pandas e Polars.
- **Formato de Armazenamento**: Os dados foram salvos em formato `.parquet` devido a sua velocidade de leitura, simplicidade de configuração comparado com um banco de dados, baixo tamanho de arquivos e boa integração com `Pyspark` e `DuckDB`.
- **Monitoramento**: Foi adotada um sistema de logs estruturados em json para possibilitar o monitoramento do pipeline por ferramentas como Loki/Grafana.

## Etapas do Pipeline ETL

![Diagrama Pipeline](https://github.com/matheusesilva/Emendas/blob/a0ce036443e880adef7c3494e171bd80ab869fcc/docs/flowchart.jpg)

### 1. Ingestão (Bronze)
- **Download do arquivo zip**: Utiliza `requests` para baixar o arquivo de emendas parlamentares do Portal da Transparência.
- **Armazenamento do zip**: Salva o arquivo em `data/00_raw`, particionado por data (formato Hive), usando Python nativo.
- **Extração dos CSVs**: Utiliza `zipfile` e `tempfile` para extrair os arquivos CSV para uma pasta temporária.
- **Leitura dos CSVs**: Utiliza `PySpark` para ler cada CSV conforme schema definido em `config.yaml` (agora sem tamanho no format).
- **Adição de colunas técnicas**: Com `PySpark`, adiciona `arquivo_fonte`, `timestamp_ingestao` e `ingestion_date`.
- **Escrita em Parquet**: Salva os dados em Parquet, particionados por data, na camada `01_bronze` (um diretório por tabela), usando `PySpark`.
- **Logging estruturado**: Utiliza o módulo `logging` para registrar logs em JSON, incluindo ID de job, tempo de execução e status.
- **Verificação de existência do zip**: Antes do download, verifica se o arquivo já existe usando `os.path`.

### 2. Transformação (Silver)
- **Limpeza e padronização**: Utiliza `PySpark` para aplicar regras de qualidade, remover valores inválidos e padronizar formatos.
- **Enriquecimento dos dados**: Integrações e cálculos adicionais usando `PySpark` e funções customizadas.
- **Escrita dos dados tratados**: Salva os dados prontos para análise em `data/02_silver`.

### 3. Modelagem e Visualização (Gold)
- **Modelagem analítica**: Utiliza `DuckDB` para criar tabelas agregadas, sumarizações e consultas SQL rápidas sobre os dados Parquet da camada silver.
- **Escrita dos dados gold**: Salva os dados modelados em `data/03_gold`, prontos para dashboards e relatórios.
- **Dashboard interativo**: Utiliza `Streamlit` para criar uma aplicação web interativa, permitindo explorar e visualizar os dados gold de forma dinâmica.

![Imagem Dashboard](https://github.com/matheusesilva/Emendas/blob/a0ce036443e880adef7c3494e171bd80ab869fcc/docs/dashboard_streamlit.jpg)

## Como Executar
1. Execute o pipeline pelo `main.py` ou scripts/notebooks específicos.
2. Execute o dashboard Streamlit em `src/` para visualizar os dados gold:
   ```bash
   streamlit run dashboard/app.py
   ```
3. Os dados processados estarão disponíveis nas pastas `data/01_bronze`, `data/02_silver` e `data/03_gold`.
4. Para agendamento das execuções, recomenda-se a utilização de cron jobs. Exemplo de agendamento diário às 2h da manhã:
   ```cron
   0 2 * * * cd /caminho/para/seu/projeto && /usr/bin/python3 main.py >> logs/cron_etl.log 2>&1
   ```
   > Este comando executa o pipeline diariamente às 2h, salvando logs em `logs/cron_etl.log`. Ajuste o caminho do Python e do projeto conforme seu ambiente.

## Requisitos
- Python 3.8+
- PySpark
- DuckDB
- Streamlit
- requests
- Jupyter Notebook (opcional)

## Fonte Original dos Dados
- **Página de download**: [Portal da Transparência](https://portaldatransparencia.gov.br/download-de-dados/emendas-parlamentares)
- **Dicionário de dados**: [Portal da Transparência](https://portaldatransparencia.gov.br/dicionario-de-dados/emendas-parlamentares)

## Licença
MIT

---

> Projeto em desenvolvimento contínuo. Para dúvidas, sugestões ou bugs, abra uma issue.
