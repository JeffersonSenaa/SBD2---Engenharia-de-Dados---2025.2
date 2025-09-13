# Projeto de Banco de Dados 2 – Engenharia de Dados com Data Lake e PySpark

## 📌 Descrição

Este projeto foi desenvolvido para a disciplina **Banco de Dados 2**, com o objetivo de aplicar conceitos de **Engenharia de Dados** utilizando um **Data Lake** e ferramentas do ecossistema **Apache Spark (PySpark)**.

A proposta é construir uma arquitetura que permita **armazenar, processar e analisar dados em larga escala**, explorando técnicas modernas de ingestão, transformação e organização de dados para análise.

## 🎯 Objetivos

* Compreender os conceitos de **Data Lake** e sua importância para Big Data.
* Implementar pipelines de ingestão e transformação de dados utilizando **PySpark**.
* Organizar dados em diferentes camadas do Data Lake (**Raw, Processed, Curated**).
* Realizar consultas e análises exploratórias sobre os dados tratados.
* Demonstrar boas práticas de **Engenharia de Dados** em um ambiente acadêmico.

## 🛠️ Tecnologias Utilizadas

* **Python 3.x**
* **Apache Spark / PySpark**
* **Data Lake (estrutura em camadas Raw, Processed, Curated)**
* **Hadoop HDFS ou sistema de arquivos local** (dependendo da configuração do ambiente)
* **Jupyter Notebook** para análises exploratórias
* **Git/GitHub** para versionamento

## 📂 Estrutura do Projeto

```
📦 projeto-bd2-datalake
├── data
│   ├── raw/          # Dados brutos
│   ├── processed/    # Dados processados
│   └── curated/      # Dados prontos para consumo
├── notebooks/        # Jupyter Notebooks com análises e ETL
├── scripts/          # Scripts PySpark para ingestão e transformação
├── docs/             # Documentação do projeto
└── README.md
```

## ⚙️ Funcionalidades

* Ingestão de dados em formato **CSV/JSON/Parquet** para o Data Lake.
* Processamento de dados brutos com **PySpark** (limpeza, padronização, enriquecimento).
* Armazenamento otimizado em formatos de alto desempenho (**Parquet/Delta**).
* Consultas analíticas sobre dados tratados.

## 🚀 Como Executar o Projeto

1. **Clone o repositório**

```bash
git clone h[ttps://github.com/jeffersonsenaa/sbd2-engenharia-de-dados-2025.2.git](https://github.com/JeffersonSenaa/SBD2---Engenharia-de-Dados---2025.2/)
cd projeto-bd2-datalake
```

2. **Crie o ambiente e instale dependências**

```bash
pip install -r requirements.txt
```

3. **Inicie o Spark** e execute os notebooks/scripts para processar os dados.

```bash
pyspark
```

## 📊 Exemplos de Uso

* Ingestão de dados de vendas e clientes para o Data Lake.
* Transformação e cálculo de métricas como **ticket médio, clientes ativos, vendas por região**.
* Criação de uma camada **curated** com dados prontos para BI.

## 📖 Aprendizados

Durante o desenvolvimento do projeto, serão explorados:

* Diferenças entre **Data Warehouse x Data Lake**.
* Estruturação de pipelines ETL/ELT.
* Otimização de armazenamento e consulta em Big Data.
* Aplicações práticas de **Engenharia de Dados**.

## 👨‍💻 Autores

