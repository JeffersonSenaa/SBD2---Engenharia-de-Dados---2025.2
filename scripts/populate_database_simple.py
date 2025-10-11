#!/usr/bin/env python3
"""
Script para popular o banco de dados a partir do CSV
Implementa pipeline ETL: Bronze -> Silver (Star Schema)
"""
import sys
import os
import time
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text

DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'sbd2_vehicle'),
    'user': os.getenv('POSTGRES_USER', 'sbd2_vehicle'),
    'password': os.getenv('POSTGRES_PASSWORD', 'sbd2_vehicle')
}

CSV_PATH = '/home/jovyan/work/data/bronze/vehicle_price_prediction.csv'
DDL_PATH = '/home/jovyan/work/src/silver/ddl.sql'

def get_engine():
    """
    Cria e retorna a engine SQLAlchemy para conexão com PostgreSQL
    """
    connection_string = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(connection_string)


def wait_for_database(max_attempts=30, delay=5):
    """
    Aguarda o banco de dados ficar disponív
    """
    print("Aguardando banco de dados...")
    for attempt in range(max_attempts):
        try:
            engine = get_engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print(f"✓ Banco disponível após {attempt + 1} tentativa(s)")
            return True
        except Exception as e:
            if attempt < max_attempts - 1:
                print(f"Tentativa {attempt + 1}/{max_attempts}: Aguardando...")
                time.sleep(delay)
    
    print("ERRO: Timeout - banco não ficou disponível")
    return False

def create_bronze_table(engine):
    """
    Cria a tabela Bronze (dados brutos do CSV)
    Esta tabela recebe os dados exatamente como estão no CSV
    """
    print("\n[1/4] Criando tabela Bronze...")
    
    ddl = """
    CREATE SCHEMA IF NOT EXISTS bronze;
    DROP TABLE IF EXISTS bronze.vehicle_prices CASCADE;
    CREATE TABLE IF NOT EXISTS bronze.vehicle_prices (
        id SERIAL PRIMARY KEY,
        -- Dados do CSV (20 colunas originais)
        make VARCHAR(100),
        model VARCHAR(100),
        year INTEGER,
        mileage INTEGER,
        engine_hp FLOAT,
        transmission VARCHAR(50),
        fuel_type VARCHAR(50),
        drivetrain VARCHAR(50),
        body_type VARCHAR(50),
        exterior_color VARCHAR(50),
        interior_color VARCHAR(50),
        owner_count INTEGER,
        accident_history VARCHAR(50),
        seller_type VARCHAR(50),
        condition VARCHAR(50),
        trim VARCHAR(50),
        vehicle_age INTEGER,
        mileage_per_year FLOAT,
        brand_popularity FLOAT,
        price FLOAT,
        -- Metadados de ingestão
        _ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        _source_file VARCHAR(255)
    );
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(text(ddl))
            conn.commit()
        print("  ✓ Tabela Bronze criada")
        return True
    except Exception as e:
        print(f"  ✗ Erro ao criar tabela Bronze: {e}")
        return False


def create_silver_tables(engine):
    """
    Cria as tabelas Silver usando o arquivo DDL
    
    Estrutura (Star Schema):
    - dim_modelo: informações do veículo (marca, modelo, ano, etc)
    - dim_especificacao: especificações (cor, dono, acidentes, etc)
    - fato_veiculo: tabela fato com métricas (preço, kilometragem, etc)
    """
    print("\n[2/4] Criando tabelas Silver (Star Schema)...")
    
    try:
        # Ler o arquivo DDL
        with open(DDL_PATH, 'r') as f:
            ddl_content = f.read()
        
        # Executar o DDL
        with engine.connect() as conn:
            # Criar schema silver se não existir
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
            
            # Executar cada statement do DDL
            for statement in ddl_content.split(';'):
                if statement.strip():
                    conn.execute(text(statement))
            
            conn.commit()
        
        print("  Tabelas Silver criadas:")
        print("    - silver.dim_modelo")
        print("    - silver.dim_especificacao")
        print("    - silver.fato_veiculo")
        return True
        
    except Exception as e:
        print(f"  Erro ao criar tabelas Silver: {e}")
        return False

def load_bronze_data(engine):
    """
    Extrai dados do CSV e carrega na tabela Bronze
    
    Passos:
    1. Verifica se já existem dados
    2. Lê o arquivo CSV
    3. Adiciona metadados (_ingestion_timestamp, _source_file)
    4. Insere no banco em chunks (10.000 linhas por vez)
    """
    print("\n[3/4] Carregando dados Bronze (CSV -> Banco)...")
    print("="*60)
    
    start_time = datetime.now()
    
    try:
        # Verificar se já tem dados
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM bronze.vehicle_prices"))
            count = result.scalar()
            if count > 0:
                print(f"  Bronze já contém {count:,} registros. Pulando.")
                return True
        
        # Ler CSV
        print("  Lendo CSV...")
        df = pd.read_csv(CSV_PATH)
        print(f"  {len(df):,} linhas carregadas")
        
        # TRANSFORMAÇÃO 1: Tratar nulos em accident_history
        # Conforme análise do notebook: 75% são nulos
        # Decisão: preencher com 'None' (em vez de deixar NULL)
        print("  Aplicando transformação: accident_history")
        df['accident_history'] = df['accident_history'].fillna('None')
        
        # Adicionar metadados
        df['_ingestion_timestamp'] = datetime.now()
        df['_source_file'] = 'vehicle_price_prediction.csv'
        
        # Inserir no banco em chunks (melhor performance)
        print("  Inserindo no banco...")
        df.to_sql(
            'vehicle_prices',
            engine,
            schema='bronze',
            if_exists='append',
            index=False,
            chunksize=10000
        )
        
        duration = (datetime.now() - start_time).total_seconds()
        print(f"  ✓ Bronze carregado: {len(df):,} registros em {duration:.1f}s")
        
        return True
        
    except Exception as e:
        print(f"  Erro: {e}")
        return False

def transform_and_load_silver(engine):
    """
    Processa dados Bronze e popula tabelas Silver (Star Schema)
    
    TRANSFORMAÇÕES:
    ---------------
    1. Lê dados da tabela Bronze
    2. Cria dimensões únicas:
       - dim_modelo: combinação única de (make, model, year, etc)
       - dim_especificacao: combinação única de (cor, condição, etc)
    3. Cria tabela fato referenciando as dimensões
    
    Isso implementa um Star Schema para análise otimizada
    """
    print("\n[4/4] Transformando e carregando Silver...")
    print("="*60)
    
    start_time = datetime.now()
    
    try:
        # Verificar se já tem dados
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM silver.fato_veiculo"))
            count = result.scalar()
            if count > 0:
                print(f"  Silver já contém {count:,} registros. Pulando.")
                return True
        
        # Ler dados da Bronze
        print("  Lendo dados Bronze...")
        df = pd.read_sql("SELECT * FROM bronze.vehicle_prices", engine)
        print(f"  {len(df):,} registros lidos")
        
        # ==========================================
        # PASSO 1: Criar dim_modelo
        # ==========================================
        print("\n  Criando dim_modelo...")
        
        # Selecionar colunas da dimensão modelo
        dim_modelo_cols = [
            'make', 'model', 'year', 'engine_hp',
            'transmission', 'fuel_type', 'drivetrain',
            'body_type', 'trim'
        ]
        
        # Criar DataFrame com combinações únicas
        df_dim_modelo = df[dim_modelo_cols].drop_duplicates().reset_index(drop=True)
        
        # Adicionar id_modelo (começando de 1)
        df_dim_modelo.insert(0, 'id_modelo', range(1, len(df_dim_modelo) + 1))
        
        # Inserir no banco
        df_dim_modelo.to_sql(
            'dim_modelo',
            engine,
            schema='silver',
            if_exists='append',
            index=False,
            chunksize=5000
        )
        
        print(f"  {len(df_dim_modelo):,} modelos únicos")
        
        # ==========================================
        # PASSO 2: Criar dim_especificacao
        # ==========================================
        print("\n  Criando dim_especificacao...")
        
        # Selecionar colunas da dimensão especificação
        dim_espec_cols = [
            'exterior_color', 'interior_color', 'owner_count',
            'accident_history', 'seller_type', 'condition',
            'vehicle_age'
        ]
        
        # Criar DataFrame com combinações únicas
        df_dim_espec = df[dim_espec_cols].drop_duplicates().reset_index(drop=True)
        
        # Adicionar id_especificacao
        df_dim_espec.insert(0, 'id_especificacao', range(1, len(df_dim_espec) + 1))
        
        # Inserir no banco
        df_dim_espec.to_sql(
            'dim_especificacao',
            engine,
            schema='silver',
            if_exists='append',
            index=False,
            chunksize=5000
        )
        
        print(f"  {len(df_dim_espec):,} especificações únicas")
        
        # ==========================================
        # PASSO 3: Criar tabela FATO
        # ==========================================
        print("\n  Criando fato_veiculo...")
        
        # Fazer merge para obter os IDs das dimensões
        # Juntar com dim_modelo
        df_fato = df.merge(
            df_dim_modelo,
            on=dim_modelo_cols,
            how='left'
        )
        
        # Juntar com dim_especificacao
        df_fato = df_fato.merge(
            df_dim_espec,
            on=dim_espec_cols,
            how='left'
        )
        
        # Selecionar apenas colunas da tabela fato
        df_fato = df_fato[[
            'id_modelo',
            'id_especificacao',
            'mileage',
            'mileage_per_year',
            'brand_popularity',
            'price'
        ]].copy()
        
        # Adicionar id_fato
        df_fato.insert(0, 'id_fato', range(1, len(df_fato) + 1))
        
        # Inserir no banco
        df_fato.to_sql(
            'fato_veiculo',
            engine,
            schema='silver',
            if_exists='append',
            index=False,
            chunksize=10000
        )
        
        duration = (datetime.now() - start_time).total_seconds()
        print(f"\n  Silver completo: {len(df_fato):,} fatos em {duration:.1f}s")
        
        return True
        
    except Exception as e:
        print(f"  Erro: {e}")
        import traceback
        traceback.print_exc()
        return False

def verify_data(engine):
    """
    Verifica se os dados foram carregados corretamente
    Mostra estatísticas das tabelas criadas
    """
    print("\n" + "="*60)
    print("VERIFICAÇÃO DE DADOS")
    print("="*60)
    
    try:
        with engine.connect() as conn:
            # Bronze
            result = conn.execute(text("SELECT COUNT(*) FROM bronze.vehicle_prices"))
            bronze_count = result.scalar()
            print(f"\n  Bronze: {bronze_count:,} registros")
            
            # Dim Modelo
            result = conn.execute(text("SELECT COUNT(*) FROM silver.dim_modelo"))
            modelo_count = result.scalar()
            print(f"  Dim Modelo: {modelo_count:,} modelos únicos")
            
            # Dim Especificacao
            result = conn.execute(text("SELECT COUNT(*) FROM silver.dim_especificacao"))
            espec_count = result.scalar()
            print(f"  Dim Especificação: {espec_count:,} especificações únicas")
            
            # Fato
            result = conn.execute(text("SELECT COUNT(*) FROM silver.fato_veiculo"))
            fato_count = result.scalar()
            print(f"  Fato Veículo: {fato_count:,} registros")
            
            # Estatísticas de preço
            print("\n" + "-"*60)
            print("ESTATÍSTICAS DE PREÇO:")
            print("-"*60)
            
            result = conn.execute(text("""
                SELECT 
                    ROUND(AVG(price)::numeric, 2) as preco_medio,
                    ROUND(MIN(price)::numeric, 2) as preco_minimo,
                    ROUND(MAX(price)::numeric, 2) as preco_maximo
                FROM silver.fato_veiculo
            """))
            stats = result.fetchone()
            print(f"Preço Médio:  ${stats[0]:,.2f}")
            print(f"Preço Mínimo: ${stats[1]:,.2f}")
            print(f"Preço Máximo: ${stats[2]:,.2f}")
            
            # Top 5 marcas
            print("\n" + "-"*60)
            print("TOP 5 MARCAS (por volume):")
            print("-"*60)
            
            result = conn.execute(text("""
                SELECT m.make, COUNT(*) as total
                FROM silver.fato_veiculo f
                JOIN silver.dim_modelo m ON f.id_modelo = m.id_modelo
                GROUP BY m.make
                ORDER BY total DESC
                LIMIT 5
            """))
            
            for row in result:
                print(f"  {row[0]:15s}: {row[1]:,}")
        
        return True
        
    except Exception as e:
        print(f"  Erro na verificação: {e}")
        return False

def main():
    """
    Pipeline ETL completo:
    
    1. Conecta ao banco
    2. Cria tabelas Bronze e Silver
    3. Carrega CSV -> Bronze
    4. Transforma Bronze -> Silver (Star Schema)
    5. Verifica resultados
    """
    print("="*60)
    print("ETL: CSV -> BRONZE -> SILVER (Star Schema)")
    print("="*60)
    print(f"Início: {datetime.now().strftime('%H:%M:%S')}\n")
    
    # PASSO 1: Conectar ao banco
    if not wait_for_database():
        print("\n  FALHA: Não foi possível conectar ao banco")
        sys.exit(1)
    
    engine = get_engine()
    
    # PASSO 2: Criar tabela Bronze
    if not create_bronze_table(engine):
        print("\n  FALHA: Erro ao criar tabela Bronze")
        sys.exit(1)
    
    # PASSO 3: Criar tabelas Silver
    if not create_silver_tables(engine):
        print("\n  FALHA: Erro ao criar tabelas Silver")
        sys.exit(1)
    
    # PASSO 4: Carregar dados Bronze
    if not load_bronze_data(engine):
        print("\n  FALHA: Erro ao carregar dados Bronze")
        sys.exit(1)
    
    # PASSO 5: Transformar e carregar Silver
    if not transform_and_load_silver(engine):
        print("\n  FALHA: Erro ao processar Silver")
        sys.exit(1)
    
    # PASSO 6: Verificar dados
    if not verify_data(engine):
        print("\n  Verificação apresentou problemas")
    
    # Sucesso!
    print("\n" + "="*60)
    print("  ETL CONCLUÍDO COM SUCESSO!")
    print("="*60)
    print(f"Fim: {datetime.now().strftime('%H:%M:%S')}")
    print("\nEstrutura criada:")
    print("  • bronze.vehicle_prices (1M registros)")
    print("  • silver.dim_modelo (modelos únicos)")
    print("  • silver.dim_especificacao (especificações únicas)")
    print("  • silver.fato_veiculo (1M fatos)")
    print("\nAcesse o Jupyter para análises:")
    print("  http://localhost:8888")
    print()


if __name__ == "__main__":
    main()
