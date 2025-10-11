#!/usr/bin/env python3
"""
Popular o banco de dados a partir do CSV
Executa automaticamente via docker-compose
"""
import sys
import os
import time
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text

# Configurações do banco
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'sbd2_vehicle'),
    'user': os.getenv('POSTGRES_USER', 'sbd2_vehicle'),
    'password': os.getenv('POSTGRES_PASSWORD', 'sbd2_vehicle')
}

# Caminho do CSV
CSV_PATH = '/home/jovyan/work/data/bronze/vehicle_price_prediction.csv'

def get_engine():
    """Cria engine de conexão com o banco"""
    connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(connection_string)

def wait_for_database(max_attempts=30, delay=5):
    """Aguarda o banco de dados ficar disponível"""
    for attempt in range(max_attempts):
        try:
            engine = get_engine()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            if attempt < max_attempts - 1:
                print(f"Tentativa {attempt + 1}/{max_attempts}: Aguardando...")
                time.sleep(delay)
    
    print("Timeout: Banco de dados não ficou disponível")
    return False

def create_tables(engine):
    """Cria as tabelas necessárias"""
    
    # Tabela Bronze (dados brutos)
    create_bronze_table = """
    CREATE TABLE IF NOT EXISTS vehicle_prices_bronze (
        id SERIAL PRIMARY KEY,
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
        _ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        _source_file VARCHAR(255)
    );
    """
    
    # Tabela Silver (dados limpos)
    create_silver_table = """
    CREATE TABLE IF NOT EXISTS vehicle_prices_silver (
        id SERIAL PRIMARY KEY,
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
        price_category VARCHAR(50),
        age_category VARCHAR(50),
        mileage_category VARCHAR(50),
        condition_score INTEGER,
        _quality_score FLOAT,
        _processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        _source_bronze_id INTEGER
    );
    """
    
    # Tabela de logs
    create_logs_table = """
    CREATE TABLE IF NOT EXISTS etl_logs (
        id SERIAL PRIMARY KEY,
        operation VARCHAR(50),
        status VARCHAR(50),
        records_processed INTEGER,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        duration_seconds FLOAT,
        message TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(text(create_bronze_table))
            conn.execute(text(create_silver_table))
            conn.execute(text(create_logs_table))
            conn.commit()
        return True
    except Exception as e:
        print(f"Erro ao criar tabelas: {e}")
        return False

def load_bronze_data(engine):
    """Carrega dados do CSV para a tabela Bronze"""
    print("\n" + "="*60)
    print("="*60)
    
    start_time = datetime.now()
    
    try:
        # Verificar se já tem dados
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM vehicle_prices_bronze"))
            count = result.scalar()
            if count > 0:
                print(f"Tabela Bronze já contém {count:,} registros. Pulando inserção.")
                return True
        
        # Ler CSV
        df = pd.read_csv(CSV_PATH)
        print(f"CSV carregado: {len(df):,} linhas, {len(df.columns)} colunas")
        
        # Adicionar metadados
        df['_ingestion_timestamp'] = datetime.now()
        df['_source_file'] = 'vehicle_price_prediction.csv'
        
        # Inserir no banco (em chunks)
        df.to_sql('vehicle_prices_bronze', engine, if_exists='append', index=False, chunksize=10000)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f"   Registros: {len(df):,}")
        print(f"   Tempo: {duration:.2f} segundos")
        
        # Registrar log
        log_operation(engine, 'BRONZE_LOAD', 'SUCCESS', len(df), start_time, end_time)
        
        return True
        
    except Exception as e:
        end_time = datetime.now()
        print(f"Erro ao carregar dados Bronze: {e}")
        log_operation(engine, 'BRONZE_LOAD', 'ERROR', 0, start_time, end_time, str(e))
        return False

def transform_and_load_silver(engine):
    """Transforma dados Bronze e carrega na tabela Silver"""
    print("\n" + "="*60)
    print("="*60)
    
    start_time = datetime.now()
    
    try:
        # Verificar se já tem dados
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM vehicle_prices_silver"))
            count = result.scalar()
            if count > 0:
                return True
        
        # Ler dados da tabela Bronze
        df = pd.read_sql("SELECT * FROM vehicle_prices_bronze", engine)
        print(f"{len(df):,} registros carregados")
        
        # Transformações
        
        # 1. Tratar valores nulos em accident_history
        df['accident_history'] = df['accident_history'].fillna('Unknown')
        
        # 2. Criar categorias de preço
        def categorize_price(price):
            if pd.isna(price):
                return 'Unknown'
            elif price < 15000:
                return 'Budget'
            elif price < 30000:
                return 'Economy'
            elif price < 50000:
                return 'Mid-Range'
            elif price < 80000:
                return 'Premium'
            else:
                return 'Luxury'
        
        df['price_category'] = df['price'].apply(categorize_price)
        
        # 3. Criar categorias de idade
        current_year = datetime.now().year
        def categorize_age(year):
            if pd.isna(year):
                return 'Unknown'
            age = current_year - year
            if age <= 3:
                return 'New'
            elif age <= 7:
                return 'Recent'
            elif age <= 15:
                return 'Used'
            else:
                return 'Old'
        
        df['age_category'] = df['year'].apply(categorize_age)
        
        # 4. Criar categorias de quilometragem
        def categorize_mileage(miles):
            if pd.isna(miles):
                return 'Unknown'
            elif miles < 30000:
                return 'Low'
            elif miles < 80000:
                return 'Medium'
            elif miles < 150000:
                return 'High'
            else:
                return 'Very High'
        
        df['mileage_category'] = df['mileage'].apply(categorize_mileage)
        
        # 5. Mapear condição para score numérico
        condition_map = {
            'Excellent': 5,
            'Very Good': 4,
            'Good': 3,
            'Fair': 2,
            'Poor': 1
        }
        df['condition_score'] = df['condition'].map(condition_map).fillna(3)
        
        # 6. Calcular score de qualidade (0-100)
        df['_quality_score'] = 100.0  # Começar com 100
        df.loc[df['accident_history'] == 'Unknown', '_quality_score'] -= 10
        df.loc[df['accident_history'] == 'Major', '_quality_score'] -= 20
        df.loc[df['accident_history'] == 'Minor', '_quality_score'] -= 5
        df.loc[df['condition'] == 'Poor', '_quality_score'] -= 15
        df.loc[df['condition'] == 'Fair', '_quality_score'] -= 10
        df.loc[df['owner_count'] > 3, '_quality_score'] -= 5
        df.loc[df['mileage'] > 150000, '_quality_score'] -= 10
        
        # 7. Adicionar metadados
        df['_processed_timestamp'] = datetime.now()
        df['_source_bronze_id'] = df['id']
        
        # Remover colunas de metadados do Bronze
        df = df.drop(columns=['_ingestion_timestamp', '_source_file'], errors='ignore')
        
        # Selecionar apenas colunas da tabela Silver
        silver_columns = [
            'make', 'model', 'year', 'mileage', 'engine_hp', 
            'transmission', 'fuel_type', 'drivetrain', 'body_type',
            'exterior_color', 'interior_color', 'owner_count',
            'accident_history', 'seller_type', 'condition', 'trim',
            'vehicle_age', 'mileage_per_year', 'brand_popularity', 'price',
            'price_category', 'age_category', 'mileage_category',
            'condition_score', '_quality_score', '_processed_timestamp', '_source_bronze_id'
        ]
        
        df_silver = df[silver_columns]
        
        # Inserir no banco
        df_silver.to_sql('vehicle_prices_silver', engine, if_exists='append', index=False, chunksize=10000)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f"   Registros: {len(df_silver):,}")
        print(f"   Tempo: {duration:.2f} segundos")
        
        # Registrar log
        log_operation(engine, 'SILVER_LOAD', 'SUCCESS', len(df_silver), start_time, end_time)
        
        return True
        
    except Exception as e:
        end_time = datetime.now()
        print(f"Erro ao processar dados Silver: {e}")
        log_operation(engine, 'SILVER_LOAD', 'ERROR', 0, start_time, end_time, str(e))
        return False

def log_operation(engine, operation, status, records, start_time, end_time, message=""):
    """Registra operação no log"""
    try:
        duration = (end_time - start_time).total_seconds()
        log_entry = pd.DataFrame([{
            'operation': operation,
            'status': status,
            'records_processed': records,
            'start_time': start_time,
            'end_time': end_time,
            'duration_seconds': duration,
            'message': message
        }])
        log_entry.to_sql('etl_logs', engine, if_exists='append', index=False)
    except Exception as e:
        print(f"Erro ao registrar log: {e}")

def verify_data(engine):
    """Verifica se os dados foram carregados corretamente"""
    print("\n" + "="*60)
    print("="*60)
    
    try:
        with engine.connect() as conn:
            # Contar Bronze
            result = conn.execute(text("SELECT COUNT(*) FROM vehicle_prices_bronze"))
            bronze_count = result.scalar()
            print(f"Tabela Bronze: {bronze_count:,} registros")
            
            # Contar Silver
            result = conn.execute(text("SELECT COUNT(*) FROM vehicle_prices_silver"))
            silver_count = result.scalar()
            print(f"Tabela Silver: {silver_count:,} registros")
            
            # Contar logs
            result = conn.execute(text("SELECT COUNT(*) FROM etl_logs"))
            logs_count = result.scalar()
            print(f"Logs de execução: {logs_count} entradas")
            
            # Mostrar distribuição de categorias
            if silver_count > 0:
                print("\nDistribuição de Categorias:")
                result = conn.execute(text("""
                    SELECT price_category, COUNT(*) as total 
                    FROM vehicle_prices_silver 
                    GROUP BY price_category 
                    ORDER BY total DESC
                """))
                for row in result:
                    print(f"   {row[0]}: {row[1]:,}")
        
        return True
        
    except Exception as e:
        print(f"Erro na verificação: {e}")
        return False


def main():
    print("="*60)
    print("="*60)
    print(f"Iniciado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("")
    
    # 1. Aguardar banco ficar disponível
    if not wait_for_database():
        print("\nFalha ao conectar com o banco de dados")
        sys.exit(1)
    
    # 2. Criar engine
    engine = get_engine()
    
    # 3. Criar tabelas
    if not create_tables(engine):
        print("\nFalha ao criar tabelas")
        sys.exit(1)
    
    # 4. Carregar dados Bronze
    if not load_bronze_data(engine):
        print("\nFalha ao carregar dados Bronze")
        sys.exit(1)
    
    # 5. Transformar e carregar dados Silver
    if not transform_and_load_silver(engine):
        print("\nFalha ao processar dados Silver")
        sys.exit(1)
    
    # 6. Verificar dados
    if not verify_data(engine):
        print("\nVerificação dos dados apresentou problemas")

if __name__ == "__main__":
    main()

