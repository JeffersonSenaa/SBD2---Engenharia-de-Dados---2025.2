# Comando para acesso as Tabelas

```
SCHEMA: bronze
├─ vehicle_prices (1.000.000 registros)

SCHEMA: silver
├─ dim_modelo (981.719 registros)
├─ dim_especificacao (29.671 registros)
└─ fato_veiculo (1.000.000 registros)
```

---

## PROBLEMA COMUM: "Não encontro as tabelas"

**Por quê:**
- As tabelas estão em **schemas específicos** (`bronze` e `silver`)
- O comando `\dt` sozinho lista apenas o schema `public`
- Você precisa especificar o schema!

---

## Solução 1: Via Terminal (psql)

### Conectar ao Banco:

```bash
docker exec postgres-db psql -U sbd2_vehicle -d sbd2_vehicle
```

### Comandos CORRETOS para listar tabelas:

```sql
-- ERRADO (não mostra nada)
\dt

-- CORRETO - Listar schemas
\dn

-- CORRETO - Listar tabelas do Bronze
\dt bronze.*

-- CORRETO - Listar tabelas do Silver
\dt silver.*

-- CORRETO - Listar TODAS as tabelas
\dt *.*
```

### Consultas SQL:

```sql
-- Ver dados da Bronze
SELECT * FROM bronze.vehicle_prices LIMIT 10;

-- Ver dados da dim_modelo
SELECT * FROM silver.dim_modelo LIMIT 10;

-- Ver dados da dim_especificacao
SELECT * FROM silver.dim_especificacao LIMIT 10;

-- Ver dados do fato
SELECT * FROM silver.fato_veiculo LIMIT 10;

-- Contar registros
SELECT COUNT(*) FROM bronze.vehicle_prices;
SELECT COUNT(*) FROM silver.dim_modelo;
SELECT COUNT(*) FROM silver.dim_especificacao;
SELECT COUNT(*) FROM silver.fato_veiculo;
```

---

## Solução 2: Via Jupyter Notebook

### Conectar ao Banco:

```python
import pandas as pd
from sqlalchemy import create_engine

# Criar conexão
engine = create_engine('postgresql://sbd2_vehicle:sbd2_vehicle@postgres:5432/sbd2_vehicle')

# Listar schemas
query = """
SELECT schema_name 
FROM information_schema.schemata 
WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
"""
schemas = pd.read_sql(query, engine)
print("Schemas disponíveis:")
print(schemas)

# Listar tabelas do Bronze
query = """
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'bronze'
"""
tabelas_bronze = pd.read_sql(query, engine)
print("\nTabelas no schema BRONZE:")
print(tabelas_bronze)

# Listar tabelas do Silver
query = """
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'silver'
"""
tabelas_silver = pd.read_sql(query, engine)
print("\nTabelas no schema SILVER:")
print(tabelas_silver)
```

### Ver Dados:

```python
# Bronze
df_bronze = pd.read_sql("SELECT * FROM bronze.vehicle_prices LIMIT 10", engine)
print(df_bronze)

# Dim Modelo
df_modelo = pd.read_sql("SELECT * FROM silver.dim_modelo LIMIT 10", engine)
print(df_modelo)

# Dim Especificação
df_espec = pd.read_sql("SELECT * FROM silver.dim_especificacao LIMIT 10", engine)
print(df_espec)

# Fato Veículo
df_fato = pd.read_sql("SELECT * FROM silver.fato_veiculo LIMIT 10", engine)
print(df_fato)
```

---

## Solução 3: Via DBeaver/pgAdmin

Se você está usando um cliente visual (DBeaver, pgAdmin, etc):

1. **Conecte ao banco:**
   - Host: `localhost`
   - Port: `5433` (porta externa do Docker)
   - Database: `sbd2_vehicle`
   - User: `sbd2_vehicle`
   - Password: `sbd2_vehicle`

2. **Expanda os schemas:**
   ```
   Databases
   └─ sbd2_vehicle
      ├─ Schemas
      │  ├─ public (vazio)
      │  ├─ bronze 
      │  │  └─ Tables
      │  │     └─ vehicle_prices
      │  └─ silver 
      │     └─ Tables
      │        ├─ dim_modelo
      │        ├─ dim_especificacao
      │        └─ fato_veiculo
   ```

---

## Comandos de Teste Rápido

### Terminal:

```bash
# Conectar
docker exec postgres-db psql -U sbd2_vehicle -d sbd2_vehicle

# Dentro do psql:
\dn                    -- Listar schemas
\dt bronze.*           -- Tabelas do Bronze
\dt silver.*           -- Tabelas do Silver

-- Consultas rápidas
SELECT COUNT(*) FROM bronze.vehicle_prices;
SELECT COUNT(*) FROM silver.dim_modelo;
SELECT COUNT(*) FROM silver.fato_veiculo;

-- Exemplo com JOIN
SELECT 
    m.make,
    COUNT(*) as total
FROM silver.fato_veiculo f
JOIN silver.dim_modelo m ON f.id_modelo = m.id_modelo
GROUP BY m.make
ORDER BY total DESC
LIMIT 5;
```

---

## Exemplo Completo de Análise

```python
# No Jupyter Notebook
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://sbd2_vehicle:sbd2_vehicle@postgres:5432/sbd2_vehicle')

# Análise 1: Preço médio por marca
query = """
SELECT 
    m.make as marca,
    ROUND(AVG(f.price)::numeric, 2) as preco_medio,
    COUNT(*) as quantidade
FROM silver.fato_veiculo f
JOIN silver.dim_modelo m ON f.id_modelo = m.id_modelo
GROUP BY m.make
ORDER BY preco_medio DESC
LIMIT 10
"""

df = pd.read_sql(query, engine)
print("Top 10 Marcas Mais Caras (média):")
print(df)

# Análise 2: Impacto de acidentes no valor
query = """
SELECT 
    e.accident_history as historico_acidente,
    ROUND(AVG(f.price)::numeric, 2) as preco_medio,
    COUNT(*) as quantidade
FROM silver.fato_veiculo f
JOIN silver.dim_especificacao e ON f.id_especificacao = e.id_especificacao
GROUP BY e.accident_history
ORDER BY preco_medio DESC
"""

df = pd.read_sql(query, engine)
print("\nImpacto de Acidentes no Preço:")
print(df)
```

---

## Problemas Comuns

### 1. "relation does not exist"

**Erro:**
```sql
SELECT * FROM vehicle_prices;
ERROR: relation "vehicle_prices" does not exist
```

**Solução:** Especifique o schema!
```sql
SELECT * FROM bronze.vehicle_prices;  -- ✓ CORRETO
```

### 2. "schema does not exist"

**Solução:** Os schemas foram criados. Verifique com:
```sql
\dn
```

Você deve ver:
```
bronze
silver
public
```

### 3. No DBeaver/pgAdmin não vejo nada

**Solução:**
- Clique com botão direito em "Schemas"
- Escolha "Refresh"
- Expanda `bronze` e `silver`

### 4. "permission denied"

**Solução:** Você está usando o usuário correto?
```bash
# ✓ CORRETO
psql -U sbd2_vehicle -d sbd2_vehicle

# ✗ ERRADO
psql -U postgres -d postgres
```

---

## 📋 Verificação Completa

Cole no terminal do PostgreSQL:

```sql
-- 1. Ver schemas
\dn

-- 2. Ver tabelas do Bronze
\dt bronze.*

-- 3. Ver tabelas do Silver
\dt silver.*

-- 4. Contar registros
SELECT 'Bronze' as tabela, COUNT(*) as registros FROM bronze.vehicle_prices
UNION ALL
SELECT 'Dim Modelo', COUNT(*) FROM silver.dim_modelo
UNION ALL
SELECT 'Dim Especificação', COUNT(*) FROM silver.dim_especificacao
UNION ALL
SELECT 'Fato Veículo', COUNT(*) FROM silver.fato_veiculo;

-- 5. Ver estrutura das tabelas
\d bronze.vehicle_prices
\d silver.dim_modelo
\d silver.dim_especificacao
\d silver.fato_veiculo
```

---

## Exemplo de Consulta Completa

```sql
-- Análise: Veículos mais caros por marca e modelo
SELECT 
    m.make as marca,
    m.model as modelo,
    m.year as ano,
    ROUND(AVG(f.price)::numeric, 2) as preco_medio,
    COUNT(*) as quantidade
FROM silver.fato_veiculo f
JOIN silver.dim_modelo m ON f.id_modelo = m.id_modelo
WHERE m.year >= 2020
GROUP BY m.make, m.model, m.year
ORDER BY preco_medio DESC
LIMIT 20;
```

---

## Dica Importante

**SEMPRE especifique o schema nas consultas:**

```sql
SELECT * FROM bronze.vehicle_prices;
SELECT * FROM silver.dim_modelo;
SELECT * FROM silver.fato_veiculo;
```

---

## Teste Rápido

1. **Conecte ao banco:**
   ```bash
   docker exec postgres-db psql -U sbd2_vehicle -d sbd2_vehicle
   ```

2. **Cole este comando:**
   ```sql
   SELECT 
       'Bronze' as camada,
       COUNT(*) as registros
   FROM bronze.vehicle_prices
   UNION ALL
   SELECT 'Silver - Fatos', COUNT(*)
   FROM silver.fato_veiculo;
   ```

3. **Resultado esperado:**
   ```
       camada      | registros
   ----------------+-----------
    Bronze         | 1,000,000
    Silver - Fatos | 1,000,000
   ```

---


