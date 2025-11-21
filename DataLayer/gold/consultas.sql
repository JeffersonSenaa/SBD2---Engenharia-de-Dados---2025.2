-- ===================================================================
-- ANALYTICS - Camada Gold (Data Warehouse)
-- Consultas SQL para Dashboards de Business Intelligence
-- ===================================================================
-- Schema: dw
-- Tabelas: DIM_MODELO, DIM_CONDICAO, DIM_COR, FATO_VEICULO
-- ===================================================================


-- ===================================================================
-- 1. VISÃO EXECUTIVA - KPIs Principais do Negócio
-- ===================================================================
-- Cláusulas SQL: SELECT, COUNT, AVG, MIN, MAX, ROUND, CAST, CASE WHEN
-- Uso: Dashboard principal - Indicadores gerais
-- ===================================================================

SELECT 
    COUNT(*) AS total_veiculos,
    COUNT(DISTINCT m.make) AS total_marcas,
    COUNT(DISTINCT m.model) AS total_modelos,
    ROUND(AVG(f.price)::numeric, 2) AS preco_medio,
    ROUND(MIN(f.price)::numeric, 2) AS preco_minimo,
    ROUND(MAX(f.price)::numeric, 2) AS preco_maximo,
    ROUND(AVG(f.mileage)::numeric, 2) AS quilometragem_media,
    MIN(m.year) AS ano_mais_antigo,
    MAX(m.year) AS ano_mais_recente,
    COUNT(CASE WHEN c.accident_history = 'nenhum' THEN 1 END) AS veiculos_sem_acidente,
    ROUND(
        100.0 * COUNT(CASE WHEN c.accident_history = 'nenhum' THEN 1 END) / COUNT(*), 
        2
    ) AS percentual_sem_acidente
FROM dw.FATO_VEICULO f
JOIN dw.DIM_MODELO m ON f.SRK_modelo = m.SRK_modelo
JOIN dw.DIM_CONDICAO c ON f.SRK_condicao = c.SRK_condicao;


-- ===================================================================
-- 2. RANKING DE MARCAS POR VALOR DE MERCADO
-- ===================================================================
-- Cláusulas SQL: JOIN, GROUP BY, HAVING, ORDER BY, LIMIT, Agregações múltiplas
-- Uso: Dashboard de posicionamento de marcas premium
-- ===================================================================

SELECT 
    m.make AS marca,
    COUNT(*) AS quantidade_veiculos,
    ROUND(AVG(f.price)::numeric, 2) AS preco_medio,
    ROUND(MIN(f.price)::numeric, 2) AS preco_minimo,
    ROUND(MAX(f.price)::numeric, 2) AS preco_maximo,
    ROUND(STDDEV(f.price)::numeric, 2) AS desvio_padrao_preco,
    ROUND(AVG(m.engine_hp)::numeric, 2) AS potencia_media_hp
FROM dw.FATO_VEICULO f
JOIN dw.DIM_MODELO m ON f.SRK_modelo = m.SRK_modelo
GROUP BY m.make
HAVING COUNT(*) >= 10  -- Apenas marcas com pelo menos 10 veículos
ORDER BY preco_medio DESC
LIMIT 15;


-- ===================================================================
-- 3. ANÁLISE DE DEPRECIAÇÃO POR ANO E MARCA
-- ===================================================================
-- Cláusulas SQL: JOIN, WHERE, GROUP BY, HAVING, ORDER BY, Window Functions
-- Uso: Dashboard de análise de depreciação temporal
-- ===================================================================

SELECT 
    m.make AS marca,
    m.year AS ano,
    COUNT(*) AS quantidade,
    ROUND(AVG(f.price)::numeric, 2) AS preco_medio,
    ROUND(AVG(f.vehicle_age)::numeric, 2) AS idade_media,
    ROUND(AVG(f.mileage_per_year)::numeric, 2) AS km_por_ano_medio,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY f.price)::numeric, 2) AS mediana_preco
FROM dw.FATO_VEICULO f
JOIN dw.DIM_MODELO m ON f.SRK_modelo = m.SRK_modelo
WHERE m.year >= 2015  -- Últimos 10 anos aproximadamente
GROUP BY m.make, m.year
HAVING COUNT(*) >= 3
ORDER BY m.make, m.year DESC;


-- ===================================================================
-- 4. IMPACTO DE ACIDENTES NO PREÇO
-- ===================================================================
-- Cláusulas SQL: JOIN, GROUP BY, CASE WHEN, ORDER BY, Múltiplas agregações
-- Uso: Dashboard de análise de risco e precificação
-- ===================================================================

SELECT 
    c.accident_history AS historico_acidentes,
    COUNT(*) AS quantidade,
    ROUND(AVG(f.price)::numeric, 2) AS preco_medio,
    ROUND(MIN(f.price)::numeric, 2) AS preco_minimo,
    ROUND(MAX(f.price)::numeric, 2) AS preco_maximo,
    ROUND(AVG(f.mileage)::numeric, 2) AS quilometragem_media,
    ROUND(AVG(f.owner_count)::numeric, 2) AS media_proprietarios,
    CASE 
        WHEN c.accident_history = 'nenhum' THEN 'Sem risco'
        WHEN c.accident_history = 'menor' THEN 'Risco baixo'
        ELSE 'Risco alto'
    END AS categoria_risco
FROM dw.FATO_VEICULO f
JOIN dw.DIM_CONDICAO c ON f.SRK_condicao = c.SRK_condicao
GROUP BY c.accident_history
ORDER BY preco_medio DESC;


-- ===================================================================
-- 5. MATRIZ DE SEGMENTAÇÃO: Acidentes x Faixa de Preço
-- ===================================================================
-- Cláusulas SQL: JOIN, CASE WHEN aninhado, GROUP BY, ORDER BY
-- Uso: Dashboard de análise cruzada para segmentação
-- ===================================================================

SELECT 
    c.accident_history AS historico_acidentes,
    CASE 
        WHEN f.price < 15000 THEN '1. Economia (< 15k)'
        WHEN f.price BETWEEN 15000 AND 30000 THEN '2. Intermediário (15k-30k)'
        WHEN f.price BETWEEN 30000 AND 50000 THEN '3. Premium (30k-50k)'
        ELSE '4. Luxo (> 50k)'
    END AS faixa_preco,
    COUNT(*) AS quantidade,
    ROUND(AVG(f.price)::numeric, 2) AS preco_medio_segmento,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentual_total
FROM dw.FATO_VEICULO f
JOIN dw.DIM_CONDICAO c ON f.SRK_condicao = c.SRK_condicao
GROUP BY c.accident_history, faixa_preco
ORDER BY c.accident_history, faixa_preco;


-- ===================================================================
-- 6. TOP MODELOS MAIS POPULARES E SEUS ATRIBUTOS
-- ===================================================================
-- Cláusulas SQL: JOIN (múltiplos), GROUP BY, ORDER BY, LIMIT
-- Uso: Dashboard de inventário - Modelos em destaque
-- ===================================================================

SELECT 
    m.make AS marca,
    m.model AS modelo,
    COUNT(*) AS quantidade,
    ROUND(AVG(f.price)::numeric, 2) AS preco_medio,
    ROUND(AVG(f.mileage)::numeric, 2) AS km_media,
    ROUND(AVG(m.engine_hp)::numeric, 2) AS potencia_media_hp,
    m.fuel_type AS tipo_combustivel_predominante,
    m.transmission AS transmissao_predominante,
    MIN(m.year) AS ano_mais_antigo,
    MAX(m.year) AS ano_mais_recente
FROM dw.FATO_VEICULO f
JOIN dw.DIM_MODELO m ON f.SRK_modelo = m.SRK_modelo
GROUP BY m.make, m.model, m.fuel_type, m.transmission
ORDER BY quantidade DESC
LIMIT 20;


-- ===================================================================
-- 7. ANÁLISE DE COMBUSTÍVEL E TRANSMISSÃO
-- ===================================================================
-- Cláusulas SQL: JOIN, GROUP BY (múltiplas colunas), ORDER BY
-- Uso: Dashboard de especificações técnicas do mercado
-- ===================================================================

SELECT 
    m.fuel_type AS tipo_combustivel,
    m.transmission AS transmissao,
    COUNT(*) AS quantidade,
    ROUND(AVG(f.price)::numeric, 2) AS preco_medio,
    ROUND(AVG(m.engine_hp)::numeric, 2) AS potencia_media_hp,
    ROUND(AVG(f.mileage)::numeric, 2) AS km_media,
    ROUND(AVG(f.brand_popularity)::numeric, 4) AS popularidade_media
FROM dw.FATO_VEICULO f
JOIN dw.DIM_MODELO m ON f.SRK_modelo = m.SRK_modelo
GROUP BY m.fuel_type, m.transmission
ORDER BY quantidade DESC;


-- ===================================================================
-- 8. OPORTUNIDADES DE NEGÓCIO - Melhor Custo-Benefício
-- ===================================================================
-- Cláusulas SQL: Subquery, JOIN (múltiplos), WHERE (múltiplas condições), ORDER BY, LIMIT
-- Uso: Dashboard de recomendações de compra
-- ===================================================================

SELECT 
    m.make || ' ' || m.model AS veiculo,
    m.year AS ano,
    f.mileage AS quilometragem,
    ROUND(f.price::numeric, 2) AS preco,
    ROUND((SELECT AVG(price) FROM dw.FATO_VEICULO) - f.price::numeric, 2) AS economia_vs_media,
    m.engine_hp AS potencia_hp,
    m.fuel_type AS combustivel,
    m.transmission AS transmissao,
    c.accident_history AS historico_acidentes,
    c.condition AS condicao,
    co.exterior_color AS cor_externa
FROM dw.FATO_VEICULO f
JOIN dw.DIM_MODELO m ON f.SRK_modelo = m.SRK_modelo
JOIN dw.DIM_CONDICAO c ON f.SRK_condicao = c.SRK_condicao
JOIN dw.DIM_COR co ON f.SRK_cor = co.SRK_cor
WHERE f.price < (SELECT AVG(price) FROM dw.FATO_VEICULO)
  AND f.mileage < 100000
  AND m.year >= 2018
  AND c.accident_history = 'nenhum'
  AND f.vehicle_age <= 6
ORDER BY f.price ASC
LIMIT 30;


-- ===================================================================
-- 9. ANÁLISE DE CONDIÇÃO E TIPO DE VENDEDOR
-- ===================================================================
-- Cláusulas SQL: JOIN, GROUP BY, ORDER BY, Agregações
-- Uso: Dashboard de análise de canais de venda
-- ===================================================================

SELECT 
    c.seller_type AS tipo_vendedor,
    c.condition AS condicao,
    COUNT(*) AS quantidade,
    ROUND(AVG(f.price)::numeric, 2) AS preco_medio,
    ROUND(AVG(f.mileage)::numeric, 2) AS km_media,
    ROUND(AVG(f.owner_count)::numeric, 2) AS media_proprietarios,
    COUNT(CASE WHEN c.accident_history != 'nenhum' THEN 1 END) AS total_com_acidentes
FROM dw.FATO_VEICULO f
JOIN dw.DIM_CONDICAO c ON f.SRK_condicao = c.SRK_condicao
GROUP BY c.seller_type, c.condition
ORDER BY quantidade DESC;


-- ===================================================================
-- 10. ANÁLISE DE CORES MAIS VALORIZADAS
-- ===================================================================
-- Cláusulas SQL: JOIN (múltiplos), GROUP BY, HAVING, ORDER BY
-- Uso: Dashboard de tendências de mercado
-- ===================================================================

SELECT 
    co.exterior_color AS cor_externa,
    co.interior_color AS cor_interna,
    COUNT(*) AS quantidade,
    ROUND(AVG(f.price)::numeric, 2) AS preco_medio,
    ROUND(AVG(m.year)::numeric, 0) AS ano_medio,
    ROUND(AVG(f.brand_popularity)::numeric, 4) AS popularidade_media
FROM dw.FATO_VEICULO f
JOIN dw.DIM_COR co ON f.SRK_cor = co.SRK_cor
JOIN dw.DIM_MODELO m ON f.SRK_modelo = m.SRK_modelo
GROUP BY co.exterior_color, co.interior_color
HAVING COUNT(*) >= 5  -- Apenas combinações com pelo menos 5 veículos
ORDER BY preco_medio DESC;


-- ===================================================================
-- 11. ANÁLISE MULTIDIMENSIONAL - Tipo de Carroceria
-- ===================================================================
-- Cláusulas SQL: JOIN, WHERE, GROUP BY, ORDER BY, múltiplas agregações
-- Uso: Dashboard de segmentação por categoria de veículo
-- ===================================================================

SELECT 
    m.body_type AS tipo_carroceria,
    m.drivetrain AS tracao,
    COUNT(*) AS quantidade,
    ROUND(AVG(f.price)::numeric, 2) AS preco_medio,
    ROUND(AVG(m.engine_hp)::numeric, 2) AS potencia_media_hp,
    ROUND(AVG(f.mileage)::numeric, 2) AS km_media,
    ROUND(AVG(m.year)::numeric, 0) AS ano_medio,
    COUNT(DISTINCT m.make) AS total_marcas_disponiveis
FROM dw.FATO_VEICULO f
JOIN dw.DIM_MODELO m ON f.SRK_modelo = m.SRK_modelo
WHERE m.body_type IS NOT NULL
GROUP BY m.body_type, m.drivetrain
ORDER BY quantidade DESC;


-- ===================================================================
-- 12. RELATÓRIO DETALHADO PARA ANÁLISE PREDITIVA
-- ===================================================================
-- Cláusulas SQL: JOIN (4 tabelas), múltiplas cálculos, ORDER BY
-- Uso: Exportação para modelos de Machine Learning / Análise avançada
-- ===================================================================

SELECT 
    f.SRK_veiculo AS id_veiculo,
    m.make AS marca,
    m.model AS modelo,
    m.year AS ano,
    f.vehicle_age AS idade_veiculo,
    f.mileage AS quilometragem,
    f.mileage_per_year AS km_por_ano,
    f.price AS preco,
    m.engine_hp AS potencia_hp,
    m.transmission AS transmissao,
    m.fuel_type AS tipo_combustivel,
    m.drivetrain AS tracao,
    m.body_type AS tipo_carroceria,
    f.owner_count AS num_proprietarios,
    c.accident_history AS historico_acidentes,
    c.seller_type AS tipo_vendedor,
    c.condition AS condicao,
    co.exterior_color AS cor_externa,
    co.interior_color AS cor_interna,
    f.brand_popularity AS popularidade_marca,
    CASE 
        WHEN f.price < 20000 THEN 'Economico'
        WHEN f.price < 40000 THEN 'Intermediario'
        ELSE 'Premium'
    END AS segmento_preco
FROM dw.FATO_VEICULO f
JOIN dw.DIM_MODELO m ON f.SRK_modelo = m.SRK_modelo
JOIN dw.DIM_CONDICAO c ON f.SRK_condicao = c.SRK_condicao
JOIN dw.DIM_COR co ON f.SRK_cor = co.SRK_cor
ORDER BY f.SRK_veiculo;


-- ===================================================================
-- FIM DO ARQUIVO ANALYTICS
-- ===================================================================
-- Como usar em ferramentas de BI:
-- 1. Power BI: Obter Dados > PostgreSQL > Consulta Personalizada
-- 2. Tableau: Conectar ao PostgreSQL > Consulta SQL Personalizada
-- 3. Metabase: Nova Pergunta > SQL Nativo
-- 4. Grafana: Adicionar Painel > PostgreSQL
-- ===================================================================

