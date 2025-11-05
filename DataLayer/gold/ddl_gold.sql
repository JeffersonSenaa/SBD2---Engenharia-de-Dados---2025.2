CREATE SCHEMA IF NOT EXISTS gold;

DROP TABLE IF EXISTS gold.FATO_VEICULO;
DROP TABLE IF EXISTS gold.DIM_MODELO;
DROP TABLE IF EXISTS gold.DIM_CONDICAO;
DROP TABLE IF EXISTS gold.DIM_COR;

CREATE TABLE gold.DIM_COR (
    SRK_cor BIGSERIAL PRIMARY KEY,
    exterior_color VARCHAR(6),
    interior_color VARCHAR(6)
);

CREATE TABLE gold.DIM_CONDICAO (
    SRK_condicao BIGSERIAL PRIMARY KEY,
    seller_type VARCHAR(7),
    condition VARCHAR(9),
    accident_history VARCHAR(5)
);

CREATE TABLE gold.DIM_MODELO (
    SRK_modelo BIGSERIAL PRIMARY KEY,
    make VARCHAR(13),
    model VARCHAR(14),
    year INTEGER,
    trim VARCHAR(7),
    engine_hp INTEGER,
    transmission VARCHAR(9),
    fuel_type VARCHAR(8),
    drivetrain VARCHAR(3),
    body_type VARCHAR(12)
);

CREATE TABLE gold.FATO_VEICULO (
    SRK_veiculo BIGSERIAL PRIMARY KEY,
    mileage INTEGER,
    price FLOAT,
    owner_count INTEGER,
    vehicle_age INTEGER,
    mileage_per_year FLOAT,
    brand_popularity FLOAT,
    
    SRK_modelo BIGINT NOT NULL,
    SRK_condicao BIGINT NOT NULL,
    SRK_cor BIGINT NOT NULL
);

ALTER TABLE gold.FATO_VEICULO ADD CONSTRAINT SRK_FATO_VEICULO_modelo
    FOREIGN KEY (SRK_modelo)
    REFERENCES gold.DIM_MODELO (SRK_modelo);
    ON DELETE RESTRICT;

ALTER TABLE gold.FATO_VEICULO ADD CONSTRAINT SRK_FATO_VEICULO_condicao
    FOREIGN KEY (SRK_condicao)
    REFERENCES gold.DIM_CONDICAO (SRK_condicao);
    ON DELETE RESTRICT;

ALTER TABLE gold.FATO_VEICULO ADD CONSTRAINT SRK_FATO_VEICULO_cor
    FOREIGN KEY (SRK_cor)
    REFERENCES gold.DIM_COR (SRK_cor);
    ON DELETE RESTRICT;

CREATE INDEX idx_fato_veiculo_modelo ON gold.FATO_VEICULO (SRK_modelo);
CREATE INDEX idx_fato_veiculo_condicao ON gold.FATO_VEICULO (SRK_condicao);
CREATE INDEX idx_fato_veiculo_cor ON gold.FATO_VEICULO (SRK_cor);