
CREATE SCHEMA IF NOT EXISTS silver;
DROP TABLE IF EXISTS silver.fato_veiculo CASCADE;
DROP TABLE IF EXISTS silver.dim_modelo CASCADE;
DROP TABLE IF EXISTS silver.dim_especificacao CASCADE;
CREATE TABLE silver.dim_modelo (
    id_modelo BIGINT PRIMARY KEY NOT NULL,
    make VARCHAR(13),
    model VARCHAR(14),
    year INTEGER,  
    engine_hp INTEGER,
    transmission VARCHAR(9),
    fuel_type VARCHAR(8),
    drivetrain VARCHAR(3),
    body_type VARCHAR(12),
    trim VARCHAR(7)
);

CREATE TABLE silver.dim_especificacao (
    id_especificacao BIGINT PRIMARY KEY NOT NULL,
    exterior_color VARCHAR(6),
    interior_color VARCHAR(6),
    owner_count INTEGER,
    accident_history VARCHAR(5),
    seller_type VARCHAR(7),
    condition VARCHAR(9),
    vehicle_age INTEGER
);

CREATE TABLE silver.fato_veiculo (
    id_fato BIGINT PRIMARY KEY NOT NULL,
    id_modelo BIGINT NOT NULL,
    id_especificacao BIGINT NOT NULL,
    mileage INTEGER,
    mileage_per_year FLOAT,
    brand_popularity FLOAT,
    price FLOAT,

    CONSTRAINT fk_fato_modelo FOREIGN KEY (id_modelo)
        REFERENCES silver.dim_modelo (id_modelo)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_fato_especificacao FOREIGN KEY (id_especificacao)
        REFERENCES silver.dim_especificacao (id_especificacao)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);

CREATE INDEX idx_fato_modelo ON silver.fato_veiculo (id_modelo);
CREATE INDEX idx_fato_especificacao ON silver.fato_veiculo (id_especificacao);
CREATE INDEX idx_fato_price ON silver.fato_veiculo (price);
CREATE INDEX idx_modelo_make_model ON silver.dim_modelo (make, model);
