
CREATE SCHEMA IF NOT EXISTS silver;
DROP TABLE IF EXISTS silver.veiculo CASCADE;
CREATE TABLE silver.veiculo (
    id BIGINT PRIMARY KEY NOT NULL,
    make VARCHAR(13),
    model VARCHAR(14),
    year INTEGER,  
    mileage INTEGER,
    engine_hp INTEGER,
    transmission VARCHAR(9),
    fuel_type VARCHAR(8),
    drivetrain VARCHAR(3),
    body_type VARCHAR(12),
    exterior_color VARCHAR(6),
    interior_color VARCHAR(6),
    owner_count INTEGER,
    accident_history VARCHAR(5),
    seller_type VARCHAR(7),
    condition VARCHAR(9),
    trim VARCHAR(7),
    vehicle_age INTEGER,
    mileage_per_year FLOAT,
    brand_popularity FLOAT,
    price FLOAT
);

CREATE INDEX idx_veiculo ON silver.veiculo (id);

