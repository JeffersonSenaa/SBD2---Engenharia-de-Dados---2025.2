# Dicionário de Dados

| Atributo | Tipo de Dado | Descrição |
|:-------------:|:-------------:|:-------------:|
| Id    | BIGINT       | PK - Identificar o Veículo |
| make  | VARCHAR(13)  | Fabricante do carro  |
| model  | VARCHAR(14)  | Modelo específico do carro  |
| year |Int | Ano em que o carro foi fabricado |
|mileage |Int | Distancia total percorrida pelo carro antes da venda, em milhas |
|engine_hp | Int | Quantos cavalos o motor tem de potência |
|transmission | VARCHAR(9) | Tipo de transmissão do carro, se é manual ou automático |
|fuel_type |VARCHAR(8) |Tipo de combustível que o carro consome, se é Gasolina, Diesel ou Elétrico |
|drivetrain | VARCHAR(3) | Tipo de tração do carro, se é dianteiro(FWD), traseiro(RWD) ou integral(AWD) |
|body_type | VARCHAR(12) | Tipo de carroçaria, se é SUV, Sedan, Minivan  |
|exterior_color | VARCHAR(6) | Cor externa do carro |
|interior_color | VARCHAR(6) | Cor interna do carro |
|owner_count | Int | Número de donos anteriores dos veículo |
|accident_history | VARCHAR(5) | O histórico de acidente do veículo, sendo categorizado em nenhum, menor e maior |
|seller_type | VARCHAR(7) | tipo de entidade que vendeu o veículo, se é privado ou por vendedor(concessionário) |
|condition |VARCHAR(9) | Condição do veículo a ser vendido |
|trim |VARCHAR(7) |Nível de acabamento do veículo |
|vehicle_age | Int | Idade do carro a ser vendido |
|mileage_per_year | Float | Média de milhas percorridas por ano do veículo |
|brand_popularity | Float | Pontuação representando a popularidade da marca baseado na frequência dentro deste dataset |
|price | Float | Preço em que o carro foi comprado |