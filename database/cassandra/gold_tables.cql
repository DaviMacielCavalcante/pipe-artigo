CREATE TABLE gold.empresas_por_uf(
    uf TEXT,
    quantidade INT,
    PRIMARY KEY (uf)
);

CREATE TABLE gold.capital_por_porte (
    porte INT,
    capital_medio DECIMAL,
    PRIMARY KEY (porte)
);

CREATE TABLE gold.socios_por_empresa (
    cnpj_basico TEXT,
    quantidade_socios INT,
    PRIMARY KEY (cnpj_basico)
);

CREATE TABLE gold.filiais_inaptas_por_empresa (
    cnpj_basico TEXT, 
    quantidade INT, 
    PRIMARY KEY (cnpj_basico)
);

CREATE TABLE gold.ranking_empresas_filiais_inaptas (
    bucket TEXT, 
    quantidade INT, 
    cnpj_basico TEXT, 
    PRIMARY KEY (bucket, quantidade, cnpj_basico)
) WITH CLUSTERING ORDER BY (quantidade DESC);