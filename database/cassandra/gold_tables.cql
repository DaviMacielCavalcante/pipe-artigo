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
    razao_social_ou_nome_empresarial TEXT,
    quantidade_de_socios INT,
    PRIMARY KEY (cnpj_basico)
);

CREATE TABLE gold.empresas_por_natureza(
    descricao TEXT,
    quantidade_de_empresas INT,
    PRIMARY KEY (descricao)
);

CREATE TABLE gold.filiais_inaptas_por_empresa (
    cnpj_basico TEXT, 
    razao_social_ou_nome_empresarial TEXT,
    quantidade INT, 
    PRIMARY KEY (cnpj_basico)
);

CREATE TABLE gold.rank_empresas_filiais_inaptas (
    bucket TEXT, 
    quantidade INT, 
    razao_social_ou_nome_empresarial TEXT,
    cnpj_basico TEXT, 
    PRIMARY KEY (bucket, quantidade, cnpj_basico)
) WITH CLUSTERING ORDER BY (quantidade DESC);