CREATE TABLE silver.empresas (
    cnpj_basico TEXT,
    razao_social_ou_nome_empresarial TEXT,
    natureza_juridica INT,
    qualificacao_do_responsavel INT,
    capital_social_da_empresa DECIMAL,
    porte_da_empresa INT,
    ente_federativo_responsavel TEXT,
    PRIMARY KEY (cnpj_basico)
);

CREATE TABLE silver.estabelecimentos (
    cnpj_basico TEXT,
    cnpj_ordem TEXT,
    cnpj_dv TEXT,
    identificador_matriz_ou_filial TEXT,
    nome_fantasia TEXT,
    situacao_cadastral INT,
    data_situacao_cadastral DATE,
    motivo_situacao_cadastral INT,
    nome_da_cidade_no_exterior TEXT,
    pais INT,
    data_de_inicio_atividade DATE,
    cnae_fiscal_principal INT,
    cnae_fiscal_secundaria TEXT,
    tipo_de_logradouro TEXT,
    numero TEXT,
    complemento TEXT,
    bairro TEXT,
    cep TEXT,
    uf TEXT,
    municipio INT,
    ddd_1 TEXT,
    telefone_1 TEXT,
    ddd_2 TEXT,
    telefone_2 TEXT,
    ddd_do_fax TEXT,
    fax TEXT,
    correio_eletronico TEXT,
    situacao_especial TEXT,
    data_da_situacao_especial DATE,
    PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
);

CREATE TABLE silver.cnaes (
    codigo INT,
    descricao TEXT,
    PRIMARY KEY (codigo) 
);

CREATE TABLE silver.motivos (
    codigo INT,
    descricao TEXT,
    PRIMARY KEY (codigo) 
);

CREATE TABLE silver.municipios (
    codigo INT,
    descricao TEXT,
    PRIMARY KEY (codigo) 
);

CREATE TABLE silver.naturezas (
    codigo INT,
    descricao TEXT,
    PRIMARY KEY (codigo) 
);

CREATE TABLE silver.paises (
    codigo INT,
    descricao TEXT,
    PRIMARY KEY (codigo) 
);

CREATE TABLE silver.qualificacoes (
    codigo INT,
    descricao TEXT,
    PRIMARY KEY (codigo) 
);

CREATE TABLE silver.simples (
    cnpj_basico TEXT,
    opcao_pelo_simples TEXT,
    data_de_opcao_pelo_simples DATE, 
    data_de_exclusao_do_simples DATE, 
    opcao_pelo_mei TEXT, 
    data_de_opcao_pelo_mei DATE,
    data_de_exclusao_do_mei DATE,
    PRIMARY KEY (cnpj_basico) 
);

CREATE TABLE silver.socios (
    cnpj_basico TEXT,
    identificador_de_socio INT,
    nome_do_socio_ou_razao_social TEXT, 
    cnpj_ou_cpf_do_socio TEXT, 
    qualificacao_do_socio INT, 
    data_de_entrada_sociedade DATE,
    pais INT,
    representante_legal TEXT,
    nome_do_representante TEXT,
    qualificacao_do_representante_legal INT,
    faixa_etaria INT,
    PRIMARY KEY (cnpj_basico, cnpj_ou_cpf_do_socio) 
);