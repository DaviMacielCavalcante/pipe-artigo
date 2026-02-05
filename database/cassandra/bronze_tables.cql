CREATE TABLE bronze.empresas (
    cnpj_basico TEXT,
    razao_social TEXT,
    natureza_juridica TEXT,
    qualificacao_do_responsavel TEXT,
    capital_social_da_empresa TEXT,
    porte_da_empresa TEXT,
    ente_federativo_responsavel TEXT,
    PRIMARY KEY (cnpj_basico)
);

CREATE TABLE bronze.estabelecimentos (
    cnpj_basico TEXT,
    cnpj_ordem TEXT,
    cnpj_dv TEXT,
    identificador_matriz_ou_filial TEXT,
    nome_fantasia TEXT,
    situacao_cadastral TEXT,
    data_situacao_cadastral TEXT,
    motivo_situacao_cadastral TEXT,
    nome_da_cidade_no_exterior TEXT,
    pais TEXT,
    data_de_inicio_atividade TEXT,
    cnae_fiscal_principal TEXT,
    cnae_fiscal_secundaria TEXT,
    tipo_de_logradouro TEXT,
    numero TEXT,
    complemento TEXT,
    bairro TEXT,
    cep TEXT,
    uf TEXT,
    municipio TEXT,
    ddd_1 TEXT,
    telefone_1 TEXT,
    ddd_2 TEXT,
    telefone_2 TEXT,
    ddd_do_fax TEXT,
    fax TEXT,
    correio_eletronico TEXT,
    situacao_especial TEXT,
    data_da_situacao_especial TEXT,
    PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
);

CREATE TABLE bronze.cnaes (
    codigo TEXT,
    descricao TEXT,
    PRIMARY KEY (codigo) 
);

CREATE TABLE bronze.motivos (
    id TEXT,
    descricao TEXT,
    PRIMARY KEY (id) 
);

CREATE TABLE bronze.municipios (
    codigo TEXT,
    descricao TEXT,
    PRIMARY KEY (codigo) 
);

CREATE TABLE bronze.naturezas (
    codigo TEXT,
    descricao TEXT,
    PRIMARY KEY (codigo) 
);

CREATE TABLE bronze.paises (
    codigo TEXT,
    descricao TEXT,
    PRIMARY KEY (codigo) 
);

CREATE TABLE bronze.qualificacoes (
    codigo TEXT,
    descricao TEXT,
    PRIMARY KEY (codigo) 
);

CREATE TABLE bronze.simples (
    cnpj_basico TEXT,
    opcao_pelo_simples TEXT,
    data_de_opcao_pelo_simples TEXT, 
    data_de_exclusao_do_simples TEXT, 
    opcao_pelo_mei TEXT, 
    data_de_opcao_pelo_mei TEXT,
    data_de_exclusao_do_mei TEXT,
    PRIMARY KEY (cnpj_basico) 
);

CREATE TABLE bronze.socios (
    cnpj_basico TEXT,
    identificador_de_socio TEXT,
    nome_do_socio_ou_razao_social TEXT, 
    cnpj_ou_cpf_do_socio TEXT, 
    qualificacao_do_socio TEXT, 
    data_de_entrada_sociedade TEXT,
    pais TEXT,
    representante_legal TEXT,
    nome_do_representante TEXT,
    qualificacao_do_representante_legal TEXT,
    faixa_etaria TEXT,
    PRIMARY KEY (cnpj_basico, cnpj_ou_cpf_do_socio) 
);