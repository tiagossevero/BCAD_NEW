-- ============================================================================
-- PROJETO GENESIS V6 - OUTPUT FINAL COM VL_CT
-- ============================================================================
-- Combina pipeline V6 (PGDAS apenas) com cálculo de Crédito Tributário
-- Formato 100% compatível com Streamlit e padrão ACL
-- Base Legal: LC 123/2006, Art. 3º, § 4º, Incisos III e IV
-- ============================================================================

SET REQUEST_POOL = 'medium';

-- ============================================================================
-- PARTE 1: TABELAS BASE (Se não existirem, executar BCAD 2026.json primeiro)
-- As tabelas bcad_01 a bcad_14c devem existir
-- ============================================================================

-- ============================================================================
-- PARTE 2: IMPORTAR TABELA SELIC PARA CÁLCULO DE JUROS
-- ============================================================================

DROP TABLE IF EXISTS gessimples.bcad_v6_selic;
CREATE TABLE gessimples.bcad_v6_selic STORED AS PARQUET AS
SELECT
    tis_referencia AS pa,
    tis_vl_indice AS taxa_selic
FROM usr_sat_ctacte.tab_indice
WHERE tid_nom_indice = 'SELIC'
  AND tis_referencia >= 202001
  AND tis_referencia <= 202612;

COMPUTE STATS gessimples.bcad_v6_selic;

-- ============================================================================
-- PARTE 3: CONSOLIDAR PGDAS PARA CÁLCULO DE VL_CT
-- ============================================================================

DROP TABLE IF EXISTS gessimples.bcad_v6_pgdas_consolidado;
CREATE TABLE gessimples.bcad_v6_pgdas_consolidado STORED AS PARQUET AS
SELECT
    nu_cnpj_grupo AS cnpj_raiz,
    nu_cnpj AS cnpj_completo,
    nu_per_ref AS periodo_apuracao,
    CAST(nu_per_ref AS STRING) AS pa,

    -- Receitas
    COALESCE(vl_rec_bruta_estab, 0) AS vl_rpa_int,
    COALESCE(vl_icms_sc, 0) AS vl_icms,

    -- VL_ATIV para cálculo do ICMS estimado (ACL)
    COALESCE(vl_venda, 0) +
    COALESCE(vl_venda_st, 0) +
    COALESCE(vl_prest_serv_com, 0) +
    COALESCE(vl_prest_serv_transp, 0) +
    COALESCE(vl_outros, 0) AS vl_ativ,

    sg_uf AS uf,

    CURRENT_TIMESTAMP() AS dt_carga

FROM usr_sat_ods.sna_pgdasd_estabelecimento_raw
WHERE nu_per_ref >= 202001
  AND nu_per_ref <= 202612;

COMPUTE STATS gessimples.bcad_v6_pgdas_consolidado;

-- ============================================================================
-- PARTE 4: CALCULAR ICMS ESTIMADO E VL_CT POR EMPRESA+PA
-- Lógica ACL: VL_CT = ICMS_ESTIMADO + JUROS + MULTA
-- ============================================================================

DROP TABLE IF EXISTS gessimples.bcad_v6_icms_estimado;
CREATE TABLE gessimples.bcad_v6_icms_estimado STORED AS PARQUET AS
SELECT
    p.cnpj_raiz,
    v.cpf_socio,
    v.num_grupo,
    p.periodo_apuracao AS pa,
    p.uf,
    p.vl_ativ,
    p.vl_icms,
    v.periodo_exclusao,
    v.ano_apuracao,

    -- tVL_ICMS_ESTIMADO (ACL): 7% da atividade menos ICMS declarado
    CASE
        WHEN p.vl_icms > 0.00
             AND p.uf = 'SC'
             AND p.periodo_apuracao >= CAST(v.periodo_exclusao AS INT)
        THEN GREATEST((p.vl_ativ * 0.07) - p.vl_icms, 0)
        ELSE 0.00
    END AS vl_icms_estimado,

    s.taxa_selic,

    CURRENT_TIMESTAMP() AS dt_carga

FROM gessimples.bcad_14c_violacoes_detalhe_pgdas v
INNER JOIN gessimples.bcad_v6_pgdas_consolidado p
    ON v.cnpj_raiz = p.cnpj_raiz
LEFT JOIN gessimples.bcad_v6_selic s
    ON p.periodo_apuracao = s.pa
WHERE v.flag_simples_nacional = 1
  AND p.periodo_apuracao >= 202001;

COMPUTE STATS gessimples.bcad_v6_icms_estimado;

-- Calcular Juros, Multa e VL_CT
DROP TABLE IF EXISTS gessimples.bcad_v6_icms_com_ct;
CREATE TABLE gessimples.bcad_v6_icms_com_ct STORED AS PARQUET AS
SELECT
    *,

    -- VL_JUROS (ACL): ICMS_ESTIMADO * taxa_selic / 100
    CASE
        WHEN vl_icms_estimado > 0 AND taxa_selic IS NOT NULL
        THEN (vl_icms_estimado * taxa_selic) / 100.00
        ELSE 0.00
    END AS vl_juros,

    -- VL_MULTA (ACL): 20% do ICMS estimado
    CASE
        WHEN vl_icms_estimado > 0
        THEN vl_icms_estimado * 0.2
        ELSE 0.00
    END AS vl_multa,

    -- VL_CT (ACL): ICMS + Juros + Multa
    CASE
        WHEN vl_icms_estimado > 0
        THEN vl_icms_estimado +
             COALESCE((vl_icms_estimado * taxa_selic) / 100.00, 0) +
             (vl_icms_estimado * 0.2)
        ELSE 0.00
    END AS vl_ct

FROM gessimples.bcad_v6_icms_estimado;

COMPUTE STATS gessimples.bcad_v6_icms_com_ct;

-- ============================================================================
-- PARTE 5: CONSOLIDAR VL_CT POR CNPJ_RAIZ (SOMA DE TODOS OS PAs)
-- ============================================================================

DROP TABLE IF EXISTS gessimples.bcad_v6_icms_cobrar;
CREATE TABLE gessimples.bcad_v6_icms_cobrar STORED AS PARQUET AS
SELECT
    cnpj_raiz,
    cpf_socio,
    num_grupo,
    uf,

    SUM(vl_icms_estimado) AS vl_icms_estimado_total,
    SUM(vl_juros) AS vl_juros_total,
    SUM(vl_multa) AS vl_multa_total,
    SUM(vl_ct) AS vl_ct,

    CURRENT_TIMESTAMP() AS dt_carga

FROM gessimples.bcad_v6_icms_com_ct
GROUP BY
    cnpj_raiz,
    cpf_socio,
    num_grupo,
    uf;

COMPUTE STATS gessimples.bcad_v6_icms_cobrar;

-- ============================================================================
-- PARTE 6: OUTPUT FINAL - FORMATO 100% ACL
-- Tabela principal para o Streamlit
-- JOIN entre bcad_13c (resumo) e bcad_14c (detalhe)
-- ============================================================================

DROP TABLE IF EXISTS gessimples.bcad_v6_output_final;
CREATE TABLE gessimples.bcad_v6_output_final STORED AS PARQUET AS
SELECT
    -- Dados do Grupo (da tabela 13c - resumo)
    d.num_grupo,
    COALESCE(g.qtd_empresas_total, g.qtd_empresas_sn + g.qtd_empresas_normal) AS qte_cnpj,
    1 AS qte_socio,  -- Sera recalculado depois se necessario

    -- Valores
    COALESCE(ic.vl_ct, 0.00) AS vl_ct,
    d.receita_bruta_empresa AS vl_rba_pgdas,
    d.receita_global_grupo AS receita_pa_fato,

    -- Dados da Empresa (da tabela 14c - detalhe)
    d.cnpj_completo AS cnpj,
    d.cnpj_raiz,
    d.cpf_socio AS cpf,
    d.uf,

    -- Datas e Qualificacao
    '' AS dt_ini_qualificacao,
    '' AS qualificacao,

    -- Regime
    CASE
        WHEN d.flag_simples_nacional = 1 THEN 'SN'
        ELSE 'NL'
    END AS regime_no_efeito,

    -- FLAG_PERIODO baseado em ano_apuracao
    CASE d.ano_apuracao
        WHEN 2021 THEN '21'
        WHEN 2022 THEN '22'
        WHEN 2023 THEN '23'
        WHEN 2024 THEN '24'
        WHEN 2025 THEN '25'
        ELSE CAST(d.ano_apuracao AS STRING)
    END AS flag_periodo,

    -- Periodos
    CAST(d.periodo_exclusao AS STRING) AS dt_fato,

    -- DT_EFEITO (mes seguinte ao fato)
    CASE
        WHEN d.periodo_exclusao % 100 = 12
        THEN CAST((d.periodo_exclusao / 100 + 1) * 100 + 1 AS STRING)
        ELSE CAST(d.periodo_exclusao + 1 AS STRING)
    END AS dt_efeito,

    CAST(d.periodo_exclusao AS STRING) AS pa_fato_ini,
    '-' AS pa_fato_fin,
    '' AS pa_fin_resp,
    '' AS pa_resp,

    -- Chave unica
    CONCAT(
        LPAD(d.cpf_socio, 11, '0'),
        LPAD(d.cnpj_raiz, 8, '0'),
        CAST(d.periodo_exclusao AS STRING)
    ) AS chave_cpf_raiz_pa,

    -- EMITE_TE_SC
    CASE
        WHEN d.uf = 'SC'
             AND d.flag_simples_nacional = 1
             AND COALESCE(ic.vl_ct, 0) > 0
        THEN 'S'
        ELSE 'N'
    END AS emite_te_sc,

    -- EMITE_TE
    CASE
        WHEN d.flag_simples_nacional = 1
             AND COALESCE(ic.vl_ct, 0) > 0
        THEN 'S'
        ELSE 'N'
    END AS emite_te,

    -- ACAO
    CASE
        WHEN COALESCE(ic.vl_ct, 0) > 0
             AND d.uf = 'SC'
             AND d.flag_simples_nacional = 1
        THEN 'EXCLUSAO_COM_DEBITO'
        WHEN COALESCE(ic.vl_ct, 0) = 0
             AND d.uf = 'SC'
             AND d.flag_simples_nacional = 1
        THEN 'EXCLUSAO_SEM_DEBITO'
        ELSE 'SEM_INTERESSE'
    END AS acao,

    -- ACAO_PRIORIDADE
    CASE
        WHEN COALESCE(ic.vl_ct, 0) > 0
             AND d.uf = 'SC'
             AND d.flag_simples_nacional = 1
        THEN 1  -- EXCLUSAO_COM_DEBITO
        WHEN COALESCE(ic.vl_ct, 0) = 0
             AND d.uf = 'SC'
             AND d.flag_simples_nacional = 1
        THEN 2  -- EXCLUSAO_SEM_DEBITO
        ELSE 3  -- SEM_INTERESSE
    END AS acao_prioridade,

    -- Dados adicionais
    d.razao_social,
    CASE
        WHEN d.flag_simples_nacional = 1 THEN 'ATIVA - SN'
        ELSE 'ATIVA'
    END AS situacao_cadastral,

    -- Tipo de inciso (LC 123/2006)
    d.tipo_inciso,
    d.situacao_limite,
    d.tipo_exclusao,

    -- Ano de apuracao
    d.ano_apuracao,

    -- Receitas detalhadas
    d.receita_bruta_empresa,
    d.receita_global_grupo,
    d.periodos_declarados,

    -- Percentual de participacao
    d.perc_participacao,
    d.flag_maior_10pct,

    -- Fonte dos dados
    d.fonte_dados,

    -- Dados do resumo (tabela 13c)
    g.qtd_empresas_sn,
    g.qtd_empresas_normal,
    g.ufs_empresas,

    CURRENT_TIMESTAMP() AS dt_processamento

FROM gessimples.bcad_14c_violacoes_detalhe_pgdas d
INNER JOIN gessimples.bcad_13c_violacoes_final_pgdas g
    ON d.num_grupo = g.num_grupo
    AND d.cpf_socio = g.cpf_socio
    AND d.ano_apuracao = g.ano_apuracao
LEFT JOIN gessimples.bcad_v6_icms_cobrar ic
    ON d.cnpj_raiz = ic.cnpj_raiz
    AND d.cpf_socio = ic.cpf_socio
    AND d.num_grupo = ic.num_grupo;

COMPUTE STATS gessimples.bcad_v6_output_final;

-- ============================================================================
-- PARTE 7: VIEW RESUMO (para Dashboard Executivo)
-- ============================================================================

DROP VIEW IF EXISTS gessimples.vw_bcad_v6_resumo_dashboard;
CREATE VIEW gessimples.vw_bcad_v6_resumo_dashboard AS
SELECT
    num_grupo,
    cpf,
    qte_cnpj,
    qte_socio,
    uf,
    razao_social,
    situacao_cadastral,
    acao,
    flag_periodo,
    vl_ct,
    receita_pa_fato,
    dt_fato,
    dt_efeito,
    emite_te_sc,
    tipo_inciso,
    situacao_limite,
    tipo_exclusao,
    ano_apuracao
FROM gessimples.bcad_v6_output_final;

-- ============================================================================
-- PARTE 8: ESTATÍSTICAS FINAIS
-- ============================================================================

SELECT
    'ESTATÍSTICAS GERAIS V6' AS categoria,
    COUNT(*) AS total_registros,
    COUNT(DISTINCT num_grupo) AS total_grupos,
    COUNT(DISTINCT cnpj_raiz) AS total_empresas,
    COUNT(DISTINCT cpf) AS total_socios,
    SUM(vl_ct) AS credito_total,
    SUM(CASE WHEN acao = 'EXCLUSAO_COM_DEBITO' THEN 1 ELSE 0 END) AS exclusao_com_debito,
    SUM(CASE WHEN acao = 'EXCLUSAO_SEM_DEBITO' THEN 1 ELSE 0 END) AS exclusao_sem_debito,
    SUM(CASE WHEN acao = 'SEM_INTERESSE' THEN 1 ELSE 0 END) AS sem_interesse
FROM gessimples.bcad_v6_output_final;

-- Por UF
SELECT
    uf,
    COUNT(*) AS qtd,
    COUNT(DISTINCT num_grupo) AS grupos,
    SUM(vl_ct) AS credito_total
FROM gessimples.bcad_v6_output_final
GROUP BY uf
ORDER BY credito_total DESC;

-- Por Ação
SELECT
    acao,
    COUNT(*) AS qtd,
    COUNT(DISTINCT num_grupo) AS grupos,
    SUM(vl_ct) AS credito_total
FROM gessimples.bcad_v6_output_final
GROUP BY acao;

-- Por Tipo de Inciso
SELECT
    tipo_inciso,
    COUNT(*) AS qtd,
    COUNT(DISTINCT num_grupo) AS grupos,
    SUM(vl_ct) AS credito_total,
    AVG(receita_pa_fato) AS receita_media
FROM gessimples.bcad_v6_output_final
GROUP BY tipo_inciso;

-- Top 20 grupos por crédito
SELECT
    num_grupo,
    cpf,
    qte_cnpj,
    uf,
    acao,
    vl_ct,
    receita_pa_fato,
    tipo_inciso
FROM gessimples.bcad_v6_output_final
ORDER BY vl_ct DESC
LIMIT 20;
