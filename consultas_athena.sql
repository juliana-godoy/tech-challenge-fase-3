-- Tech Challenge Fase 3 - consultas executivas para Amazon Athena
-- Catálogo: fiap_tech_challenge_fase3
-- Resultados Athena: s3://lab-255375488094/data-output/athena-results/
-- Percentuais usam o denominador elegível já calculado nas tabelas Gold.

USE fiap_tech_challenge_fase3;

-- 0. Inventário do catálogo
SHOW TABLES;

-- 1. KPIs anuais: estrutura, senioridade, trabalho, tecnologias e IA
SELECT
    ano_pesquisa,
    total_respondentes,
    percentual_mulheres,
    percentual_seniores,
    percentual_remoto,
    percentual_sql,
    percentual_python,
    percentual_ia,
    ia_elegiveis_validos,
    ia_respostas_contraditorias
FROM gold_kpis_anuais
ORDER BY ano_pesquisa;

-- 2. Estrutura do mercado por cargo e senioridade
SELECT
    ano_pesquisa,
    cargo_padronizado,
    senioridade,
    respondentes,
    denominador_ano,
    percentual_ano
FROM gold_mercado_profissionais
WHERE respondentes >= 30
ORDER BY ano_pesquisa, respondentes DESC;

-- 3. Perfis valorizados: remuneração aproximada por senioridade
-- A média usa apenas pontos médios de faixas salariais fechadas.
SELECT
    ano_pesquisa,
    dimensao_valor AS senioridade,
    respondentes_ponto_medio AS n,
    salario_medio_aproximado,
    faixa_mediana_ordem
FROM gold_remuneracao
WHERE dimensao_tipo = 'senioridade'
  AND respondentes_ponto_medio >= 30
ORDER BY ano_pesquisa, salario_medio_aproximado DESC;

-- 4. Perfis valorizados: remuneração por cargo em 2024
SELECT
    dimensao_valor AS cargo,
    respondentes_ponto_medio AS n,
    salario_medio_aproximado,
    faixa_mediana_ordem
FROM gold_remuneracao
WHERE ano_pesquisa = 2024
  AND dimensao_tipo = 'cargo'
  AND respondentes_ponto_medio >= 30
ORDER BY salario_medio_aproximado DESC;

-- 5. Diversidade por cargo e senioridade; suprime grupos muito pequenos
SELECT
    ano_pesquisa,
    genero,
    cargo_padronizado,
    senioridade,
    respondentes,
    denominador_segmento,
    percentual_segmento,
    gestores,
    respondentes_com_salario,
    salario_medio_aproximado
FROM gold_diversidade
WHERE respondentes >= 30
ORDER BY ano_pesquisa, cargo_padronizado, senioridade, respondentes DESC;

-- 6. Adoção geral de tecnologias
-- Cloud de 2023 não aparece: naquele ano a pergunta mede preferência, não uso.
SELECT
    ano_pesquisa,
    tecnologia,
    respondentes_elegiveis AS n,
    adotantes,
    percentual_adocao
FROM gold_tecnologias
WHERE segmento_tipo = 'geral'
ORDER BY tecnologia, ano_pesquisa;

-- 7. Tecnologias por cargo em 2024, somente grupos com n >= 30
SELECT
    tecnologia,
    segmento_valor AS cargo,
    respondentes_elegiveis AS n,
    adotantes,
    percentual_adocao
FROM gold_tecnologias
WHERE ano_pesquisa = 2024
  AND segmento_tipo = 'cargo'
  AND respondentes_elegiveis >= 30
ORDER BY tecnologia, percentual_adocao DESC;

-- 8. Adoção profissional de IA generativa (apenas 2023-2024)
SELECT
    ano_pesquisa,
    respondentes_validos AS n,
    adotantes_ia,
    percentual_adocao_ia,
    uso_gratuito,
    uso_pago_proprio,
    uso_pago_empresa,
    uso_copilot
FROM gold_inteligencia_artificial
WHERE segmento_tipo = 'geral'
ORDER BY ano_pesquisa;

-- 9. IA por senioridade; grupos pequenos não são interpretados
SELECT
    ano_pesquisa,
    segmento_valor AS senioridade,
    respondentes_validos AS n,
    percentual_adocao_ia
FROM gold_inteligencia_artificial
WHERE segmento_tipo = 'senioridade'
  AND respondentes_validos >= 30
ORDER BY ano_pesquisa, percentual_adocao_ia DESC;

-- 10. Região x modelo de trabalho x senioridade x remuneração
SELECT
    ano_pesquisa,
    regiao_moradia,
    modelo_trabalho_padronizado,
    senioridade,
    respondentes,
    respondentes_ponto_medio AS n_salario,
    salario_medio_aproximado,
    respondentes_validos_ia AS n_ia,
    percentual_adocao_ia
FROM gold_regiao_trabalho
WHERE respondentes >= 30
ORDER BY ano_pesquisa, regiao_moradia, respondentes DESC;

-- 11. Auditoria: nenhuma linha é descartada na passagem Bronze -> Silver
SELECT
    ano,
    etapa,
    registros_entrada,
    registros_saida,
    registros_descartados,
    duplicidades_excedentes_preservadas,
    ids_nulos,
    regioes_invalidas,
    senioridades_invalidas,
    status
FROM auditoria_pipeline
ORDER BY ano;
