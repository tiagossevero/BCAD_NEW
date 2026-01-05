# GENESIS V6 - Sistema de Analise de Grupos Economicos

Sistema para identificacao de empresas e socios que descumprem a LC 123/2006, Art. 3, 4, Incisos III e IV.

## Base Legal

**LC 123/2006, Art. 3, 4 - Nao podera se beneficiar do Simples Nacional:**

- **Inciso III**: pessoa juridica de cujo capital participe pessoa fisica que seja inscrita como empresario ou seja socia de outra empresa SN, desde que a **receita bruta global ultrapasse R$ 4.800.000,00**.

- **Inciso IV**: pessoa juridica cujo titular ou socio participe com **mais de 10%** do capital de outra empresa nao beneficiada pelo SN, desde que a **receita bruta global ultrapasse R$ 4.800.000,00**.

## Estrutura do Projeto

```
BCAD_NEW/
├── BCADASTRO_V6.py              # Dashboard Streamlit V6 (NOVO)
├── BCAD_V6_OUTPUT_FINAL.sql     # SQL para criar tabela output V6 (NOVO)
├── BCAD 2026.json               # Queries SQL V6 originais (PGDAS)
├── BCADASTRO old projeto anterior.json  # Queries SQL antigas (referencia)
├── BCADASTRO (3).py             # Dashboard antigo (depreciado)
└── README.md                    # Esta documentacao
```

## Como Usar

### 1. Executar os SQLs no Impala

**Primeiro**, execute as queries do arquivo `BCAD 2026.json` para criar as tabelas base V6:
- `bcad_01` a `bcad_14c` (pipeline completo)

**Depois**, execute o script `BCAD_V6_OUTPUT_FINAL.sql` para criar:
- Tabela SELIC para calculo de juros
- Calculo de VL_CT (ICMS + Juros + Multa)
- Tabela final `bcad_v6_output_final`

### 2. Configurar Credenciais

Crie o arquivo `.streamlit/secrets.toml`:

```toml
[impala_credentials]
user = "seu_usuario"
password = "sua_senha"
```

### 3. Executar o Dashboard

```bash
streamlit run BCADASTRO_V6.py
```

## Tabelas V6

### Tabela Principal: `gessimples.bcad_v6_output_final`

| Coluna | Descricao |
|--------|-----------|
| num_grupo | Numero do grupo economico |
| qte_cnpj | Quantidade de CNPJs no grupo |
| qte_socio | Quantidade de socios |
| vl_ct | Credito tributario (ICMS + Juros + Multa) |
| receita_pa_fato | Receita no periodo do fato gerador |
| cnpj_raiz | CNPJ raiz (8 digitos) |
| cpf | CPF do socio |
| uf | Unidade federativa |
| acao | EXCLUSAO_COM_DEBITO / EXCLUSAO_SEM_DEBITO / SEM_INTERESSE |
| emite_te_sc | S/N - Emite Termo de Exclusao em SC |
| tipo_inciso | INCISO_III / INCISO_IV / AMBOS |
| situacao_limite | Status do limite (EXCESSO_20PCT, ACIMA_LIMITE, etc.) |
| flag_periodo | Periodo de irregularidade (21, 22, 23, 24, 25) |

## Calculo do Credito Tributario (VL_CT)

```
VL_CT = VL_ICMS_ESTIMADO + VL_JUROS + VL_MULTA

Onde:
- VL_ICMS_ESTIMADO = (VL_ATIV * 0.07) - VL_ICMS_DECLARADO
- VL_JUROS = VL_ICMS_ESTIMADO * TAXA_SELIC / 100
- VL_MULTA = VL_ICMS_ESTIMADO * 0.20 (20%)
```

## Diferencas V6 vs Versao Anterior

| Aspecto | Versao Anterior | V6 |
|---------|-----------------|-----|
| Fonte de Dados | PGDAS + DIME | Apenas PGDAS |
| Incisos | Apenas IV | III e IV |
| Pipeline | 4 scripts | 1 script unificado |
| Tabela Final | bcadastro_output_final_acl | bcad_v6_output_final |

## Contato

Receita Estadual de Santa Catarina
Sistema GENESIS - Grupos Economicos e Simples Nacional
