# GENESIS V6 - Sistema de Análise de Grupos Econômicos

Sistema de análise de conformidade tributária desenvolvido pela Receita Estadual de Santa Catarina para identificação de empresas e sócios que violam a **Lei Complementar 123/2006** (Lei do Simples Nacional).

## Índice

- [Sobre o Projeto](#sobre-o-projeto)
- [Base Legal](#base-legal)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Tecnologias Utilizadas](#tecnologias-utilizadas)
- [Instalação](#instalação)
- [Funcionalidades](#funcionalidades)
- [Esquema do Banco de Dados](#esquema-do-banco-de-dados)
- [Cálculo do Crédito Tributário](#cálculo-do-crédito-tributário)
- [Classificações](#classificações)
- [Configuração](#configuração)
- [Segurança](#segurança)
- [Diferenças V6 vs Versão Anterior](#diferenças-v6-vs-versão-anterior)
- [Contribuição](#contribuição)
- [Licença](#licença)
- [Contato](#contato)

## Sobre o Projeto

O **GENESIS V6** é um sistema de análise de grupos econômicos que identifica violações nos requisitos do Simples Nacional, calculando o crédito tributário devido (ICMS + Juros + Multa) e gerando relatórios para ações fiscais.

### Principais Objetivos

- Identificar grupos econômicos com receita acima do limite legal (R$ 4.800.000,00)
- Calcular o crédito tributário (VL_CT) composto por ICMS estimado, juros (SELIC) e multa
- Gerar relatórios executivos para tomada de decisão fiscal
- Acompanhar a evolução dos grupos ao longo dos períodos fiscais

## Base Legal

Este sistema implementa a **Lei Complementar 123/2006**, Artigo 3º, § 4º:

> **Não poderá se beneficiar do tratamento jurídico diferenciado previsto nesta Lei Complementar:**

### Inciso III
Pessoa jurídica de cujo capital participe pessoa física que seja inscrita como empresário ou seja sócia de outra empresa que receba tratamento jurídico diferenciado nos termos desta Lei Complementar, desde que a **receita bruta global ultrapasse o limite de R$ 4.800.000,00**.

### Inciso IV
Pessoa jurídica cujo titular ou sócio participe com **mais de 10%** do capital de outra empresa não beneficiada por esta Lei Complementar, desde que a **receita bruta global ultrapasse o limite de R$ 4.800.000,00**.

## Estrutura do Projeto

```
BCAD_NEW/
├── README.md                              # Documentação do projeto
├── BCADASTRO_V6.py                       # Dashboard principal Streamlit (1.488 linhas)
├── BCAD_V6_OUTPUT_FINAL.sql              # Pipeline SQL para tabela de saída (407 linhas)
├── BCAD 2026.json                        # Queries SQL para tabelas bcad_01 a bcad_14c
├── BCADASTRO (3).py                      # Dashboard antigo (depreciado)
└── BCADASTRO old projeto anterior.json   # Queries legado (referência)
```

### Descrição dos Arquivos

| Arquivo | Descrição |
|---------|-----------|
| `BCADASTRO_V6.py` | Dashboard interativo Streamlit com 5 páginas de análise |
| `BCAD_V6_OUTPUT_FINAL.sql` | Script SQL que cria tabelas SELIC, calcula VL_CT e gera tabela final |
| `BCAD 2026.json` | Queries SQL para criação das tabelas intermediárias (bcad_01 a bcad_14c) |

## Tecnologias Utilizadas

### Frontend
| Tecnologia | Versão | Descrição |
|------------|--------|-----------|
| Streamlit | - | Framework Python para dashboards interativos |
| Plotly | - | Visualizações interativas (Express & Graph Objects) |
| Pandas | - | Processamento e manipulação de dados |
| NumPy | - | Operações numéricas |

### Backend/Banco de Dados
| Tecnologia | Descrição |
|------------|-----------|
| Apache Impala | Engine SQL para big data |
| SQLAlchemy | ORM Python para conexão com banco de dados |
| impyla | Driver Python para Impala |
| LDAP | Mecanismo de autenticação com SSL/TLS |

### Fontes de Dados
| Fonte | Descrição |
|-------|-----------|
| PGDAS-D | Sistema de Apuração do Simples Nacional |
| BCADASTRO | Cadastro Brasileiro de Empresas e Sócios |
| Índice SELIC | Banco Central do Brasil (cálculo de juros) |
| Cadastros SC | Registros estaduais (vw_cad_vinculo, vw_ods_contrib) |

## Instalação

### Pré-requisitos

- Python 3.8+
- Acesso ao banco de dados Impala (`bdaworkernode02.sef.sc.gov.br:21050`)
- Credenciais LDAP para autenticação no Impala

### Passo 1: Clonar o Repositório

```bash
git clone https://github.com/tiagossevero/BCAD_NEW.git
cd BCAD_NEW
```

### Passo 2: Instalar Dependências

```bash
pip install streamlit pandas numpy plotly sqlalchemy impyla
```

### Passo 3: Executar Pipeline SQL

Execute as queries do arquivo `BCAD 2026.json` para criar as tabelas base (`bcad_01` a `bcad_14c`):

```bash
# Executar queries individualmente ou via script
impala-shell -q "QUERY_DO_ARQUIVO_JSON"
```

Depois execute o pipeline principal:

```bash
impala-shell -f BCAD_V6_OUTPUT_FINAL.sql
```

### Passo 4: Configurar Credenciais

Crie o diretório e arquivo de configuração:

```bash
mkdir -p .streamlit
```

Crie o arquivo `.streamlit/secrets.toml`:

```toml
[impala_credentials]
user = "seu_usuario"
password = "sua_senha"
```

### Passo 5: Executar o Dashboard

```bash
streamlit run BCADASTRO_V6.py
```

O dashboard estará disponível em: `http://localhost:8501`

## Funcionalidades

O sistema possui **5 páginas principais**:

### 1. Dashboard Executivo 📊

Visão geral com indicadores-chave:

- **KPIs Principais**
  - Total de grupos econômicos
  - Total de empresas
  - Total de sócios
  - Crédito tributário total

- **Distribuições**
  - Por ação fiscal (Exclusão com/sem débito, Sem interesse)
  - Por UF (distribuição geográfica)
  - Por período (2021-2025)
  - Por tipo de inciso (III, IV, Ambos)

### 2. Ranking de Grupos 🏆

Análise comparativa dos grupos:

- Top 50 grupos por crédito tributário
- Filtros avançados:
  - Crédito mínimo
  - Quantidade mínima de empresas
  - Tipo de ação fiscal
- Estatísticas agregadas
- Gráficos de barras e dispersão
- **Download em CSV**

### 3. Análise de Grupo 🔍

Detalhamento por grupo econômico:

- Busca por número do grupo ou CPF
- Indicadores detalhados do grupo
- Lista completa de empresas do grupo
- Informações por empresa:
  - CNPJ e razão social
  - Receita bruta
  - Região/UF
  - Regime tributário

### 4. Análise de Empresa 🏢

Informações detalhadas por empresa:

- Busca e filtro de empresas
- Dados cadastrais completos
- Lista de sócios associados
- Histórico de receita
- Situação no Simples Nacional

### 5. Relatório Executivo 📋

Relatórios para tomada de decisão:

- Top 50 grupos consolidados
- Informações detalhadas por grupo
- **Download em CSV** para análises externas

## Esquema do Banco de Dados

### Tabela Principal: `gessimples.bcad_v6_output_final`

| Coluna | Descrição | Tipo |
|--------|-----------|------|
| `num_grupo` | ID do grupo econômico | INT |
| `qte_cnpj` | Quantidade de empresas no grupo | INT |
| `qte_socio` | Quantidade de sócios | INT |
| `vl_ct` | Crédito tributário (ICMS + Juros + Multa) | DOUBLE |
| `receita_pa_fato` | Receita no período do fato gerador | DOUBLE |
| `cnpj_raiz` | CNPJ raiz (8 dígitos) | STRING |
| `cnpj` | CNPJ completo | STRING |
| `cpf` | CPF do sócio | STRING |
| `uf` | Estado | STRING |
| `acao` | Ação fiscal | STRING |
| `emite_te_sc` | Emite Termo de Exclusão em SC | STRING (S/N) |
| `tipo_inciso` | Tipo de inciso (III/IV/AMBOS) | STRING |
| `situacao_limite` | Situação do limite | STRING |
| `flag_periodo` | Ano (21/22/23/24/25) | STRING |
| `razao_social` | Nome da empresa | STRING |
| `regime_no_efeito` | Regime (SN/NL) | STRING |

### Tabelas de Suporte

| Tabela | Descrição |
|--------|-----------|
| `bcad_v6_selic` | Índice SELIC para cálculo de juros |
| `bcad_v6_pgdas_consolidado` | Dados consolidados do PGDAS |
| `bcad_v6_icms_estimado` | Cálculo de ICMS estimado |
| `bcad_v6_icms_com_ct` | ICMS com juros e multas |
| `bcad_v6_icms_cobrar` | Total VL_CT por empresa/sócio |
| `vw_bcad_v6_resumo_dashboard` | View resumida para dashboard |

### Pipeline de Tabelas Intermediárias

```
bcad_01 → bcad_02 → ... → bcad_14c → bcad_v6_output_final
```

## Cálculo do Crédito Tributário

O **VL_CT** (Valor do Crédito Tributário) é calculado pela seguinte fórmula:

```
VL_CT = VL_ICMS_ESTIMADO + VL_JUROS + VL_MULTA
```

### Componentes

| Componente | Fórmula | Descrição |
|------------|---------|-----------|
| `VL_ICMS_ESTIMADO` | `(VL_ATIV × 0.07) - VL_ICMS_DECLARADO` | 7% da atividade menos ICMS declarado |
| `VL_JUROS` | `VL_ICMS_ESTIMADO × TAXA_SELIC / 100` | Juros baseados na taxa SELIC |
| `VL_MULTA` | `VL_ICMS_ESTIMADO × 0.20` | Multa de 20% sobre ICMS estimado |

### Exemplo de Cálculo

```
VL_ATIV = R$ 100.000,00
VL_ICMS_DECLARADO = R$ 2.000,00
TAXA_SELIC = 50%

VL_ICMS_ESTIMADO = (100.000 × 0.07) - 2.000 = R$ 5.000,00
VL_JUROS = 5.000 × 50 / 100 = R$ 2.500,00
VL_MULTA = 5.000 × 0.20 = R$ 1.000,00

VL_CT = 5.000 + 2.500 + 1.000 = R$ 8.500,00
```

## Classificações

### Ações Fiscais (ACAO)

| Valor | Descrição | Critério |
|-------|-----------|----------|
| `EXCLUSAO_COM_DEBITO` | Exclusão com débito tributário | VL_CT > 0 e residente em SC |
| `EXCLUSAO_SEM_DEBITO` | Exclusão sem débito tributário | VL_CT = 0 e residente em SC |
| `SEM_INTERESSE` | Sem interesse fiscal | Fora da jurisdição de SC |

### Situação do Limite

| Valor | Descrição | Faixa de Receita |
|-------|-----------|------------------|
| `EXCESSO_20PCT` | Excede 20% acima do limite | > R$ 5.760.000,00 |
| `ACIMA_LIMITE` | Acima do limite básico | R$ 4.800.000,01 a R$ 5.760.000,00 |
| `DENTRO_LIMITE` | Dentro dos limites | ≤ R$ 4.800.000,00 |

### Tipo de Inciso

| Valor | Descrição | Situação |
|-------|-----------|----------|
| `INCISO_III` | Múltiplas empresas SN | Sócio em 2+ empresas optantes pelo Simples Nacional |
| `INCISO_IV` | SN + participação em não-SN | Sócio com >10% em empresa não optante |
| `AMBOS` | Viola os dois incisos | Ambas condições violadas simultaneamente |

## Configuração

### Configuração do Streamlit

```python
# Configurações da página
st.set_page_config(
    page_title="GENESIS V6 - Analise de Grupos Economicos",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)
```

### Parâmetros de Conexão

```python
IMPALA_HOST = 'bdaworkernode02.sef.sc.gov.br'
IMPALA_PORT = 21050
DATABASE = 'gessimples'
TABELA_PRINCIPAL = 'bcad_v6_output_final'
```

### Cache de Dados

O sistema utiliza cache do Streamlit para otimizar performance:

```python
@st.cache_data(ttl=3600)  # Cache de 1 hora
def carregar_dados():
    ...
```

## Segurança

### Autenticação

- **Dashboard**: Acesso protegido por senha (autenticação baseada em sessão)
- **Banco de Dados**: Autenticação LDAP com SSL/TLS

### Armazenamento de Credenciais

As credenciais são armazenadas de forma segura em `.streamlit/secrets.toml`:

```toml
[impala_credentials]
user = "seu_usuario"
password = "sua_senha"
```

> **Importante**: O arquivo `secrets.toml` deve ser adicionado ao `.gitignore` para não ser versionado.

### Conexão Segura

```python
# Conexão com SSL/TLS
engine = create_engine(
    f"impala://{host}:{port}/{database}",
    connect_args={
        "auth_mechanism": "LDAP",
        "use_ssl": True
    }
)
```

## Diferenças V6 vs Versão Anterior

| Aspecto | Versão Anterior | V6 |
|---------|-----------------|-----|
| Fonte de Dados | PGDAS + DIME | Apenas PGDAS |
| Incisos | Apenas IV | III e IV |
| Pipeline | 4 scripts | 1 script unificado |
| Tabela Final | `bcadastro_output_final_acl` | `bcad_v6_output_final` |
| Dashboard | Básico | 5 páginas completas |
| Cálculo VL_CT | Simplificado | ICMS + Juros + Multa |

### Melhorias na V6

- ✅ Análise de ambos os incisos (III e IV)
- ✅ Cálculo completo de crédito tributário com juros SELIC
- ✅ Dashboard executivo com KPIs
- ✅ Ranking de grupos econômicos
- ✅ Análise detalhada por grupo e empresa
- ✅ Exportação de relatórios em CSV
- ✅ Cache de dados para melhor performance

## Contribuição

1. Faça um fork do projeto
2. Crie uma branch para sua feature:
   ```bash
   git checkout -b feature/nova-feature
   ```
3. Commit suas mudanças:
   ```bash
   git commit -m 'Adiciona nova feature'
   ```
4. Push para a branch:
   ```bash
   git push origin feature/nova-feature
   ```
5. Abra um Pull Request

### Padrões de Código

- Utilize nomes de variáveis em português (alinhado com o domínio tributário)
- Documente funções com docstrings
- Siga o padrão PEP 8 para Python
- Teste localmente antes de submeter PR

## Licença

Este projeto é de uso interno da **Secretaria de Estado da Fazenda de Santa Catarina (SEF/SC)**.

## Contato

**Receita Estadual de Santa Catarina**
Sistema GENESIS - Grupos Econômicos e Simples Nacional

---

**Versão**: 6.0
**Última atualização**: Janeiro 2026
