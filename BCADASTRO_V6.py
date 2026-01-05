"""
Sistema GENESIS V6 - Grupos Economicos e Simples Nacional
Receita Estadual de Santa Catarina
Dashboard Streamlit v6.0 - Modernizado para tabelas V6 (PGDAS)
Base Legal: LC 123/2006, Art. 3, 4, Incisos III e IV
"""

# =============================================================================
# 1. IMPORTS E CONFIGURACOES
# =============================================================================

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import warnings
import ssl

# Configuracoes SSL
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

warnings.filterwarnings('ignore')

# Configuracao da pagina
st.set_page_config(
    page_title="GENESIS V6 - Analise de Grupos Economicos",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# 2. AUTENTICACAO
# =============================================================================

SENHA = "tsevero123"  # Altere conforme necessario

def check_password():
    """Sistema de autenticacao."""
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        st.markdown("<div style='text-align: center; padding: 50px;'><h1>Acesso Restrito</h1></div>", unsafe_allow_html=True)

        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            senha_input = st.text_input("Digite a senha:", type="password", key="pwd_input")
            if st.button("Entrar", use_container_width=True):
                if senha_input == SENHA:
                    st.session_state.authenticated = True
                    st.rerun()
                else:
                    st.error("Senha incorreta")
        st.stop()

check_password()

# =============================================================================
# 3. ESTILOS CSS
# =============================================================================

st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .sub-header {
        font-size: 1.8rem;
        font-weight: bold;
        color: #2c3e50;
        margin-top: 2rem;
        margin-bottom: 1rem;
        border-bottom: 3px solid #3498db;
        padding-bottom: 0.5rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 15px;
        color: white;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }

    /* ESTILO DOS KPIs */
    div[data-testid="stMetric"] {
        background-color: #ffffff;
        border: 2px solid #2c3e50;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }

    div[data-testid="stMetric"] > label {
        font-weight: 600;
        color: #2c3e50;
    }

    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;
        font-weight: bold;
        color: #1f77b4;
    }

    .alert-critico {
        background-color: #ffebee;
        border-left: 5px solid #c62828;
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
    }
    .alert-alto {
        background-color: #fff3e0;
        border-left: 5px solid #ef6c00;
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
    }
    .alert-medio {
        background-color: #fff9c4;
        border-left: 5px solid #fbc02d;
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
    }
    .alert-positivo {
        background-color: #e8f5e9;
        border-left: 5px solid #2e7d32;
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
    }
    .info-box {
        background-color: #e3f2fd;
        border-left: 5px solid #1565c0;
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
    }
    .legal-box {
        background-color: #fff8e1;
        border: 2px solid #f57f17;
        padding: 20px;
        border-radius: 8px;
        margin: 20px 0;
    }
    .stDataFrame {
        font-size: 0.9rem;
    }
</style>
""", unsafe_allow_html=True)

# =============================================================================
# 4. CONFIGURACAO DO BANCO DE DADOS
# =============================================================================

IMPALA_HOST = 'bdaworkernode02.sef.sc.gov.br'
IMPALA_PORT = 21050
DATABASE = 'gessimples'

# Tabela principal V6
TABELA_PRINCIPAL = 'bcad_v6_output_final'

# Credenciais (carregadas de forma segura)
IMPALA_USER = st.secrets["impala_credentials"]["user"]
IMPALA_PASSWORD = st.secrets["impala_credentials"]["password"]

@st.cache_resource
def get_impala_engine():
    """Cria e retorna engine Impala (compartilhado entre sessoes)."""
    try:
        engine = create_engine(
            f'impala://{IMPALA_HOST}:{IMPALA_PORT}/{DATABASE}',
            connect_args={
                'user': IMPALA_USER,
                'password': IMPALA_PASSWORD,
                'auth_mechanism': 'LDAP',
                'use_ssl': True
            }
        )
        return engine
    except Exception as e:
        st.error(f"Erro ao criar engine Impala: {e}")
        return None

def testar_conexao(engine):
    """Testa se a conexao esta funcionando."""
    if engine is None:
        return False

    try:
        with engine.connect() as conn:
            pass
        return True
    except Exception as e:
        st.sidebar.error(f"Erro na conexao: {str(e)[:100]}")
        return False

# =============================================================================
# 5. FUNCOES DE CARREGAMENTO DE DADOS - V6
# =============================================================================

@st.cache_data(ttl=3600)
def carregar_resumo_geral(_engine):
    """Carrega estatisticas gerais da tabela V6."""
    try:
        query = f"""
            SELECT
                COUNT(DISTINCT num_grupo) as total_grupos,
                COUNT(DISTINCT cnpj_raiz) as total_empresas,
                COUNT(DISTINCT cpf) as total_socios,
                SUM(CASE WHEN acao = 'EXCLUSAO_COM_DEBITO' THEN 1 ELSE 0 END) as exclusao_com_debito,
                SUM(CASE WHEN acao = 'EXCLUSAO_SEM_DEBITO' THEN 1 ELSE 0 END) as exclusao_sem_debito,
                SUM(CASE WHEN acao = 'SEM_INTERESSE' THEN 1 ELSE 0 END) as sem_interesse,
                SUM(vl_ct) as credito_total,
                AVG(vl_ct) as credito_medio,
                MAX(vl_ct) as credito_maximo,
                SUM(CASE WHEN emite_te_sc = 'S' THEN 1 ELSE 0 END) as emite_te_sc,
                COUNT(DISTINCT CASE WHEN uf = 'SC' THEN cnpj_raiz END) as empresas_sc,
                SUM(receita_pa_fato) as receita_total,
                AVG(receita_pa_fato) as receita_media
            FROM {DATABASE}.{TABELA_PRINCIPAL}
        """
        df = pd.read_sql(query, _engine)
        return df.iloc[0].to_dict()
    except Exception as e:
        st.error(f"Erro ao carregar resumo: {e}")
        return {}

@st.cache_data(ttl=3600)
def carregar_distribuicao_acao(_engine):
    """Carrega distribuicao por acao."""
    try:
        query = f"""
            SELECT
                acao,
                COUNT(DISTINCT num_grupo) as qtd_grupos,
                COUNT(DISTINCT cnpj_raiz) as qtd_empresas,
                SUM(vl_ct) as credito_total,
                AVG(vl_ct) as credito_medio,
                AVG(receita_pa_fato) as receita_media,
                MAX(receita_pa_fato) as receita_maxima
            FROM {DATABASE}.{TABELA_PRINCIPAL}
            GROUP BY acao
            ORDER BY qtd_grupos DESC
        """
        return pd.read_sql(query, _engine)
    except Exception as e:
        st.error(f"Erro ao carregar distribuicao: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def carregar_distribuicao_periodo(_engine):
    """Carrega distribuicao por periodo (FLAG_PERIODO)."""
    try:
        query = f"""
            SELECT
                flag_periodo,
                COUNT(DISTINCT num_grupo) as qtd_grupos,
                COUNT(DISTINCT cnpj_raiz) as qtd_empresas,
                SUM(vl_ct) as credito_total,
                AVG(vl_ct) as credito_medio
            FROM {DATABASE}.{TABELA_PRINCIPAL}
            WHERE flag_periodo IS NOT NULL AND flag_periodo != ''
            GROUP BY flag_periodo
            ORDER BY qtd_grupos DESC
        """
        return pd.read_sql(query, _engine)
    except Exception as e:
        st.error(f"Erro ao carregar periodos: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def carregar_distribuicao_uf(_engine):
    """Carrega distribuicao por UF."""
    try:
        query = f"""
            SELECT
                uf,
                COUNT(DISTINCT num_grupo) as qtd_grupos,
                COUNT(DISTINCT cnpj_raiz) as qtd_empresas,
                SUM(vl_ct) as credito_total,
                AVG(vl_ct) as credito_medio,
                SUM(CASE WHEN emite_te_sc = 'S' THEN 1 ELSE 0 END) as emite_te,
                SUM(CASE WHEN acao = 'EXCLUSAO_COM_DEBITO' THEN 1 ELSE 0 END) as exclusao_debito
            FROM {DATABASE}.{TABELA_PRINCIPAL}
            GROUP BY uf
            ORDER BY qtd_empresas DESC
        """
        return pd.read_sql(query, _engine)
    except Exception as e:
        st.error(f"Erro ao carregar UF: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def carregar_distribuicao_inciso(_engine):
    """Carrega distribuicao por tipo de inciso (III ou IV)."""
    try:
        query = f"""
            SELECT
                tipo_inciso,
                COUNT(DISTINCT num_grupo) as qtd_grupos,
                COUNT(DISTINCT cnpj_raiz) as qtd_empresas,
                COUNT(DISTINCT cpf) as qtd_socios,
                SUM(vl_ct) as credito_total,
                AVG(receita_pa_fato) as receita_media
            FROM {DATABASE}.{TABELA_PRINCIPAL}
            WHERE tipo_inciso IS NOT NULL AND tipo_inciso != ''
            GROUP BY tipo_inciso
            ORDER BY qtd_grupos DESC
        """
        return pd.read_sql(query, _engine)
    except Exception as e:
        st.error(f"Erro ao carregar incisos: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def carregar_top_grupos(_engine, limite=50):
    """Carrega top grupos por credito."""
    query = f"""
    SELECT
        num_grupo,
        cpf,
        qte_cnpj,
        qte_socio,
        SUM(vl_ct) as vl_ct_total,
        MAX(receita_pa_fato) as receita_maxima,
        CASE
            WHEN SUM(CASE WHEN acao = 'EXCLUSAO_COM_DEBITO' THEN 1 ELSE 0 END) > 0
                THEN 'EXCLUSAO_COM_DEBITO'
            WHEN SUM(CASE WHEN acao = 'EXCLUSAO_SEM_DEBITO' THEN 1 ELSE 0 END) > 0
                THEN 'EXCLUSAO_SEM_DEBITO'
            ELSE 'SEM_INTERESSE'
        END as acao_principal,
        MAX(flag_periodo) as periodo_principal,
        COUNT(DISTINCT cnpj_raiz) as empresas_grupo,
        COUNT(DISTINCT CASE WHEN uf = 'SC' THEN cnpj_raiz END) as empresas_sc,
        COUNT(DISTINCT CASE WHEN emite_te_sc = 'S' THEN cnpj_raiz END) as te_emitir,
        MAX(tipo_inciso) as tipo_inciso,
        MAX(situacao_limite) as situacao_limite
    FROM {DATABASE}.{TABELA_PRINCIPAL}
    GROUP BY num_grupo, cpf, qte_cnpj, qte_socio
    ORDER BY vl_ct_total DESC
    LIMIT {limite}
    """
    return pd.read_sql(query, _engine)

@st.cache_data(ttl=3600)
def carregar_lista_grupos(_engine):
    """Carrega lista de grupos para selecao."""
    try:
        query = f"""
            SELECT
                num_grupo,
                cpf,
                qte_cnpj,
                qte_socio,
                MAX(tipo_inciso) as tipo_inciso
            FROM {DATABASE}.{TABELA_PRINCIPAL}
            GROUP BY num_grupo, cpf, qte_cnpj, qte_socio
            ORDER BY num_grupo
        """
        return pd.read_sql(query, _engine)
    except Exception as e:
        st.error(f"Erro ao carregar lista: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def carregar_lista_empresas(_engine):
    """Carrega lista de empresas cadastradas."""
    try:
        query = f"""
            SELECT DISTINCT
                cnpj_raiz,
                razao_social,
                uf,
                situacao_cadastral
            FROM {DATABASE}.{TABELA_PRINCIPAL}
            ORDER BY razao_social
            LIMIT 2000
        """
        return pd.read_sql(query, _engine)
    except Exception as e:
        st.error(f"Erro ao carregar empresas: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=1800)
def carregar_detalhes_grupo(_engine, num_grupo):
    """Carrega todos os detalhes de um grupo especifico."""
    try:
        query = f"""
            SELECT *
            FROM {DATABASE}.{TABELA_PRINCIPAL}
            WHERE num_grupo = {num_grupo}
            ORDER BY vl_ct DESC, uf, razao_social
        """
        df = pd.read_sql(query, _engine)

        if not df.empty:
            # Garantir campos
            if 'vl_ct' not in df.columns:
                df['vl_ct'] = 0
            if 'receita_pa_fato' not in df.columns:
                df['receita_pa_fato'] = 0

            # Ordenar e deduplicar
            df = df.sort_values(
                ['cnpj_raiz', 'vl_ct', 'receita_pa_fato'],
                ascending=[True, False, False]
            )

            qtd_antes = len(df)
            df = df.drop_duplicates(subset=['cnpj_raiz'], keep='first')
            qtd_depois = len(df)

            if qtd_antes != qtd_depois:
                duplicatas = qtd_antes - qtd_depois
                st.info(f"{duplicatas} registros duplicados foram removidos automaticamente.")

            df = df.sort_values('vl_ct', ascending=False)

        return df
    except Exception as e:
        st.error(f"Erro ao carregar grupo: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=1800)
def carregar_detalhes_empresa(_engine, cnpj_raiz):
    """Carrega dados de uma empresa."""
    try:
        query = f"""
            SELECT *
            FROM {DATABASE}.{TABELA_PRINCIPAL}
            WHERE cnpj_raiz = '{cnpj_raiz}'
        """
        return pd.read_sql(query, _engine)
    except Exception as e:
        st.error(f"Erro ao carregar empresa: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=1800)
def carregar_historico_grupo(_engine, cpf):
    """Carrega historico RBA do CPF/grupo usando tabelas V6."""
    try:
        query = f"""
            SELECT
                ano_apuracao as ano,
                COUNT(DISTINCT cnpj_raiz) as qtd_empresas,
                SUM(receita_bruta_empresa) as rba_total,
                AVG(receita_bruta_empresa) as rba_media,
                SUM(vl_ct) as credito_total
            FROM {DATABASE}.{TABELA_PRINCIPAL}
            WHERE cpf = '{cpf}'
            GROUP BY ano_apuracao
            ORDER BY ano_apuracao
        """
        return pd.read_sql(query, _engine)
    except Exception as e:
        st.error(f"Erro ao carregar historico: {e}")
        return pd.DataFrame()

# =============================================================================
# 6. FUNCOES AUXILIARES
# =============================================================================

def formatar_cnpj(cnpj):
    """Formata CNPJ para XX.XXX.XXX."""
    if pd.isna(cnpj):
        return ""
    cnpj = str(cnpj).zfill(8)
    return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}"

def formatar_cpf(cpf):
    """Formata CPF completo."""
    if pd.isna(cpf):
        return ""
    cpf = str(cpf).zfill(11)
    if len(cpf) == 11:
        return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:11]}"
    return cpf

def formatar_moeda(valor):
    """Formata valor monetario."""
    if pd.isna(valor) or valor == 0:
        return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(',', '_').replace('.', ',').replace('_', '.')

def formatar_periodo(periodo):
    """Formata periodo AAAAMM para MM/AAAA."""
    if pd.isna(periodo) or periodo is None:
        return 'N/A'
    periodo_str = str(periodo)
    if len(periodo_str) == 6:
        return f"{periodo_str[4:6]}/{periodo_str[0:4]}"
    return periodo_str

def criar_badge_acao(acao):
    """Cria badge visual para acao."""
    if acao == 'EXCLUSAO_COM_DEBITO':
        return 'Exclusao c/ Debito'
    elif acao == 'EXCLUSAO_SEM_DEBITO':
        return 'Exclusao s/ Debito'
    else:
        return 'Sem Interesse'

def criar_badge_inciso(tipo_inciso):
    """Cria badge para tipo de inciso."""
    if tipo_inciso == 'INCISO_III':
        return 'III - 2+ empresas SN'
    elif tipo_inciso == 'INCISO_IV':
        return 'IV - >10% em nao-SN'
    elif tipo_inciso == 'AMBOS':
        return 'III e IV'
    else:
        return tipo_inciso

def criar_filtros_sidebar():
    """Cria filtros visuais na sidebar."""
    with st.sidebar.expander("Configuracoes Visuais", expanded=False):
        tema = st.selectbox(
            "Tema dos Graficos",
            ["plotly", "plotly_white", "plotly_dark", "seaborn", "ggplot2"],
            index=1,
            key='tema_graficos'
        )
    return {'tema': tema}

# =============================================================================
# 7. PAGINAS DO DASHBOARD
# =============================================================================

def dashboard_executivo(dados, filtros):
    """Dashboard executivo principal."""
    st.markdown("<h1 class='main-header'>Dashboard Executivo GENESIS V6</h1>", unsafe_allow_html=True)

    # Base Legal
    st.markdown("""
    <div class='legal-box'>
        <h3>Base Legal: LC 123/2006, Art. 3, 4</h3>
        <p><strong>Nao podera se beneficiar do Simples Nacional a pessoa juridica:</strong></p>
        <p><strong>III</strong> - de cujo capital participe pessoa fisica que seja inscrita como empresario ou seja socia de outra empresa SN,
        desde que a <strong>receita bruta global ultrapasse R$ 4.800.000,00</strong>.</p>
        <p><strong>IV</strong> - cujo titular ou socio participe com mais de 10% do capital de outra empresa nao beneficiada pelo SN,
        desde que a <strong>receita bruta global ultrapasse R$ 4.800.000,00</strong>.</p>
    </div>
    """, unsafe_allow_html=True)

    resumo = dados.get('resumo_geral', {})

    if not resumo:
        st.warning("Dados nao carregados.")
        return

    # KPIs Principais
    st.markdown("<div class='sub-header'>Indicadores Principais</div>", unsafe_allow_html=True)

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric(
            "Total de Grupos",
            f"{resumo.get('total_grupos', 0):,}",
            help="Grupos economicos identificados com 2+ empresas"
        )

    with col2:
        st.metric(
            "Total de Empresas",
            f"{resumo.get('total_empresas', 0):,}",
            delta=f"SC: {resumo.get('empresas_sc', 0):,}",
            help="Empresas nos grupos identificados"
        )

    with col3:
        st.metric(
            "Total de Socios",
            f"{resumo.get('total_socios', 0):,}",
            help="Socios/titulares unicos identificados"
        )

    with col4:
        credito_total = resumo.get('credito_total', 0)
        st.metric(
            "Credito Total",
            formatar_moeda(credito_total),
            help="Soma de ICMS + Juros + Multa"
        )

    with col5:
        credito_medio = resumo.get('credito_medio', 0)
        st.metric(
            "Credito Medio",
            formatar_moeda(credito_medio),
            help="Valor medio por empresa"
        )

    st.markdown("---")

    # Acoes Fiscais
    st.markdown("<div class='sub-header'>Distribuicao por Acao Fiscal</div>", unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)

    with col1:
        exclusao_debito = resumo.get('exclusao_com_debito', 0)
        st.markdown(f"""
        <div class='alert-critico'>
            <h2 style='color: #c62828; margin: 0;'>{exclusao_debito:,}</h2>
            <p style='margin: 5px 0 0 0;'><strong>Exclusao COM Debito</strong></p>
            <small>Empresas SC com credito tributario</small>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        exclusao_sem = resumo.get('exclusao_sem_debito', 0)
        st.markdown(f"""
        <div class='alert-alto'>
            <h2 style='color: #ef6c00; margin: 0;'>{exclusao_sem:,}</h2>
            <p style='margin: 5px 0 0 0;'><strong>Exclusao SEM Debito</strong></p>
            <small>Empresas SC sem debito apurado</small>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        sem_interesse = resumo.get('sem_interesse', 0)
        st.markdown(f"""
        <div class='alert-positivo'>
            <h2 style='color: #2e7d32; margin: 0;'>{sem_interesse:,}</h2>
            <p style='margin: 5px 0 0 0;'><strong>Sem Interesse</strong></p>
            <small>Fora de SC ou regime encerrado</small>
        </div>
        """, unsafe_allow_html=True)

    # Mais KPIs
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        emite_te = resumo.get('emite_te_sc', 0)
        st.metric(
            "Emissao de TE (SC)",
            f"{emite_te:,}",
            delta=f"{emite_te/max(resumo.get('total_empresas', 1), 1)*100:.1f}%",
            help="Termos de Exclusao a serem emitidos"
        )

    with col2:
        receita_total = resumo.get('receita_total', 0)
        st.metric(
            "Receita Total",
            formatar_moeda(receita_total),
            help="Soma das receitas no fato gerador"
        )

    with col3:
        receita_media = resumo.get('receita_media', 0)
        st.metric(
            "Receita Media",
            formatar_moeda(receita_media),
            help="Receita media por empresa"
        )

    with col4:
        credito_max = resumo.get('credito_maximo', 0)
        st.metric(
            "Credito Maximo",
            formatar_moeda(credito_max),
            help="Maior credito individual"
        )

    st.markdown("---")

    # Graficos
    col1, col2 = st.columns(2)

    with col1:
        df_acao = dados.get('dist_acao', pd.DataFrame())
        if not df_acao.empty:
            fig_acao = px.pie(
                df_acao,
                values='qtd_empresas',
                names='acao',
                title='Distribuicao por Acao Fiscal',
                template=filtros['tema'],
                color='acao',
                color_discrete_map={
                    'EXCLUSAO_COM_DEBITO': '#c62828',
                    'EXCLUSAO_SEM_DEBITO': '#ef6c00',
                    'SEM_INTERESSE': '#2e7d32'
                },
                hole=0.4
            )
            st.plotly_chart(fig_acao, use_container_width=True)

    with col2:
        df_inciso = dados.get('dist_inciso', pd.DataFrame())
        if not df_inciso.empty:
            fig_inciso = px.bar(
                df_inciso,
                x='tipo_inciso',
                y='qtd_grupos',
                title='Distribuicao por Tipo de Inciso (LC 123)',
                template=filtros['tema'],
                text='qtd_grupos',
                color='credito_total',
                color_continuous_scale='Reds'
            )
            fig_inciso.update_traces(textposition='outside')
            st.plotly_chart(fig_inciso, use_container_width=True)

    # Distribuicao Geografica
    st.markdown("<div class='sub-header'>Distribuicao Geografica</div>", unsafe_allow_html=True)

    df_uf = dados.get('dist_uf', pd.DataFrame())
    if not df_uf.empty:
        col1, col2 = st.columns(2)

        with col1:
            df_uf_top = df_uf.head(15)
            fig_uf_empresas = px.bar(
                df_uf_top,
                x='uf',
                y='qtd_empresas',
                title='Estados por Quantidade de Empresas',
                template=filtros['tema'],
                text='qtd_empresas',
                color='qtd_empresas',
                color_continuous_scale='Blues'
            )
            fig_uf_empresas.update_traces(textposition='outside')
            st.plotly_chart(fig_uf_empresas, use_container_width=True)

        with col2:
            df_uf_credito = df_uf[df_uf['credito_total'] > 0].head(15)
            fig_uf_credito = px.bar(
                df_uf_credito,
                x='uf',
                y='credito_total',
                title='Estados por Credito Tributario',
                template=filtros['tema'],
                text='credito_total',
                color='credito_total',
                color_continuous_scale='Reds'
            )
            fig_uf_credito.update_traces(textposition='outside', texttemplate='R$ %{text:,.0f}')
            st.plotly_chart(fig_uf_credito, use_container_width=True)

    # Periodos
    st.markdown("<div class='sub-header'>Distribuicao por Periodo</div>", unsafe_allow_html=True)

    df_periodo = dados.get('dist_periodo', pd.DataFrame())
    if not df_periodo.empty:
        fig_periodo = px.bar(
            df_periodo.head(10),
            x='flag_periodo',
            y='qtd_grupos',
            title='Top 10 Periodos com Irregularidades',
            template=filtros['tema'],
            text='qtd_grupos',
            color='credito_total',
            color_continuous_scale='Oranges'
        )
        fig_periodo.update_traces(textposition='outside')
        st.plotly_chart(fig_periodo, use_container_width=True)


def ranking_grupos(dados, filtros):
    """Ranking de grupos por credito tributario."""
    st.markdown("<h1 class='main-header'>Ranking de Grupos Economicos</h1>", unsafe_allow_html=True)

    st.markdown("""
    <div class='info-box'>
        <strong>Sobre este Ranking:</strong><br>
        Lista os grupos economicos ordenados por credito tributario (VL_CT = ICMS + Juros + Multa).<br>
        Classificacao por tipo de inciso conforme LC 123/2006.
    </div>
    """, unsafe_allow_html=True)

    # Configuracoes
    st.subheader("Configuracoes do Ranking")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        top_n = st.slider("Top N grupos", 10, 100, 50, 5, key='top_n_ranking')

    with col2:
        filtro_acao = st.multiselect(
            "Filtrar por Acao",
            ['EXCLUSAO_COM_DEBITO', 'EXCLUSAO_SEM_DEBITO', 'SEM_INTERESSE'],
            default=['EXCLUSAO_COM_DEBITO'],
            key='filtro_acao_rank'
        )

    with col3:
        min_credito = st.number_input(
            "Credito Minimo (R$)",
            min_value=0,
            value=0,
            step=1000,
            key='min_credito'
        )

    with col4:
        min_empresas = st.number_input(
            "Min. Empresas",
            min_value=2,
            value=2,
            step=1,
            key='min_empresas'
        )

    df_top = dados.get('top_grupos', pd.DataFrame())

    if df_top.empty:
        st.warning("Dados nao carregados.")
        return

    # Filtrar
    if min_credito > 0:
        df_top = df_top[df_top['vl_ct_total'] >= min_credito]

    if min_empresas > 2:
        df_top = df_top[df_top['qte_cnpj'] >= min_empresas]

    if filtro_acao:
        df_top = df_top[df_top['acao_principal'].isin(filtro_acao)]

    df_top = df_top.head(top_n)

    # Formatar para exibicao
    df_display = df_top.copy()
    df_display['posicao'] = range(1, len(df_display) + 1)
    df_display['cpf_formatado'] = df_display['cpf'].apply(formatar_cpf)
    df_display['vl_ct_formatado'] = df_display['vl_ct_total'].apply(formatar_moeda)
    df_display['receita_formatada'] = df_display['receita_maxima'].apply(formatar_moeda)
    df_display['acao_badge'] = df_display['acao_principal'].apply(criar_badge_acao)
    df_display['inciso_badge'] = df_display['tipo_inciso'].apply(criar_badge_inciso)

    # Estatisticas do filtro
    st.markdown("---")
    st.subheader("Estatisticas do Filtro")

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric("Grupos", len(df_display))

    with col2:
        st.metric("Empresas", df_display['empresas_grupo'].sum())

    with col3:
        st.metric("Empresas SC", df_display['empresas_sc'].sum())

    with col4:
        st.metric("Credito Total", formatar_moeda(df_display['vl_ct_total'].sum()))

    with col5:
        st.metric("Media/Grupo", formatar_moeda(df_display['vl_ct_total'].mean()))

    st.markdown("---")

    # Tabela principal
    st.subheader(f"Top {len(df_display)} Grupos por Credito Tributario")

    st.dataframe(
        df_display[[
            'posicao', 'num_grupo', 'cpf_formatado', 'qte_cnpj', 'qte_socio',
            'empresas_grupo', 'empresas_sc', 'te_emitir',
            'vl_ct_formatado', 'receita_formatada',
            'acao_badge', 'inciso_badge', 'periodo_principal'
        ]].rename(columns={
            'posicao': '#',
            'num_grupo': 'Grupo',
            'cpf_formatado': 'CPF Socio',
            'qte_cnpj': 'CNPJs',
            'qte_socio': 'Socios',
            'empresas_grupo': 'Empresas',
            'empresas_sc': 'SC',
            'te_emitir': 'TEs',
            'vl_ct_formatado': 'Credito Total',
            'receita_formatada': 'Receita Maxima',
            'acao_badge': 'Acao',
            'inciso_badge': 'Inciso',
            'periodo_principal': 'Periodo'
        }),
        use_container_width=True,
        height=600
    )

    # Graficos
    st.markdown("---")
    st.subheader("Visualizacoes")

    col1, col2 = st.columns(2)

    with col1:
        df_top_20 = df_display.head(20)

        fig_credito = go.Figure()

        fig_credito.add_trace(go.Bar(
            x=df_top_20['vl_ct_total'],
            y=df_top_20['num_grupo'].astype(str),
            orientation='h',
            text=df_top_20['vl_ct_total'].apply(lambda x: formatar_moeda(x)),
            textposition='outside',
            marker_color='#c62828',
            hovertemplate='<b>Grupo %{y}</b><br>Credito: %{text}<extra></extra>'
        ))

        fig_credito.update_layout(
            title='Top 20 Grupos por Credito Tributario',
            xaxis_title='Credito Tributario (R$)',
            yaxis_title='Numero do Grupo',
            template=filtros['tema'],
            height=600
        )

        st.plotly_chart(fig_credito, use_container_width=True)

    with col2:
        fig_scatter = px.scatter(
            df_display,
            x='empresas_grupo',
            y='vl_ct_total',
            size='receita_maxima',
            color='acao_principal',
            hover_name='num_grupo',
            title='Credito x Quantidade de Empresas',
            template=filtros['tema'],
            color_discrete_map={
                'EXCLUSAO_COM_DEBITO': '#c62828',
                'EXCLUSAO_SEM_DEBITO': '#ef6c00',
                'SEM_INTERESSE': '#2e7d32'
            },
            labels={
                'empresas_grupo': 'Quantidade de Empresas',
                'vl_ct_total': 'Credito Tributario (R$)',
                'acao_principal': 'Acao'
            }
        )

        fig_scatter.update_layout(height=600)
        st.plotly_chart(fig_scatter, use_container_width=True)


def analise_detalhada_grupo(dados, filtros, engine):
    """Analise detalhada de um grupo especifico."""
    st.markdown("<h1 class='main-header'>Analise Detalhada - Grupo Economico</h1>", unsafe_allow_html=True)

    lista_grupos = dados.get('lista_grupos', pd.DataFrame())

    if lista_grupos.empty:
        st.warning("Lista de grupos nao carregada.")
        return

    # Selecao do grupo
    st.subheader("Selecao do Grupo")

    col1, col2 = st.columns([3, 1])

    with col1:
        busca_grupo = st.text_input(
            "Buscar por Numero ou CPF",
            placeholder="Digite o numero do grupo ou CPF do socio...",
            key='busca_grupo'
        )

        if busca_grupo:
            lista_filtrada = lista_grupos[
                (lista_grupos['num_grupo'].astype(str).str.contains(busca_grupo, na=False)) |
                (lista_grupos['cpf'].astype(str).str.contains(busca_grupo.replace('.', '').replace('-', ''), na=False))
            ]
        else:
            lista_filtrada = lista_grupos

        num_grupo_selecionado = st.selectbox(
            "Selecione o grupo:",
            lista_filtrada['num_grupo'].tolist(),
            format_func=lambda x: f"Grupo {x} - {lista_filtrada[lista_filtrada['num_grupo']==x]['qte_cnpj'].iloc[0]} empresas",
            key='select_grupo_detalhes'
        )

    with col2:
        st.metric("Grupos Disponiveis", len(lista_filtrada))

    if not num_grupo_selecionado:
        st.info("Selecione um grupo para analise.")
        return

    if st.button("Carregar Analise Completa", type="primary", use_container_width=True):
        st.session_state.analise_carregada = True
        st.session_state.num_grupo_atual = num_grupo_selecionado

    if st.session_state.get('analise_carregada', False) and st.session_state.get('num_grupo_atual') == num_grupo_selecionado:
        with st.spinner(f'Carregando dados do Grupo {num_grupo_selecionado}...'):
            df_grupo = carregar_detalhes_grupo(engine, num_grupo_selecionado)

        if df_grupo.empty:
            st.error("Grupo nao encontrado.")
            st.session_state.analise_carregada = False
            return

        # Cabecalho
        grupo_info = df_grupo.iloc[0]
        st.markdown(f"### Grupo Economico #{num_grupo_selecionado}")
        st.caption(f"CPF Socio: {formatar_cpf(grupo_info['cpf'])} | Empresas: {grupo_info['qte_cnpj']} | Tipo: {criar_badge_inciso(grupo_info.get('tipo_inciso', 'N/A'))}")

        # KPIs do Grupo
        st.markdown("<div class='sub-header'>Indicadores do Grupo</div>", unsafe_allow_html=True)

        col1, col2, col3, col4, col5, col6 = st.columns(6)

        with col1:
            st.metric("Empresas", len(df_grupo))

        with col2:
            empresas_sc = df_grupo[df_grupo['uf'] == 'SC'].shape[0]
            st.metric("Empresas SC", empresas_sc)

        with col3:
            credito_total = df_grupo['vl_ct'].sum()
            st.metric("Credito Total", formatar_moeda(credito_total))

        with col4:
            receita_max = df_grupo['receita_pa_fato'].max()
            st.metric("Receita Maxima", formatar_moeda(receita_max))

        with col5:
            emite_te = df_grupo[df_grupo['emite_te_sc'] == 'S'].shape[0]
            st.metric("Emite TE", emite_te)

        with col6:
            ufs_distintas = df_grupo['uf'].nunique()
            st.metric("Estados", ufs_distintas)

        # Alertas
        st.markdown("---")

        if credito_total > 0:
            st.markdown(f"""
            <div class='alert-critico'>
                <strong>ALERTA DE DEBITO FISCAL</strong><br>
                Este grupo possui credito tributario de <strong>{formatar_moeda(credito_total)}</strong>.<br>
                Empresas com debito: {df_grupo[df_grupo['vl_ct'] > 0].shape[0]}<br>
                Empresas SC com TE: {emite_te}<br>
                Recomenda-se acao fiscal imediata
            </div>
            """, unsafe_allow_html=True)

        if receita_max > 4800000:
            excedente = receita_max - 4800000
            percentual = (excedente / 4800000) * 100
            st.markdown(f"""
            <div class='alert-alto'>
                <strong>ULTRAPASSAGEM DO LIMITE SIMPLES NACIONAL</strong><br>
                Receita maxima apurada: <strong>{formatar_moeda(receita_max)}</strong><br>
                Limite SN: <strong>R$ 4.800.000,00</strong><br>
                Excedente: <strong>{formatar_moeda(excedente)}</strong> ({percentual:.1f}% acima)
            </div>
            """, unsafe_allow_html=True)

        # Empresas do Grupo
        st.markdown("<div class='sub-header'>Empresas do Grupo</div>", unsafe_allow_html=True)

        df_empresas = df_grupo.copy()
        df_empresas['cnpj_formatado'] = df_empresas['cnpj_raiz'].apply(formatar_cnpj)
        df_empresas['vl_ct_formatado'] = df_empresas['vl_ct'].apply(formatar_moeda)
        df_empresas['receita_formatada'] = df_empresas['receita_pa_fato'].apply(formatar_moeda)
        df_empresas['acao_badge'] = df_empresas['acao'].apply(criar_badge_acao)

        st.dataframe(
            df_empresas[[
                'cnpj_formatado', 'razao_social', 'uf', 'situacao_cadastral',
                'acao_badge', 'vl_ct_formatado', 'receita_formatada',
                'flag_periodo', 'emite_te_sc', 'tipo_inciso'
            ]].rename(columns={
                'cnpj_formatado': 'CNPJ',
                'razao_social': 'Razao Social',
                'uf': 'UF',
                'situacao_cadastral': 'Situacao',
                'acao_badge': 'Acao',
                'vl_ct_formatado': 'Credito',
                'receita_formatada': 'Receita',
                'flag_periodo': 'Periodo',
                'emite_te_sc': 'TE-SC',
                'tipo_inciso': 'Inciso'
            }),
            use_container_width=True,
            height=500
        )

        # Download
        csv = df_empresas.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            "Download CSV",
            csv,
            f"grupo_{num_grupo_selecionado}_empresas.csv",
            "text/csv",
            key='download_grupo'
        )


def analise_detalhada_empresa(dados, filtros, engine):
    """Analise detalhada de uma empresa especifica."""
    st.markdown("<h1 class='main-header'>Analise Detalhada - Empresa</h1>", unsafe_allow_html=True)

    lista_empresas = dados.get('lista_empresas', pd.DataFrame())

    if lista_empresas.empty:
        st.warning("Lista de empresas nao carregada.")
        return

    # Selecao da empresa
    st.subheader("Selecao da Empresa")

    col1, col2 = st.columns([2, 1])

    with col1:
        busca = st.text_input(
            "Buscar por Razao Social ou CNPJ",
            placeholder="Digite parte do nome ou CNPJ...",
            key='busca_empresa'
        )

    with col2:
        filtro_uf = st.selectbox(
            "Estado",
            ['Todos'] + sorted(lista_empresas['uf'].unique().tolist()),
            key='filtro_uf_empresa'
        )

    # Filtrar lista
    lista_filtrada = lista_empresas.copy()

    if busca:
        mascara_razao = lista_filtrada['razao_social'].str.contains(busca, case=False, na=False)
        mascara_cnpj = lista_filtrada['cnpj_raiz'].astype(str).str.contains(busca.replace('.', '').replace('/', '').replace('-', ''), na=False)
        lista_filtrada = lista_filtrada[mascara_razao | mascara_cnpj]

    if filtro_uf != 'Todos':
        lista_filtrada = lista_filtrada[lista_filtrada['uf'] == filtro_uf]

    if len(lista_filtrada) > 1000:
        st.warning(f"{len(lista_filtrada):,} empresas encontradas. Mostrando apenas as primeiras 1.000.")
        lista_filtrada = lista_filtrada.head(1000)

    if lista_filtrada.empty:
        st.info("Nenhuma empresa encontrada com os filtros aplicados.")
        return

    st.caption(f"{len(lista_filtrada):,} empresas disponiveis")

    empresa_dict = dict(zip(lista_filtrada['cnpj_raiz'], lista_filtrada['razao_social']))

    cnpj_selecionado = st.selectbox(
        "Selecione a empresa:",
        lista_filtrada['cnpj_raiz'].tolist(),
        format_func=lambda x: f"{formatar_cnpj(x)} - {empresa_dict.get(x, 'N/A')}",
        key='select_empresa_drill'
    )

    if not cnpj_selecionado:
        st.info("Selecione uma empresa para analise.")
        return

    if st.button("Carregar Analise Completa", type="primary", use_container_width=True):
        with st.spinner(f'Carregando dados da empresa {formatar_cnpj(cnpj_selecionado)}...'):
            df_empresa = carregar_detalhes_empresa(engine, cnpj_selecionado)

        if df_empresa.empty:
            st.error("Empresa nao encontrada.")
            return

        empresa = df_empresa.iloc[0]

        # Cabecalho
        st.markdown(f"### {empresa['razao_social']}")
        st.caption(f"CNPJ: {formatar_cnpj(empresa['cnpj_raiz'])} | UF: {empresa['uf']} | Situacao: {empresa['situacao_cadastral']}")

        # KPIs da Empresa
        st.markdown("<div class='sub-header'>Dados da Empresa</div>", unsafe_allow_html=True)

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Grupo", empresa.get('num_grupo', 'N/A'))

        with col2:
            st.metric("Credito", formatar_moeda(empresa.get('vl_ct', 0)))

        with col3:
            st.metric("Receita", formatar_moeda(empresa.get('receita_pa_fato', 0)))

        with col4:
            st.metric("Periodo", empresa.get('flag_periodo', 'N/A'))

        # Status
        st.markdown("---")

        acao = empresa.get('acao', '')
        if acao == 'EXCLUSAO_COM_DEBITO':
            st.markdown(f"""
            <div class='alert-critico'>
                <strong>EMPRESA IDENTIFICADA EM GRUPO IRREGULAR</strong><br><br>
                Grupo: {empresa['num_grupo']}<br>
                Acao: {empresa['acao']}<br>
                Tipo Inciso: {criar_badge_inciso(empresa.get('tipo_inciso', 'N/A'))}<br>
                Credito Tributario: {formatar_moeda(empresa['vl_ct'])}<br>
                Receita (Fato): {formatar_moeda(empresa['receita_pa_fato'])}<br>
                Data Fato Gerador: {formatar_periodo(empresa.get('dt_fato', 'N/A'))}<br>
                Periodo: {empresa['flag_periodo']}<br>
                Emite TE-SC: {empresa['emite_te_sc']}
            </div>
            """, unsafe_allow_html=True)
        elif acao == 'EXCLUSAO_SEM_DEBITO':
            st.markdown(f"""
            <div class='alert-alto'>
                <strong>EMPRESA EM GRUPO IRREGULAR - SEM DEBITO</strong><br><br>
                Grupo: {empresa['num_grupo']}<br>
                Acao: {empresa['acao']}<br>
                Tipo Inciso: {criar_badge_inciso(empresa.get('tipo_inciso', 'N/A'))}<br>
                Receita (Fato): {formatar_moeda(empresa['receita_pa_fato'])}
            </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class='alert-positivo'>
                <strong>SEM INTERESSE FISCAL</strong><br>
                Esta empresa esta fora de SC ou nao atende aos criterios de acao.
            </div>
            """, unsafe_allow_html=True)


def relatorio_executivo(dados, filtros):
    """Relatorio executivo para exportacao."""
    st.markdown("<h1 class='main-header'>Relatorio Executivo</h1>", unsafe_allow_html=True)

    st.markdown("""
    <div class='info-box'>
        <strong>Sobre este Relatorio:</strong><br>
        Este modulo gera um relatorio consolidado com os principais achados da analise,
        incluindo estatisticas, tabelas e recomendacoes para acao fiscal.
    </div>
    """, unsafe_allow_html=True)

    resumo = dados.get('resumo_geral', {})
    df_acao = dados.get('dist_acao', pd.DataFrame())
    df_uf = dados.get('dist_uf', pd.DataFrame())
    df_top = dados.get('top_grupos', pd.DataFrame())
    df_inciso = dados.get('dist_inciso', pd.DataFrame())

    # Sumario Executivo
    st.markdown("<div class='sub-header'>Sumario Executivo</div>", unsafe_allow_html=True)

    data_relatorio = datetime.now().strftime('%d/%m/%Y %H:%M')

    st.markdown(f"""
    ### Sistema GENESIS V6 - Analise de Grupos Economicos
    **Data do Relatorio:** {data_relatorio}

    #### Resumo Geral

    O Sistema GENESIS V6 identificou **{resumo.get('total_grupos', 0):,} grupos economicos** formados por
    **{resumo.get('total_socios', 0):,} socios/titulares** que controlam **{resumo.get('total_empresas', 0):,} empresas**.

    #### Base Legal

    Conforme Lei Complementar 123/2006, Art. 3, 4:

    - **Inciso III**: Socios de 2+ empresas no Simples Nacional com receita global > R$ 4.800.000,00
    - **Inciso IV**: Socios com >10% em empresa nao-SN com receita global > R$ 4.800.000,00

    #### Principais Indicadores

    - **Total de Empresas:** {resumo.get('total_empresas', 0):,}
    - **Empresas em SC:** {resumo.get('empresas_sc', 0):,}
    - **Total de Grupos:** {resumo.get('total_grupos', 0):,}
    - **Socios/Titulares:** {resumo.get('total_socios', 0):,}
    - **Credito Tributario Total:** {formatar_moeda(resumo.get('credito_total', 0))}
    - **Credito Medio por Empresa:** {formatar_moeda(resumo.get('credito_medio', 0))}
    - **Receita Total (Fato Gerador):** {formatar_moeda(resumo.get('receita_total', 0))}

    #### Distribuicao por Acao Fiscal

    - **Exclusao COM Debito:** {resumo.get('exclusao_com_debito', 0):,} empresas
    - **Exclusao SEM Debito:** {resumo.get('exclusao_sem_debito', 0):,} empresas
    - **Sem Interesse:** {resumo.get('sem_interesse', 0):,} empresas

    #### Termos de Exclusao

    - **Empresas SC com TE a Emitir:** {resumo.get('emite_te_sc', 0):,}
    """)

    # Distribuicao por Inciso
    if not df_inciso.empty:
        st.markdown("---")
        st.markdown("<div class='sub-header'>Distribuicao por Tipo de Inciso</div>", unsafe_allow_html=True)

        st.dataframe(
            df_inciso.rename(columns={
                'tipo_inciso': 'Tipo Inciso',
                'qtd_grupos': 'Grupos',
                'qtd_empresas': 'Empresas',
                'qtd_socios': 'Socios',
                'credito_total': 'Credito Total',
                'receita_media': 'Receita Media'
            }),
            use_container_width=True
        )

    # Top 50 Grupos
    if not df_top.empty:
        st.markdown("---")
        st.markdown("<div class='sub-header'>Top 50 Grupos Prioritarios para Fiscalizacao</div>", unsafe_allow_html=True)

        df_top_50 = df_top.head(50).copy()
        df_top_50['ranking'] = range(1, len(df_top_50) + 1)
        df_top_50['cpf_formatado'] = df_top_50['cpf'].apply(formatar_cpf)
        df_top_50['credito_formatado'] = df_top_50['vl_ct_total'].apply(formatar_moeda)
        df_top_50['receita_formatada'] = df_top_50['receita_maxima'].apply(formatar_moeda)

        st.dataframe(
            df_top_50[[
                'ranking', 'num_grupo', 'cpf_formatado', 'qte_cnpj',
                'empresas_grupo', 'empresas_sc',
                'credito_formatado', 'receita_formatada',
                'tipo_inciso', 'periodo_principal'
            ]].rename(columns={
                'ranking': '#',
                'num_grupo': 'Grupo',
                'cpf_formatado': 'CPF Socio',
                'qte_cnpj': 'CNPJs',
                'empresas_grupo': 'Empresas',
                'empresas_sc': 'SC',
                'credito_formatado': 'Credito Total',
                'receita_formatada': 'Receita Maxima',
                'tipo_inciso': 'Inciso',
                'periodo_principal': 'Periodo'
            }),
            use_container_width=True,
            height=600
        )

        # Download do Top 50
        csv = df_top_50.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            "Download Top 50 (CSV)",
            csv,
            "genesis_v6_top50_grupos.csv",
            "text/csv",
            key='download_top50'
        )


# =============================================================================
# 8. FUNCAO PRINCIPAL
# =============================================================================

def main():
    # Sidebar
    st.sidebar.title("Sistema GENESIS V6")
    st.sidebar.caption("Grupos Economicos e Simples Nacional")
    st.sidebar.markdown("---")

    # Conectar ao banco
    engine = get_impala_engine()

    if engine is None:
        st.error("Falha na conexao com o banco de dados.")
        st.info("Verifique suas credenciais em `.streamlit/secrets.toml`")
        return

    # Testar conexao
    st.sidebar.write("Testando conexao...")
    if not testar_conexao(engine):
        st.error("Nao foi possivel conectar ao banco de dados Impala.")
        st.info(f"Certifique-se de que a tabela {DATABASE}.{TABELA_PRINCIPAL} existe.")
        st.info("Execute o script BCAD_V6_OUTPUT_FINAL.sql antes de usar este dashboard.")
        return

    st.sidebar.success("Conexao estabelecida!")

    # Menu de navegacao
    st.sidebar.subheader("Navegacao")

    paginas = [
        "Dashboard Executivo",
        "Ranking de Grupos",
        "Analise de Grupo",
        "Analise de Empresa",
        "Relatorio Executivo"
    ]

    pagina_selecionada = st.sidebar.radio(
        "Selecione uma pagina",
        paginas,
        label_visibility="collapsed"
    )

    # Carregar dados agregados
    with st.spinner('Carregando dados do sistema...'):
        dados = {
            'resumo_geral': carregar_resumo_geral(engine),
            'dist_acao': carregar_distribuicao_acao(engine),
            'dist_periodo': carregar_distribuicao_periodo(engine),
            'dist_uf': carregar_distribuicao_uf(engine),
            'dist_inciso': carregar_distribuicao_inciso(engine),
            'top_grupos': carregar_top_grupos(engine, 100),
            'lista_grupos': carregar_lista_grupos(engine),
            'lista_empresas': carregar_lista_empresas(engine)
        }

    # Info na sidebar
    resumo = dados.get('resumo_geral', {})
    if resumo:
        st.sidebar.markdown("---")
        st.sidebar.markdown("### Indicadores")
        st.sidebar.metric("Grupos", f"{resumo.get('total_grupos', 0):,}")
        st.sidebar.metric("Empresas", f"{resumo.get('total_empresas', 0):,}")
        st.sidebar.metric("Credito", formatar_moeda(resumo.get('credito_total', 0)))
        st.sidebar.metric("TEs a Emitir", f"{resumo.get('emite_te_sc', 0):,}")

    # Filtros visuais
    filtros = criar_filtros_sidebar()

    # Botao de limpar cache
    st.sidebar.markdown("---")
    if st.sidebar.button("Limpar Cache"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.sidebar.success("Cache limpo!")
        st.rerun()

    # Informacoes do sistema
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Informacoes")
    st.sidebar.caption(f"Versao: 6.0")
    st.sidebar.caption(f"Database: {DATABASE}")
    st.sidebar.caption(f"Tabela: {TABELA_PRINCIPAL}")
    st.sidebar.caption(f"Atualizado: {datetime.now().strftime('%d/%m/%Y')}")

    # Roteamento
    try:
        if pagina_selecionada == "Dashboard Executivo":
            dashboard_executivo(dados, filtros)
        elif pagina_selecionada == "Ranking de Grupos":
            ranking_grupos(dados, filtros)
        elif pagina_selecionada == "Analise de Grupo":
            analise_detalhada_grupo(dados, filtros, engine)
        elif pagina_selecionada == "Analise de Empresa":
            analise_detalhada_empresa(dados, filtros, engine)
        elif pagina_selecionada == "Relatorio Executivo":
            relatorio_executivo(dados, filtros)
    except Exception as e:
        st.error(f"Erro ao carregar pagina: {str(e)}")
        st.exception(e)

    # Rodape
    st.markdown("---")
    st.markdown(
        f"<div style='text-align: center; color: #666;'>"
        f"Sistema GENESIS V6 | Receita Estadual de SC<br>"
        f"Base Legal: LC 123/2006, Art. 3, 4, III e IV | "
        f"Ultima atualizacao: {datetime.now().strftime('%d/%m/%Y %H:%M')}"
        f"</div>",
        unsafe_allow_html=True
    )

# =============================================================================
# 9. EXECUCAO
# =============================================================================

if __name__ == "__main__":
    main()
