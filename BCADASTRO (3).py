"""
Sistema GENESIS - Grupos Econômicos e Simples Nacional
Receita Estadual de Santa Catarina
Dashboard Streamlit v2.0 - COMPLETO
Baseado nos scripts SQL: bcadastro_* (ACL Format)
"""

# =============================================================================
# 1. IMPORTS E CONFIGURAÇÕES
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

# Configurações SSL
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

warnings.filterwarnings('ignore')

# Configuração da página
st.set_page_config(
    page_title="GENESIS - Análise de Grupos Econômicos",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# 2. AUTENTICAÇÃO
# =============================================================================

SENHA = "tsevero123"  # Altere conforme necessário

def check_password():
    """Sistema de autenticação."""
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    
    if not st.session_state.authenticated:
        st.markdown("<div style='text-align: center; padding: 50px;'><h1>🔐 Acesso Restrito</h1></div>", unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            senha_input = st.text_input("Digite a senha:", type="password", key="pwd_input")
            if st.button("Entrar", use_container_width=True):
                if senha_input == SENHA:
                    st.session_state.authenticated = True
                    st.rerun()
                else:
                    st.error("❌ Senha incorreta")
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
    
    /* ESTILO DOS KPIs - BORDA PRETA */
    div[data-testid="stMetric"] {
        background-color: #ffffff;        /* Fundo branco */
        border: 2px solid #2c3e50;        /* Borda: 2px de largura, sólida, cor cinza-escuro */
        border-radius: 10px;              /* Cantos arredondados (10 pixels de raio) */
        padding: 15px;                    /* Espaçamento interno (15px em todos os lados) */
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);  /* Sombra: horizontal=0, vertical=2px, blur=4px, cor preta 10% opacidade */
    }
    
    /* Título do métrica */
    div[data-testid="stMetric"] > label {
        font-weight: 600;                 /* Negrito médio */
        color: #2c3e50;                   /* Cor do texto */
    }
    
    /* Valor do métrica */
    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;                /* Tamanho da fonte do valor */
        font-weight: bold;                /* Negrito */
        color: #1f77b4;                   /* Cor azul */
    }
    
    /* Delta (variação) */
    div[data-testid="stMetricDelta"] {
        font-size: 0.9rem;                /* Tamanho menor para delta */
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
# 4. CONFIGURAÇÃO DO BANCO DE DADOS
# =============================================================================

IMPALA_HOST = 'bdaworkernode02.sef.sc.gov.br'
IMPALA_PORT = 21050
DATABASE = 'gessimples'

# Credenciais (carregadas de forma segura)
IMPALA_USER = st.secrets["impala_credentials"]["user"]
IMPALA_PASSWORD = st.secrets["impala_credentials"]["password"]

@st.cache_resource
def get_impala_engine():
    """Cria e retorna engine Impala (compartilhado entre sessões)."""
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
        st.error(f"❌ Erro ao criar engine Impala: {e}")
        return None

def testar_conexao(engine):
    """Testa se a conexão está funcionando."""
    if engine is None:
        return False
    
    try:
        with engine.connect() as conn:
            result = conn.execute(f"SELECT COUNT(*) FROM {DATABASE}.bcadastro_output_final_acl LIMIT 1")
            return True
    except Exception as e:
        st.sidebar.error(f"❌ Erro na conexão: {str(e)[:100]}")
        return False

# =============================================================================
# 5. FUNÇÕES DE CARREGAMENTO DE DADOS - RESUMOS AGREGADOS
# =============================================================================

@st.cache_data(ttl=3600)
def carregar_resumo_geral(_engine):
    """Carrega estatísticas gerais da tabela final."""
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
            FROM {DATABASE}.bcadastro_output_final_acl
        """
        df = pd.read_sql(query, _engine)
        return df.iloc[0].to_dict()
    except Exception as e:
        st.error(f"Erro ao carregar resumo: {e}")
        import traceback
        st.text(traceback.format_exc()[:500])
        return {}

@st.cache_data(ttl=3600)
def carregar_distribuicao_acao(_engine):
    """Carrega distribuição por ação."""
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
            FROM {DATABASE}.bcadastro_output_final_acl
            GROUP BY acao
            ORDER BY qtd_grupos DESC
        """
        return pd.read_sql(query, _engine)
    except Exception as e:
        st.error(f"Erro ao carregar distribuição: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def carregar_distribuicao_periodo(_engine):
    """Carrega distribuição por período (FLAG_PERIODO)."""
    try:
        query = f"""
            SELECT 
                flag_periodo,
                COUNT(DISTINCT num_grupo) as qtd_grupos,
                COUNT(DISTINCT cnpj_raiz) as qtd_empresas,
                SUM(vl_ct) as credito_total,
                AVG(vl_ct) as credito_medio
            FROM {DATABASE}.bcadastro_output_final_acl
            WHERE flag_periodo IS NOT NULL AND flag_periodo != ''
            GROUP BY flag_periodo
            ORDER BY qtd_grupos DESC
        """
        return pd.read_sql(query, _engine)
    except Exception as e:
        st.error(f"Erro ao carregar períodos: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def carregar_distribuicao_uf(_engine):
    """Carrega distribuição por UF."""
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
            FROM {DATABASE}.bcadastro_output_final_acl
            GROUP BY uf
            ORDER BY qtd_empresas DESC
        """
        return pd.read_sql(query, _engine)
    except Exception as e:
        st.error(f"Erro ao carregar UF: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def carregar_distribuicao_qualificacao(_engine):
    """Carrega distribuição por qualificação do sócio."""
    try:
        query = f"""
            SELECT 
                qualificacao,
                COUNT(DISTINCT num_grupo) as qtd_grupos,
                COUNT(DISTINCT cnpj_raiz) as qtd_empresas,
                COUNT(DISTINCT cpf) as qtd_socios,
                SUM(vl_ct) as credito_total
            FROM {DATABASE}.bcadastro_output_final_acl
            WHERE qualificacao IS NOT NULL AND qualificacao != ''
            GROUP BY qualificacao
            ORDER BY qtd_grupos DESC
            LIMIT 15
        """
        return pd.read_sql(query, _engine)
    except Exception as e:
        st.error(f"Erro ao carregar qualificações: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def carregar_top_grupos(_engine, limite=50):
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
        COUNT(DISTINCT CASE WHEN emite_te_sc = 'S' THEN cnpj_raiz END) as te_emitir
    FROM gessimples.bcadastro_output_final_acl
    WHERE vl_ct > 0
    GROUP BY num_grupo, cpf, qte_cnpj, qte_socio
    ORDER BY vl_ct_total DESC
    LIMIT {limite}
    """
    return pd.read_sql(query, _engine)

@st.cache_data(ttl=3600)
def carregar_lista_grupos(_engine):
    """Carrega lista de grupos para seleção."""
    try:
        query = f"""
            SELECT DISTINCT
                num_grupo,
                cpf,
                qte_cnpj,
                qte_socio
            FROM {DATABASE}.bcadastro_output_final_acl
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
                situacao_cadastral_desc
            FROM {DATABASE}.bcadastro_base_cnpj_completo
            WHERE situacao_cadastral_desc = 'ATIVA'
            ORDER BY razao_social
            LIMIT 2000
        """
        return pd.read_sql(query, _engine)
    except Exception as e:
        st.error(f"Erro ao carregar empresas: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def carregar_estatisticas_cadastrais(_engine):
    """Carrega estatísticas da base cadastral."""
    try:
        query = f"""
            SELECT 
                COUNT(DISTINCT cnpj_raiz) as total_cnpj,
                COUNT(DISTINCT CASE WHEN uf = 'SC' THEN cnpj_raiz END) as cnpj_sc,
                COUNT(DISTINCT CASE WHEN situacao_cadastral_desc = 'ATIVA' THEN cnpj_raiz END) as cnpj_ativo,
                SUM(CAST(capital_social AS DOUBLE)) as capital_total,
                AVG(CAST(capital_social AS DOUBLE)) as capital_medio,
                COUNT(DISTINCT porte_empresa) as portes_distintos
            FROM {DATABASE}.bcadastro_base_cnpj_completo
        """
        df = pd.read_sql(query, _engine)
        return df.iloc[0].to_dict()
    except Exception as e:
        st.error(f"Erro ao carregar estatísticas: {e}")
        return {}

@st.cache_data(ttl=3600)
def carregar_distribuicao_porte(_engine):
    """Carrega distribuição por porte de empresa."""
    try:
        query = f"""
            SELECT 
                porte_empresa,
                COUNT(DISTINCT cnpj_raiz) as qtd_empresas,
                AVG(CAST(capital_social AS DOUBLE)) as capital_medio
            FROM {DATABASE}.bcadastro_base_cnpj_completo
            WHERE porte_empresa IS NOT NULL
            GROUP BY porte_empresa
            ORDER BY qtd_empresas DESC
        """
        return pd.read_sql(query, _engine)
    except Exception as e:
        st.error(f"Erro ao carregar porte: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def carregar_distribuicao_natureza(_engine):
    """Carrega distribuição por natureza jurídica."""
    try:
        query = f"""
            SELECT 
                natureza_juridica_desc,
                COUNT(DISTINCT cnpj_raiz) as qtd_empresas
            FROM {DATABASE}.bcadastro_base_cnpj_completo
            WHERE natureza_juridica_desc IS NOT NULL
            GROUP BY natureza_juridica_desc
            ORDER BY qtd_empresas DESC
            LIMIT 15
        """
        return pd.read_sql(query, _engine)
    except Exception as e:
        st.error(f"Erro ao carregar natureza: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def carregar_estatisticas_socios(_engine):
    """Carrega estatísticas de sócios."""
    try:
        query = f"""
            SELECT 
                COUNT(DISTINCT cpf) as total_socios,
                COUNT(DISTINCT cnpj_raiz) as empresas_com_socios,
                COUNT(*) as total_vinculos,
                COUNT(DISTINCT CASE WHEN uf = 'SC' THEN cnpj_raiz END) as vinculos_sc
            FROM {DATABASE}.bcadastro_base_socios_consolidado
        """
        df = pd.read_sql(query, _engine)
        return df.iloc[0].to_dict()
    except Exception as e:
        st.error(f"Erro ao carregar sócios: {e}")
        return {}

# =============================================================================
# 6. FUNÇÕES DE CARREGAMENTO - DETALHAMENTO (SOB DEMANDA)
# =============================================================================

@st.cache_data(ttl=1800)
def carregar_detalhes_grupo(_engine, num_grupo):
    """Carrega todos os detalhes de um grupo específico."""
    try:
        query = f"""
            SELECT *
            FROM {DATABASE}.bcadastro_output_final_acl
            WHERE num_grupo = {num_grupo}
            ORDER BY vl_ct DESC, uf, razao_social
        """
        df = pd.read_sql(query, _engine)
        
        # DEDUPLICAÇÃO ROBUSTA: Manter apenas 1 registro por CNPJ
        # Critério: Maior VL_CT, depois maior RECEITA_PA_FATO, depois DT_FATO mais recente
        if not df.empty:
            # Garantir que campos de ordenação existem
            if 'vl_ct' not in df.columns:
                df['vl_ct'] = 0
            if 'receita_pa_fato' not in df.columns:
                df['receita_pa_fato'] = 0
            if 'dt_fato' not in df.columns:
                df['dt_fato'] = ''
            
            # Ordenar e deduplica
            df = df.sort_values(
                ['cnpj_raiz', 'vl_ct', 'receita_pa_fato', 'dt_fato'], 
                ascending=[True, False, False, False]
            )
            
            qtd_antes = len(df)
            df = df.drop_duplicates(subset=['cnpj_raiz'], keep='first')
            qtd_depois = len(df)
            
            # Log de deduplicação (apenas em dev)
            if qtd_antes != qtd_depois:
                duplicatas = qtd_antes - qtd_depois
                st.info(f"ℹ️ {duplicatas} registros duplicados foram removidos automaticamente.")
            
            # Reordenar por crédito
            df = df.sort_values('vl_ct', ascending=False)
        
        return df
    except Exception as e:
        st.error(f"Erro ao carregar grupo: {e}")
        import traceback
        st.text(traceback.format_exc()[:500])
        return pd.DataFrame()

@st.cache_data(ttl=1800)
def carregar_detalhes_empresa(_engine, cnpj_raiz):
    """Carrega dados cadastrais completos da empresa."""
    try:
        query = f"""
            SELECT *
            FROM {DATABASE}.bcadastro_base_cnpj_completo
            WHERE cnpj_raiz = '{cnpj_raiz}'
        """
        df = pd.read_sql(query, _engine)
        
        # Converter colunas de data para string de forma segura
        colunas_data = ['dt_sit_cadastral', 'dt_ini_ativ', 'dt_ini_responsavel']
        for col in colunas_data:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: formatar_data(x) if pd.notna(x) else 'N/A')
        
        return df
    except Exception as e:
        st.error(f"Erro ao carregar empresa: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=1800)
def carregar_socios_empresa(_engine, cnpj_raiz):
    """Carrega sócios de uma empresa."""
    try:
        query = f"""
            SELECT *
            FROM {DATABASE}.bcadastro_base_socios_consolidado
            WHERE cnpj_raiz = '{cnpj_raiz}'
            ORDER BY socio_ou_titular DESC
        """
        return pd.read_sql(query, _engine)
    except Exception as e:
        st.error(f"Erro ao carregar sócios: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=1800)
def carregar_historico_grupo(_engine, cpf):
    """Carrega histórico RBA do CPF/grupo."""
    try:
        query = f"""
            SELECT 
                pa,
                COUNT(DISTINCT cnpj_raiz) as qtd_empresas,
                SUM(vl_rba_pgdas) as rba_total,
                AVG(vl_rba_pgdas) as rba_media,
                SUM(vl_icms_12m) as icms_total
            FROM {DATABASE}.bcadastro_tab_raiz_cpf_pai
            WHERE cpf = '{cpf}'
            GROUP BY pa
            ORDER BY pa
        """
        return pd.read_sql(query, _engine)
    except Exception as e:
        st.error(f"Erro ao carregar histórico: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=1800)
def carregar_pgdas_empresa(_engine, cnpj_raiz, periodo_inicio='202101', periodo_fim='202512'):
    """Carrega PGDAS de uma empresa."""
    try:
        query = f"""
            SELECT *
            FROM {DATABASE}.bcadastro_pgdas_consolidado
            WHERE cnpj_raiz = '{cnpj_raiz}'
              AND periodo_apuracao >= {periodo_inicio}
              AND periodo_apuracao <= {periodo_fim}
            ORDER BY periodo_apuracao DESC
        """
        return pd.read_sql(query, _engine)
    except Exception as e:
        st.error(f"Erro ao carregar PGDAS: {e}")
        return pd.DataFrame()

# =============================================================================
# 7. FUNÇÕES AUXILIARES
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
    """Formata valor monetário."""
    if pd.isna(valor) or valor == 0:
        return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(',', '_').replace('.', ',').replace('_', '.')

def formatar_data(data):
    """Formata data para string de forma segura."""
    if pd.isna(data) or data is None or data == 'N/A':
        return 'N/A'
    
    try:
        if isinstance(data, str):
            if len(data) == 8:  # YYYYMMDD
                return f"{data[6:8]}/{data[4:6]}/{data[0:4]}"
            elif len(data) == 10 and '-' in data:  # YYYY-MM-DD
                return datetime.strptime(data, '%Y-%m-%d').strftime('%d/%m/%Y')
            return data
        elif isinstance(data, (datetime, pd.Timestamp)):
            return data.strftime('%d/%m/%Y')
        elif hasattr(data, 'strftime'):
            return data.strftime('%d/%m/%Y')
        else:
            return str(data)
    except:
        return str(data) if data else 'N/A'

def formatar_periodo(periodo):
    """Formata período AAAAMM para MM/AAAA."""
    if pd.isna(periodo) or periodo is None:
        return 'N/A'
    periodo_str = str(periodo)
    if len(periodo_str) == 6:
        return f"{periodo_str[4:6]}/{periodo_str[0:4]}"
    return periodo_str

def criar_badge_acao(acao):
    """Cria badge visual para ação."""
    if acao == 'EXCLUSAO_COM_DEBITO':
        return '🔴 Exclusão c/ Débito'
    elif acao == 'EXCLUSAO_SEM_DEBITO':
        return '🟡 Exclusão s/ Débito'
    else:
        return '🟢 Sem Interesse'

def criar_filtros_sidebar():
    """Cria filtros visuais na sidebar."""
    with st.sidebar.expander("🎨 Configurações Visuais", expanded=False):
        tema = st.selectbox(
            "Tema dos Gráficos",
            ["plotly", "plotly_white", "plotly_dark", "seaborn", "ggplot2"],
            index=1,
            key='tema_graficos'
        )
    return {'tema': tema}

# =============================================================================
# 8. PÁGINAS DO DASHBOARD
# =============================================================================

def dashboard_executivo(dados, filtros):
    """Dashboard executivo principal."""
    st.markdown("<h1 class='main-header'>🏢 Dashboard Executivo GENESIS v2.0</h1>", unsafe_allow_html=True)
    
    # Base Legal
    st.markdown("""
    <div class='legal-box'>
        <h3>⚖️ Base Legal: LC 123/2006, Art. 3º, § 4º, Inciso IV</h3>
        <p><strong>Não poderá se beneficiar do Simples Nacional a pessoa jurídica:</strong></p>
        <p>IV - cujo titular ou sócio participe com mais de 10% do capital de outra empresa não beneficiada 
        por esta Lei Complementar, desde que a <strong>receita bruta global ultrapasse R$ 4.800.000,00</strong>.</p>
    </div>
    """, unsafe_allow_html=True)
    
    resumo = dados.get('resumo_geral', {})
    
    if not resumo:
        st.warning("⚠️ Dados não carregados.")
        return
    
    # KPIs Principais
    st.markdown("<div class='sub-header'>📊 Indicadores Principais</div>", unsafe_allow_html=True)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            "Total de Grupos",
            f"{resumo.get('total_grupos', 0):,}",
            help="Grupos econômicos identificados com 2+ empresas"
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
            "Total de Sócios",
            f"{resumo.get('total_socios', 0):,}",
            help="Sócios/titulares únicos identificados"
        )
    
    with col4:
        credito_total = resumo.get('credito_total', 0)
        st.metric(
            "Crédito Total",
            formatar_moeda(credito_total),
            help="Soma de ICMS + Juros + Multa"
        )
    
    with col5:
        credito_medio = resumo.get('credito_medio', 0)
        st.metric(
            "Crédito Médio",
            formatar_moeda(credito_medio),
            help="Valor médio por empresa"
        )
    
    st.markdown("---")
    
    # Ações Fiscais
    st.markdown("<div class='sub-header'>🎯 Distribuição por Ação Fiscal</div>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        exclusao_debito = resumo.get('exclusao_com_debito', 0)
        st.markdown(f"""
        <div class='alert-critico'>
            <h2 style='color: #c62828; margin: 0;'>{exclusao_debito:,}</h2>
            <p style='margin: 5px 0 0 0;'><strong>Exclusão COM Débito</strong></p>
            <small>Empresas SC com crédito tributário</small>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        exclusao_sem = resumo.get('exclusao_sem_debito', 0)
        st.markdown(f"""
        <div class='alert-alto'>
            <h2 style='color: #ef6c00; margin: 0;'>{exclusao_sem:,}</h2>
            <p style='margin: 5px 0 0 0;'><strong>Exclusão SEM Débito</strong></p>
            <small>Empresas SC sem débito apurado</small>
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
            "📄 Emissão de TE (SC)",
            f"{emite_te:,}",
            delta=f"{emite_te/max(resumo.get('total_empresas', 1), 1)*100:.1f}%",
            help="Termos de Exclusão a serem emitidos"
        )
    
    with col2:
        receita_total = resumo.get('receita_total', 0)
        st.metric(
            "💰 Receita Total",
            formatar_moeda(receita_total),
            help="Soma das receitas no fato gerador"
        )
    
    with col3:
        receita_media = resumo.get('receita_media', 0)
        st.metric(
            "📊 Receita Média",
            formatar_moeda(receita_media),
            help="Receita média por empresa"
        )
    
    with col4:
        credito_max = resumo.get('credito_maximo', 0)
        st.metric(
            "🔝 Crédito Máximo",
            formatar_moeda(credito_max),
            help="Maior crédito individual"
        )
    
    st.markdown("---")
    
    # Gráficos
    col1, col2 = st.columns(2)
    
    with col1:
        df_acao = dados.get('dist_acao', pd.DataFrame())
        if not df_acao.empty:
            fig_acao = px.pie(
                df_acao,
                values='qtd_empresas',
                names='acao',
                title='Distribuição por Ação Fiscal',
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
        df_periodo = dados.get('dist_periodo', pd.DataFrame())
        if not df_periodo.empty:
            df_top_periodo = df_periodo.head(10)
            fig_periodo = px.bar(
                df_top_periodo,
                x='qtd_grupos',
                y='flag_periodo',
                orientation='h',
                title='Top 10 Períodos com Irregularidades',
                template=filtros['tema'],
                text='qtd_grupos',
                color='credito_total',
                color_continuous_scale='Reds'
            )
            fig_periodo.update_traces(textposition='outside')
            st.plotly_chart(fig_periodo, use_container_width=True)
    
    # Distribuição Geográfica
    st.markdown("<div class='sub-header'>🗺️ Distribuição Geográfica</div>", unsafe_allow_html=True)
    
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
                title='Estados por Crédito Tributário',
                template=filtros['tema'],
                text='credito_total',
                color='credito_total',
                color_continuous_scale='Reds'
            )
            fig_uf_credito.update_traces(textposition='outside', texttemplate='R$ %{text:,.0f}')
            st.plotly_chart(fig_uf_credito, use_container_width=True)
    
    # Qualificações dos Sócios
    st.markdown("<div class='sub-header'>👥 Qualificações dos Sócios</div>", unsafe_allow_html=True)
    
    df_qualif = dados.get('dist_qualificacao', pd.DataFrame())
    if not df_qualif.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            fig_qualif = px.bar(
                df_qualif.head(10),
                x='qtd_grupos',
                y='qualificacao',
                orientation='h',
                title='Top 10 Qualificações por Grupos',
                template=filtros['tema'],
                text='qtd_grupos'
            )
            fig_qualif.update_traces(textposition='outside')
            st.plotly_chart(fig_qualif, use_container_width=True)
        
        with col2:
            fig_qualif_credito = px.bar(
                df_qualif[df_qualif['credito_total'] > 0].head(10),
                x='credito_total',
                y='qualificacao',
                orientation='h',
                title='Top 10 Qualificações por Crédito',
                template=filtros['tema'],
                text='credito_total',
                color='credito_total',
                color_continuous_scale='Oranges'
            )
            fig_qualif_credito.update_traces(textposition='outside', texttemplate='R$ %{text:,.0f}')
            st.plotly_chart(fig_qualif_credito, use_container_width=True)
    
    # Base Cadastral
    st.markdown("<div class='sub-header'>📋 Estatísticas da Base Cadastral</div>", unsafe_allow_html=True)
    
    estat_cad = dados.get('estat_cadastral', {})
    estat_socios = dados.get('estat_socios', {})
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "CNPJs Cadastrados",
            f"{estat_cad.get('total_cnpj', 0):,}",
            help="Total de CNPJs na base"
        )
    
    with col2:
        st.metric(
            "CNPJs Ativos",
            f"{estat_cad.get('cnpj_ativo', 0):,}",
            help="Empresas ativas"
        )
    
    with col3:
        st.metric(
            "Sócios Cadastrados",
            f"{estat_socios.get('total_socios', 0):,}",
            help="Total de sócios/titulares"
        )
    
    with col4:
        st.metric(
            "Vínculos Societários",
            f"{estat_socios.get('total_vinculos', 0):,}",
            help="Total de vínculos ativos"
        )

def ranking_grupos(dados, filtros):
    """Ranking de grupos por crédito tributário."""
    st.markdown("<h1 class='main-header'>🏆 Ranking de Grupos Econômicos</h1>", unsafe_allow_html=True)
    
    st.markdown("""
    <div class='info-box'>
        <strong>📋 Sobre este Ranking:</strong><br>
        Lista os grupos econômicos ordenados por crédito tributário (VL_CT = ICMS + Juros + Multa).<br>
        Apenas grupos com débito apurado são exibidos.
    </div>
    """, unsafe_allow_html=True)
    
    # Configurações
    st.subheader("⚙️ Configurações do Ranking")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        top_n = st.slider("Top N grupos", 10, 100, 50, 5, key='top_n_ranking')
    
    with col2:
        filtro_acao = st.multiselect(
            "Filtrar por Ação",
            ['EXCLUSAO_COM_DEBITO', 'EXCLUSAO_SEM_DEBITO', 'SEM_INTERESSE'],
            default=['EXCLUSAO_COM_DEBITO'],
            key='filtro_acao_rank'
        )
    
    with col3:
        min_credito = st.number_input(
            "Crédito Mínimo (R$)",
            min_value=0,
            value=0,
            step=1000,
            key='min_credito'
        )
    
    with col4:
        min_empresas = st.number_input(
            "Mín. Empresas",
            min_value=2,
            value=2,
            step=1,
            key='min_empresas'
        )
    
    df_top = dados.get('top_grupos', pd.DataFrame())
    
    if df_top.empty:
        st.warning("⚠️ Dados não carregados.")
        return
    
    # Filtrar
    if min_credito > 0:
        df_top = df_top[df_top['vl_ct_total'] >= min_credito]
    
    if min_empresas > 2:
        df_top = df_top[df_top['qte_cnpj'] >= min_empresas]
    
    if filtro_acao:
        df_top = df_top[df_top['acao_principal'].isin(filtro_acao)]
    
    df_top = df_top.head(top_n)
    
    # Formatar para exibição
    df_display = df_top.copy()
    df_display['posicao'] = range(1, len(df_display) + 1)
    df_display['cpf_formatado'] = df_display['cpf'].apply(formatar_cpf)
    df_display['vl_ct_formatado'] = df_display['vl_ct_total'].apply(formatar_moeda)
    df_display['receita_formatada'] = df_display['receita_maxima'].apply(formatar_moeda)
    df_display['acao_badge'] = df_display['acao_principal'].apply(criar_badge_acao)
    
    # Estatísticas do filtro
    st.markdown("---")
    st.subheader("📊 Estatísticas do Filtro")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Grupos", len(df_display))
    
    with col2:
        st.metric("Empresas", df_display['empresas_grupo'].sum())
    
    with col3:
        st.metric("Empresas SC", df_display['empresas_sc'].sum())
    
    with col4:
        st.metric("Crédito Total", formatar_moeda(df_display['vl_ct_total'].sum()))
    
    with col5:
        st.metric("Média/Grupo", formatar_moeda(df_display['vl_ct_total'].mean()))
    
    st.markdown("---")
    
    # Tabela principal
    st.subheader(f"📋 Top {len(df_display)} Grupos por Crédito Tributário")
    
    st.dataframe(
        df_display[[
            'posicao', 'num_grupo', 'cpf_formatado', 'qte_cnpj', 'qte_socio',
            'empresas_grupo', 'empresas_sc', 'te_emitir',
            'vl_ct_formatado', 'receita_formatada', 
            'acao_badge', 'periodo_principal'
        ]].rename(columns={
            'posicao': '#',
            'num_grupo': 'Grupo',
            'cpf_formatado': 'CPF Sócio',
            'qte_cnpj': 'CNPJs',
            'qte_socio': 'Sócios',
            'empresas_grupo': 'Empresas',
            'empresas_sc': 'SC',
            'te_emitir': 'TEs',
            'vl_ct_formatado': 'Crédito Total',
            'receita_formatada': 'Receita Máxima',
            'acao_badge': 'Ação',
            'periodo_principal': 'Período'
        }),
        use_container_width=True,
        height=600
    )
    
    # Gráficos
    st.markdown("---")
    st.subheader("📊 Visualizações")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Crédito por Grupo
        df_top_20 = df_display.head(20)
        
        fig_credito = go.Figure()
        
        fig_credito.add_trace(go.Bar(
            x=df_top_20['vl_ct_total'],
            y=df_top_20['num_grupo'].astype(str),
            orientation='h',
            text=df_top_20['vl_ct_total'].apply(lambda x: formatar_moeda(x)),
            textposition='outside',
            marker_color='#c62828',
            hovertemplate='<b>Grupo %{y}</b><br>Crédito: %{text}<extra></extra>'
        ))
        
        fig_credito.update_layout(
            title='Top 20 Grupos por Crédito Tributário',
            xaxis_title='Crédito Tributário (R$)',
            yaxis_title='Número do Grupo',
            template=filtros['tema'],
            height=600
        )
        
        st.plotly_chart(fig_credito, use_container_width=True)
    
    with col2:
        # Scatter: Crédito x Empresas
        fig_scatter = px.scatter(
            df_display,
            x='empresas_grupo',
            y='vl_ct_total',
            size='receita_maxima',
            color='acao_principal',
            hover_name='num_grupo',
            title='Crédito x Quantidade de Empresas',
            template=filtros['tema'],
            color_discrete_map={
                'EXCLUSAO_COM_DEBITO': '#c62828',
                'EXCLUSAO_SEM_DEBITO': '#ef6c00',
                'SEM_INTERESSE': '#2e7d32'
            },
            labels={
                'empresas_grupo': 'Quantidade de Empresas',
                'vl_ct_total': 'Crédito Tributário (R$)',
                'acao_principal': 'Ação'
            }
        )
        
        fig_scatter.update_layout(height=600)
        st.plotly_chart(fig_scatter, use_container_width=True)
    
    # Distribuições
    col1, col2 = st.columns(2)
    
    with col1:
        # Distribuição por Período
        dist_periodo = df_display['periodo_principal'].value_counts().reset_index()
        dist_periodo.columns = ['periodo', 'count']
        
        fig_periodo = px.pie(
            dist_periodo.head(10),
            values='count',
            names='periodo',
            title='Distribuição por Período',
            template=filtros['tema']
        )
        st.plotly_chart(fig_periodo, use_container_width=True)
    
    with col2:
        # Distribuição por Tamanho do Grupo
        df_display['faixa_empresas'] = pd.cut(
            df_display['qte_cnpj'],
            bins=[0, 3, 5, 10, 20, 100],
            labels=['2-3', '4-5', '6-10', '11-20', '20+']
        )
        
        dist_tamanho = df_display['faixa_empresas'].value_counts().reset_index()
        dist_tamanho.columns = ['faixa', 'count']
        
        fig_tamanho = px.bar(
            dist_tamanho,
            x='faixa',
            y='count',
            title='Distribuição por Tamanho do Grupo',
            template=filtros['tema'],
            text='count'
        )
        fig_tamanho.update_traces(textposition='outside')
        st.plotly_chart(fig_tamanho, use_container_width=True)

def analise_detalhada_grupo(dados, filtros, engine):
    """Análise detalhada de um grupo específico."""
    st.markdown("<h1 class='main-header'>🔬 Análise Detalhada - Grupo Econômico</h1>", unsafe_allow_html=True)
    
    lista_grupos = dados.get('lista_grupos', pd.DataFrame())
    
    if lista_grupos.empty:
        st.warning("⚠️ Lista de grupos não carregada.")
        return
    
    # Seleção do grupo
    st.subheader("🎯 Seleção do Grupo")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        # Adicionar filtro de busca
        busca_grupo = st.text_input(
            "🔍 Buscar por Número ou CPF",
            placeholder="Digite o número do grupo ou CPF do sócio...",
            key='busca_grupo'
        )
        
        # Filtrar lista
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
            format_func=lambda x: f"Grupo {x} - {lista_filtrada[lista_filtrada['num_grupo']==x]['qte_cnpj'].iloc[0]} empresas, {lista_filtrada[lista_filtrada['num_grupo']==x]['qte_socio'].iloc[0]} sócios",
            key='select_grupo_detalhes'
        )
    
    with col2:
        st.metric("Grupos Disponíveis", len(lista_filtrada))
        # Botão para limpar análise (se houver uma carregada)
        if st.session_state.get('analise_carregada', False):
            if st.button("🔄 Nova Consulta", use_container_width=True):
                st.session_state.analise_carregada = False
                st.session_state.num_grupo_atual = None
                st.rerun()
    
    if not num_grupo_selecionado:
        st.info("Selecione um grupo para análise.")
        return
    
    # Botão para carregar
    if st.button("🔍 Carregar Análise Completa", type="primary", use_container_width=True):
        # Marca que a análise foi carregada
        st.session_state.analise_carregada = True
        st.session_state.num_grupo_atual = num_grupo_selecionado
    
    # Verifica se deve mostrar a análise
    if st.session_state.get('analise_carregada', False) and st.session_state.get('num_grupo_atual') == num_grupo_selecionado:
        with st.spinner(f'🔄 Carregando dados do Grupo {num_grupo_selecionado}...'):
            df_grupo = carregar_detalhes_grupo(engine, num_grupo_selecionado)
        
        if df_grupo.empty:
            st.error("⚠️ Grupo não encontrado.")
            st.session_state.analise_carregada = False
            return
        
        # Cabeçalho
        grupo_info = df_grupo.iloc[0]
        st.markdown(f"### 🏢 Grupo Econômico #{num_grupo_selecionado}")
        st.caption(f"CPF Sócio: {formatar_cpf(grupo_info['cpf'])} | Total de Empresas: {grupo_info['qte_cnpj']} | Sócios: {grupo_info['qte_socio']}")
        
        # KPIs do Grupo
        st.markdown("<div class='sub-header'>📊 Indicadores do Grupo</div>", unsafe_allow_html=True)
        
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        
        with col1:
            st.metric("Empresas", len(df_grupo))
        
        with col2:
            empresas_sc = df_grupo[df_grupo['uf'] == 'SC'].shape[0]
            st.metric("Empresas SC", empresas_sc)
        
        with col3:
            credito_total = df_grupo['vl_ct'].sum()
            st.metric("Crédito Total", formatar_moeda(credito_total))
        
        with col4:
            receita_max = df_grupo['receita_pa_fato'].max()
            st.metric("Receita Máxima", formatar_moeda(receita_max))
        
        with col5:
            emite_te = df_grupo[df_grupo['emite_te_sc'] == 'S'].shape[0]
            st.metric("Emite TE", emite_te)
        
        with col6:
            ufs_distintas = df_grupo['uf'].nunique()
            st.metric("Estados", ufs_distintas)
        
        # Alertas
        st.markdown("---")
        
        if credito_total > 0:
            perc_credito = (credito_total / 1000000)
            st.markdown(f"""
            <div class='alert-critico'>
                <strong>⚠️ ALERTA DE DÉBITO FISCAL</strong><br>
                Este grupo possui crédito tributário de <strong>{formatar_moeda(credito_total)}</strong>.<br>
                • Empresas com débito: {df_grupo[df_grupo['vl_ct'] > 0].shape[0]}<br>
                • Empresas SC com TE: {emite_te}<br>
                • Recomenda-se ação fiscal imediata
            </div>
            """, unsafe_allow_html=True)
        
        if receita_max > 4800000:
            excedente = receita_max - 4800000
            percentual = (excedente / 4800000) * 100
            st.markdown(f"""
            <div class='alert-alto'>
                <strong>📈 ULTRAPASSAGEM DO LIMITE SIMPLES NACIONAL</strong><br>
                • Receita máxima apurada: <strong>{formatar_moeda(receita_max)}</strong><br>
                • Limite SN: <strong>R$ 4.800.000,00</strong><br>
                • Excedente: <strong>{formatar_moeda(excedente)}</strong> ({percentual:.1f}% acima)<br>
                • Período do fato: {grupo_info['dt_fato']}
            </div>
            """, unsafe_allow_html=True)
        
        # Empresas do Grupo
        st.markdown("<div class='sub-header'>🏭 Empresas do Grupo</div>", unsafe_allow_html=True)
        
        df_empresas = df_grupo.copy()
        df_empresas['cnpj_formatado'] = df_empresas['cnpj_raiz'].apply(formatar_cnpj)
        df_empresas['cpf_formatado'] = df_empresas['cpf'].apply(formatar_cpf)
        df_empresas['vl_ct_formatado'] = df_empresas['vl_ct'].apply(formatar_moeda)
        df_empresas['receita_formatada'] = df_empresas['receita_pa_fato'].apply(formatar_moeda)
        df_empresas['dt_fato_formatada'] = df_empresas['dt_fato'].apply(formatar_periodo)
        df_empresas['dt_efeito_formatada'] = df_empresas['dt_efeito'].apply(formatar_periodo)
        df_empresas['acao_badge'] = df_empresas['acao'].apply(criar_badge_acao)
        
        # Tabs para diferentes visões (com session_state para manter aba selecionada)
        if 'tab_analise_grupo' not in st.session_state:
            st.session_state.tab_analise_grupo = "📋 Tabela Completa"
        
        tab1, tab2, tab3 = st.tabs(["📋 Tabela Completa", "📊 Análises", "🔍 Detalhes Individuais"])
        
        with tab1:
            st.dataframe(
                df_empresas[[
                    'cnpj_formatado', 'razao_social', 'uf', 'situacao_cadastral',
                    'acao_badge', 'vl_ct_formatado', 'receita_formatada',
                    'dt_fato_formatada', 'dt_efeito_formatada',
                    'flag_periodo', 'emite_te_sc', 'qualificacao'
                ]].rename(columns={
                    'cnpj_formatado': 'CNPJ',
                    'razao_social': 'Razão Social',
                    'uf': 'UF',
                    'situacao_cadastral': 'Situação',
                    'acao_badge': 'Ação',
                    'vl_ct_formatado': 'Crédito',
                    'receita_formatada': 'Receita',
                    'dt_fato_formatada': 'Fato',
                    'dt_efeito_formatada': 'Efeito',
                    'flag_periodo': 'Período',
                    'emite_te_sc': 'TE-SC',
                    'qualificacao': 'Qualificação'
                }),
                use_container_width=True,
                height=500
            )
            
            # Download
            csv = df_empresas.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                "📥 Download CSV",
                csv,
                f"grupo_{num_grupo_selecionado}_empresas.csv",
                "text/csv",
                key='download_grupo'
            )
        
        with tab2:
            col1, col2 = st.columns(2)
            
            with col1:
                # Distribuição por UF
                dist_uf = df_grupo.groupby('uf').size().reset_index(name='count')
                fig_uf = px.pie(
                    dist_uf,
                    values='count',
                    names='uf',
                    title='Distribuição por UF',
                    template=filtros['tema']
                )
                st.plotly_chart(fig_uf, use_container_width=True)
                
                # Distribuição por Ação
                dist_acao = df_grupo.groupby('acao').size().reset_index(name='count')
                fig_acao = px.bar(
                    dist_acao,
                    x='acao',
                    y='count',
                    title='Distribuição por Ação',
                    template=filtros['tema'],
                    text='count',
                    color='acao',
                    color_discrete_map={
                        'EXCLUSAO_COM_DEBITO': '#c62828',
                        'EXCLUSAO_SEM_DEBITO': '#ef6c00',
                        'SEM_INTERESSE': '#2e7d32'
                    }
                )
                fig_acao.update_traces(textposition='outside')
                st.plotly_chart(fig_acao, use_container_width=True)
            
            with col2:
                # Crédito por Empresa (Top 10)
                if credito_total > 0:
                    df_top_credito = df_empresas[df_empresas['vl_ct'] > 0].nlargest(10, 'vl_ct')
                    
                    fig_credito = px.bar(
                        df_top_credito,
                        x='vl_ct',
                        y='cnpj_formatado',
                        orientation='h',
                        title='Top 10 Empresas por Crédito',
                        template=filtros['tema'],
                        text='vl_ct_formatado',
                        color='vl_ct',
                        color_continuous_scale='Reds'
                    )
                    fig_credito.update_traces(textposition='outside')
                    st.plotly_chart(fig_credito, use_container_width=True)
                
                # Distribuição por Período
                dist_periodo = df_grupo['flag_periodo'].value_counts().reset_index()
                dist_periodo.columns = ['periodo', 'count']
                
                fig_periodo = px.bar(
                    dist_periodo,
                    x='periodo',
                    y='count',
                    title='Distribuição por Período de Irregularidade',
                    template=filtros['tema'],
                    text='count'
                )
                fig_periodo.update_traces(textposition='outside')
                st.plotly_chart(fig_periodo, use_container_width=True)
        
        with tab3:
            st.markdown("### 🔍 Selecione uma Empresa para Detalhamento")
            
            # Inicializar session_state para esta empresa se não existir
            if f'cnpj_sel_grupo_{num_grupo_selecionado}' not in st.session_state:
                st.session_state[f'cnpj_sel_grupo_{num_grupo_selecionado}'] = df_empresas['cnpj_raiz'].tolist()[0]
            
            # Criar lista de CNPJs disponíveis
            cnpjs_disponiveis = df_empresas['cnpj_raiz'].tolist()
            
            # Verificar se o CNPJ salvo ainda existe na lista
            cnpj_salvo = st.session_state[f'cnpj_sel_grupo_{num_grupo_selecionado}']
            if cnpj_salvo not in cnpjs_disponiveis:
                cnpj_salvo = cnpjs_disponiveis[0]
                st.session_state[f'cnpj_sel_grupo_{num_grupo_selecionado}'] = cnpj_salvo
            
            # Encontrar o índice do CNPJ salvo
            indice_atual = cnpjs_disponiveis.index(cnpj_salvo)
            
            # Selectbox com índice fixo
            cnpj_selecionado = st.selectbox(
                "Empresa:",
                cnpjs_disponiveis,
                format_func=lambda x: f"{formatar_cnpj(x)} - {df_empresas[df_empresas['cnpj_raiz']==x]['razao_social'].iloc[0]}",
                key=f'select_empresa_grupo_{num_grupo_selecionado}',
                index=indice_atual
            )
            
            # Atualizar session_state apenas se mudou
            if cnpj_selecionado != st.session_state[f'cnpj_sel_grupo_{num_grupo_selecionado}']:
                st.session_state[f'cnpj_sel_grupo_{num_grupo_selecionado}'] = cnpj_selecionado
            
            if cnpj_selecionado:
                empresa_detalhes = df_empresas[df_empresas['cnpj_raiz'] == cnpj_selecionado].iloc[0]
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("#### 📋 Dados da Empresa")
                    st.write(f"**CNPJ:** {formatar_cnpj(empresa_detalhes['cnpj_raiz'])}")
                    st.write(f"**Razão Social:** {empresa_detalhes['razao_social']}")
                    st.write(f"**UF:** {empresa_detalhes['uf']}")
                    st.write(f"**Situação:** {empresa_detalhes['situacao_cadastral']}")
                    st.write(f"**Qualificação:** {empresa_detalhes['qualificacao']}")
                    st.write(f"**Regime:** {empresa_detalhes['regime_no_efeito']}")
                
                with col2:
                    st.markdown("#### 💰 Valores Apurados")
                    st.write(f"**Crédito Total:** {formatar_moeda(empresa_detalhes['vl_ct'])}")
                    st.write(f"**Receita (Fato):** {formatar_moeda(empresa_detalhes['receita_pa_fato'])}")
                    st.write(f"**Data Fato:** {formatar_periodo(empresa_detalhes['dt_fato'])}")
                    st.write(f"**Data Efeito:** {formatar_periodo(empresa_detalhes['dt_efeito'])}")
                    st.write(f"**Período:** {empresa_detalhes['flag_periodo']}")
                    st.write(f"**Emite TE-SC:** {empresa_detalhes['emite_te_sc']}")
                
                # Carregar dados cadastrais completos
                with st.spinner('Carregando dados cadastrais...'):
                    df_cad = carregar_detalhes_empresa(engine, cnpj_selecionado)
                    df_socios = carregar_socios_empresa(engine, cnpj_selecionado)
                
                if not df_cad.empty:
                    st.markdown("---")
                    st.markdown("#### 🏢 Dados Cadastrais Completos")
                    
                    empresa = df_cad.iloc[0]
                    
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.write(f"**Natureza Jurídica:** {empresa.get('natureza_juridica_desc', 'N/A')}")
                        st.write(f"**Porte:** {empresa.get('porte_empresa', 'N/A')}")
                        st.write(f"**Capital Social:** {formatar_moeda(empresa.get('capital_social', 0))}")
                    
                    with col2:
                        st.write(f"**CNAE Principal:** {empresa.get('cnae_principal', 'N/A')}")
                        st.write(f"**Data Abertura:** {formatar_data(empresa.get('dt_ini_ativ'))}")
                        st.write(f"**Data Situação:** {formatar_data(empresa.get('dt_sit_cadastral'))}")
                    
                    with col3:
                        st.write(f"**Município:** {empresa.get('codigo_municipio', 'N/A')}")
                        st.write(f"**CEP:** {empresa.get('cep', 'N/A')}")
                        st.write(f"**Matriz:** {'Sim' if empresa.get('flag_matriz') == 1 else 'Não'}")
                
                if not df_socios.empty:
                    st.markdown("---")
                    st.markdown("#### 👥 Quadro Societário")
                    
                    df_socios_display = df_socios.copy()
                    df_socios_display['cpf_formatado'] = df_socios_display['cpf'].apply(formatar_cpf)
                    df_socios_display['dt_ini_formatada'] = df_socios_display['dt_ini_resp'].apply(formatar_data)
                    
                    st.dataframe(
                        df_socios_display[[
                            'cpf_formatado', 'qualificacao', 'socio_ou_titular',
                            'dt_ini_formatada', 'uf'
                        ]].rename(columns={
                            'cpf_formatado': 'CPF',
                            'qualificacao': 'Qualificação',
                            'socio_ou_titular': 'Sócio/Titular',
                            'dt_ini_formatada': 'Início',
                            'uf': 'UF'
                        }),
                        use_container_width=True
                    )
        
        # Histórico do Grupo (se disponível)
        st.markdown("<div class='sub-header'>📈 Evolução Histórica do Grupo</div>", unsafe_allow_html=True)
        
        with st.spinner('Carregando histórico...'):
            df_historico = carregar_historico_grupo(engine, grupo_info['cpf'])
        
        if not df_historico.empty:
            df_historico['periodo_formatado'] = df_historico['pa'].apply(formatar_periodo)
            
            # Gráfico de evolução
            fig_evolucao = make_subplots(
                rows=2, cols=1,
                subplot_titles=('RBA Total do Grupo', 'Quantidade de Empresas'),
                vertical_spacing=0.15,
                row_heights=[0.6, 0.4]
            )
            
            # RBA Total
            fig_evolucao.add_trace(
                go.Scatter(
                    x=df_historico['pa'],
                    y=df_historico['rba_total'],
                    name='RBA Total',
                    fill='tozeroy',
                    line=dict(color='royalblue', width=2),
                    hovertemplate='<b>%{x}</b><br>RBA: R$ %{y:,.2f}<extra></extra>'
                ),
                row=1, col=1
            )
            
            # Linha do limite
            fig_evolucao.add_hline(
                y=4800000,
                line_dash="dash",
                line_color="red",
                annotation_text="Limite SN",
                row=1, col=1
            )
            
            # Quantidade de empresas
            fig_evolucao.add_trace(
                go.Bar(
                    x=df_historico['pa'],
                    y=df_historico['qtd_empresas'],
                    name='Empresas',
                    marker_color='lightblue',
                    hovertemplate='<b>%{x}</b><br>Empresas: %{y}<extra></extra>'
                ),
                row=2, col=1
            )
            
            fig_evolucao.update_xaxes(title_text="Período", row=2, col=1)
            fig_evolucao.update_yaxes(title_text="RBA (R$)", row=1, col=1)
            fig_evolucao.update_yaxes(title_text="Quantidade", row=2, col=1)
            
            fig_evolucao.update_layout(
                height=700,
                showlegend=True,
                template=filtros['tema']
            )
            
            st.plotly_chart(fig_evolucao, use_container_width=True)
            
            # Estatísticas do histórico
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Períodos Analisados", len(df_historico))
            
            with col2:
                periodos_acima = len(df_historico[df_historico['rba_total'] > 4800000])
                st.metric("Períodos Acima Limite", periodos_acima)
            
            with col3:
                rba_max = df_historico['rba_total'].max()
                st.metric("RBA Máxima", formatar_moeda(rba_max))
            
            with col4:
                icms_total = df_historico['icms_total'].sum()
                st.metric("ICMS Total", formatar_moeda(icms_total))
        else:
            st.info("Histórico não disponível para este grupo.")

def analise_detalhada_empresa(dados, filtros, engine):
    """Análise detalhada de uma empresa específica."""
    st.markdown("<h1 class='main-header'>🔬 Análise Detalhada - Empresa</h1>", unsafe_allow_html=True)
    
    lista_empresas = dados.get('lista_empresas', pd.DataFrame())
    
    if lista_empresas.empty:
        st.warning("⚠️ Lista de empresas não carregada.")
        return
    
    # Seleção da empresa
    st.subheader("🎯 Seleção da Empresa")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        busca = st.text_input(
            "🔍 Buscar por Razão Social ou CNPJ",
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
    
    # Limitar para performance
    if len(lista_filtrada) > 1000:
        st.warning(f"⚠️ {len(lista_filtrada):,} empresas encontradas. Mostrando apenas as primeiras 1.000.")
        lista_filtrada = lista_filtrada.head(1000)
    
    if lista_filtrada.empty:
        st.info("Nenhuma empresa encontrada com os filtros aplicados.")
        return
    
    st.caption(f"📊 {len(lista_filtrada):,} empresas disponíveis")
    
    # Criar dicionário para lookup
    empresa_dict = dict(zip(lista_filtrada['cnpj_raiz'], lista_filtrada['razao_social']))
    
    cnpj_selecionado = st.selectbox(
        "Selecione a empresa:",
        lista_filtrada['cnpj_raiz'].tolist(),
        format_func=lambda x: f"{formatar_cnpj(x)} - {empresa_dict.get(x, 'N/A')}",
        key='select_empresa_drill'
    )
    
    if not cnpj_selecionado:
        st.info("Selecione uma empresa para análise.")
        return
    
    # Botão para carregar
    if st.button("🔍 Carregar Análise Completa", type="primary", use_container_width=True):
        with st.spinner(f'🔄 Carregando dados da empresa {formatar_cnpj(cnpj_selecionado)}...'):
            df_empresa = carregar_detalhes_empresa(engine, cnpj_selecionado)
            df_socios = carregar_socios_empresa(engine, cnpj_selecionado)
            df_pgdas = carregar_pgdas_empresa(engine, cnpj_selecionado)
        
        if df_empresa.empty:
            st.error("⚠️ Empresa não encontrada.")
            return
        
        empresa = df_empresa.iloc[0]
        
        # Cabeçalho
        st.markdown(f"### 🏢 {empresa['razao_social']}")
        st.caption(f"CNPJ: {formatar_cnpj(empresa['cnpj_raiz'])} | UF: {empresa['uf']} | Situação: {empresa['situacao_cadastral_desc']}")
        
        # KPIs da Empresa
        st.markdown("<div class='sub-header'>📊 Dados Cadastrais</div>", unsafe_allow_html=True)
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("Porte", empresa.get('porte_empresa', 'N/A'))
        
        with col2:
            st.metric("Capital Social", formatar_moeda(empresa.get('capital_social', 0)))
        
        with col3:
            st.metric("Data Abertura", formatar_data(empresa.get('dt_ini_ativ')))
        
        with col4:
            st.metric("CNAE Principal", empresa.get('cnae_principal', 'N/A'))
        
        with col5:
            matriz = 'Sim' if empresa.get('flag_matriz') == 1 else 'Não'
            st.metric("Matriz", matriz)
        
        # Mais detalhes
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.write(f"**Natureza Jurídica:** {empresa.get('natureza_juridica_desc', 'N/A')}")
            st.write(f"**CPF Responsável:** {formatar_cpf(empresa.get('cpf_responsavel'))}")
        
        with col2:
            st.write(f"**Qualificação Resp.:** {empresa.get('qualificacao_responsavel_desc', 'N/A')}")
            st.write(f"**Município:** {empresa.get('codigo_municipio', 'N/A')}")
        
        with col3:
            st.write(f"**CEP:** {empresa.get('cep', 'N/A')}")
            st.write(f"**Data Situação:** {formatar_data(empresa.get('dt_sit_cadastral'))}")
        
        # Verificar se está na tabela final (grupos irregulares)
        st.markdown("---")
        st.markdown("<div class='sub-header'>⚠️ Status de Irregularidade</div>", unsafe_allow_html=True)
        
        # Query para verificar
        try:
            query_status = f"""
                SELECT *
                FROM {DATABASE}.bcadastro_output_final_acl
                WHERE cnpj_raiz = '{cnpj_selecionado}'
            """
            df_status = pd.read_sql(query_status, engine)
            
            if not df_status.empty:
                status = df_status.iloc[0]
                
                st.markdown(f"""
                <div class='alert-critico'>
                    <strong>🚨 EMPRESA IDENTIFICADA EM GRUPO IRREGULAR</strong><br><br>
                    • <strong>Grupo:</strong> {status['num_grupo']}<br>
                    • <strong>Ação:</strong> {status['acao']}<br>
                    • <strong>Crédito Tributário:</strong> {formatar_moeda(status['vl_ct'])}<br>
                    • <strong>Receita (Fato):</strong> {formatar_moeda(status['receita_pa_fato'])}<br>
                    • <strong>Data Fato Gerador:</strong> {formatar_periodo(status['dt_fato'])}<br>
                    • <strong>Período:</strong> {status['flag_periodo']}<br>
                    • <strong>Emite TE-SC:</strong> {status['emite_te_sc']}
                </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown("""
                <div class='alert-positivo'>
                    <strong>✅ EMPRESA REGULAR</strong><br>
                    Esta empresa não foi identificada em grupos econômicos irregulares.
                </div>
                """, unsafe_allow_html=True)
        except Exception as e:
            st.warning(f"Não foi possível verificar status: {e}")
        
        # Sócios
        if not df_socios.empty:
            st.markdown("---")
            st.markdown("<div class='sub-header'>👥 Quadro Societário</div>", unsafe_allow_html=True)
            
            df_socios_exib = df_socios.copy()
            df_socios_exib['cpf_formatado'] = df_socios_exib['cpf'].apply(formatar_cpf)
            df_socios_exib['dt_ini_formatada'] = df_socios_exib['dt_ini_resp'].apply(formatar_data)
            
            st.dataframe(
                df_socios_exib[[
                    'cpf_formatado', 'qualificacao', 'socio_ou_titular',
                    'dt_ini_formatada', 'uf', 'sit_cadastral'
                ]].rename(columns={
                    'cpf_formatado': 'CPF',
                    'qualificacao': 'Qualificação',
                    'socio_ou_titular': 'Sócio/Titular',
                    'dt_ini_formatada': 'Data Início',
                    'uf': 'UF',
                    'sit_cadastral': 'Situação'
                }),
                use_container_width=True,
                height=300
            )
        
        # PGDAS
        if not df_pgdas.empty:
            st.markdown("---")
            st.markdown("<div class='sub-header'>📈 Histórico PGDAS-D</div>", unsafe_allow_html=True)
            
            df_pgdas['periodo_formatado'] = df_pgdas['periodo_apuracao'].apply(formatar_periodo)
            
            # Gráfico de evolução
            fig_pgdas = make_subplots(
                rows=2, cols=1,
                subplot_titles=('Receita Bruta por Período', 'ICMS Recolhido'),
                vertical_spacing=0.15
            )
            
            fig_pgdas.add_trace(
                go.Scatter(
                    x=df_pgdas['periodo_apuracao'],
                    y=df_pgdas['vl_rpa_int'],
                    mode='lines+markers',
                    name='Receita',
                    line=dict(color='blue', width=2),
                    fill='tozeroy'
                ),
                row=1, col=1
            )
            
            fig_pgdas.add_trace(
                go.Bar(
                    x=df_pgdas['periodo_apuracao'],
                    y=df_pgdas['vl_icms'],
                    name='ICMS',
                    marker_color='green'
                ),
                row=2, col=1
            )
            
            fig_pgdas.update_xaxes(title_text="Período", row=2, col=1)
            fig_pgdas.update_yaxes(title_text="Receita (R$)", row=1, col=1)
            fig_pgdas.update_yaxes(title_text="ICMS (R$)", row=2, col=1)
            
            fig_pgdas.update_layout(
                height=700,
                showlegend=True,
                template=filtros['tema']
            )
            
            st.plotly_chart(fig_pgdas, use_container_width=True)
            
            # Estatísticas
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                receita_total = df_pgdas['vl_rpa_int'].sum()
                st.metric("Receita Total", formatar_moeda(receita_total))
            
            with col2:
                receita_media = df_pgdas['vl_rpa_int'].mean()
                st.metric("Receita Média", formatar_moeda(receita_media))
            
            with col3:
                icms_total = df_pgdas['vl_icms'].sum()
                st.metric("ICMS Total", formatar_moeda(icms_total))
            
            with col4:
                periodos = len(df_pgdas)
                st.metric("Períodos", periodos)

def relatorio_executivo(dados, filtros):
    """Relatório executivo para exportação."""
    st.markdown("<h1 class='main-header'>📄 Relatório Executivo</h1>", unsafe_allow_html=True)
    
    st.markdown("""
    <div class='info-box'>
        <strong>📋 Sobre este Relatório:</strong><br>
        Este módulo gera um relatório consolidado com os principais achados da análise,
        incluindo estatísticas, tabelas e recomendações para ação fiscal.
    </div>
    """, unsafe_allow_html=True)
    
    resumo = dados.get('resumo_geral', {})
    df_acao = dados.get('dist_acao', pd.DataFrame())
    df_periodo = dados.get('dist_periodo', pd.DataFrame())
    df_uf = dados.get('dist_uf', pd.DataFrame())
    df_top = dados.get('top_grupos', pd.DataFrame())
    
    # Sumário Executivo
    st.markdown("<div class='sub-header'>📊 Sumário Executivo</div>", unsafe_allow_html=True)
    
    data_relatorio = datetime.now().strftime('%d/%m/%Y %H:%M')
    
    st.markdown(f"""
    ### Sistema GENESIS - Análise de Grupos Econômicos
    **Data do Relatório:** {data_relatorio}
    
    #### Resumo Geral
    
    O Sistema GENESIS identificou **{resumo.get('total_grupos', 0):,} grupos econômicos** formados por 
    **{resumo.get('total_socios', 0):,} sócios/titulares** que controlam **{resumo.get('total_empresas', 0):,} empresas**.
    
    #### Base Legal
    
    Conforme Lei Complementar 123/2006, Art. 3º, § 4º, Inciso IV, não pode se beneficiar do Simples Nacional 
    a empresa cujo sócio participe com mais de 10% do capital de outra empresa, quando a receita bruta global 
    ultrapassar R$ 4.800.000,00.
    
    #### Principais Indicadores
    
    - **Total de Empresas:** {resumo.get('total_empresas', 0):,}
    - **Empresas em SC:** {resumo.get('empresas_sc', 0):,}
    - **Total de Grupos:** {resumo.get('total_grupos', 0):,}
    - **Sócios/Titulares:** {resumo.get('total_socios', 0):,}
    - **Crédito Tributário Total:** {formatar_moeda(resumo.get('credito_total', 0))}
    - **Crédito Médio por Empresa:** {formatar_moeda(resumo.get('credito_medio', 0))}
    - **Receita Total (Fato Gerador):** {formatar_moeda(resumo.get('receita_total', 0))}
    
    #### Distribuição por Ação Fiscal
    
    - **Exclusão COM Débito:** {resumo.get('exclusao_com_debito', 0):,} empresas
      - Empresas sediadas em SC com crédito tributário apurado
      - Recomenda-se emissão de Termo de Exclusão com cobrança
    
    - **Exclusão SEM Débito:** {resumo.get('exclusao_sem_debito', 0):,} empresas
      - Empresas sediadas em SC sem débito calculado
      - Recomenda-se emissão de Termo de Exclusão preventivo
    
    - **Sem Interesse:** {resumo.get('sem_interesse', 0):,} empresas
      - Empresas fora de SC ou com regime já encerrado
      - Não requer ação fiscal de SC
    
    #### Termos de Exclusão
    
    - **Empresas SC com TE a Emitir:** {resumo.get('emite_te_sc', 0):,}
    - **Percentual sobre Total:** {resumo.get('emite_te_sc', 0) / max(resumo.get('total_empresas', 1), 1) * 100:.1f}%
    """)
    
    st.markdown("---")
    
    # Tabelas Detalhadas
    if not df_acao.empty:
        st.markdown("<div class='sub-header'>📊 Distribuição Detalhada por Ação</div>", unsafe_allow_html=True)
        
        df_acao_display = df_acao.copy()
        df_acao_display['credito_formatado'] = df_acao_display['credito_total'].apply(formatar_moeda)
        df_acao_display['credito_medio_formatado'] = df_acao_display['credito_medio'].apply(formatar_moeda)
        df_acao_display['receita_media_formatada'] = df_acao_display['receita_media'].apply(formatar_moeda)
        df_acao_display['receita_max_formatada'] = df_acao_display['receita_maxima'].apply(formatar_moeda)
        
        st.dataframe(
            df_acao_display[[
                'acao', 'qtd_grupos', 'qtd_empresas', 
                'credito_formatado', 'credito_medio_formatado',
                'receita_media_formatada', 'receita_max_formatada'
            ]].rename(columns={
                'acao': 'Ação',
                'qtd_grupos': 'Grupos',
                'qtd_empresas': 'Empresas',
                'credito_formatado': 'Crédito Total',
                'credito_medio_formatado': 'Crédito Médio',
                'receita_media_formatada': 'Receita Média',
                'receita_max_formatada': 'Receita Máxima'
            }),
            use_container_width=True
        )
    
    # Distribuição Geográfica
    if not df_uf.empty:
        st.markdown("---")
        st.markdown("<div class='sub-header'>🗺️ Distribuição Geográfica</div>", unsafe_allow_html=True)
        
        df_uf_display = df_uf.head(20).copy()
        df_uf_display['credito_formatado'] = df_uf_display['credito_total'].apply(formatar_moeda)
        df_uf_display['credito_medio_formatado'] = df_uf_display['credito_medio'].apply(formatar_moeda)
        
        st.dataframe(
            df_uf_display[[
                'uf', 'qtd_grupos', 'qtd_empresas', 'exclusao_debito', 'emite_te',
                'credito_formatado', 'credito_medio_formatado'
            ]].rename(columns={
                'uf': 'UF',
                'qtd_grupos': 'Grupos',
                'qtd_empresas': 'Empresas',
                'exclusao_debito': 'Exclusões c/ Débito',
                'emite_te': 'TEs a Emitir',
                'credito_formatado': 'Crédito Total',
                'credito_medio_formatado': 'Crédito Médio'
            }),
            use_container_width=True,
            height=500
        )
    
    # Top 50 Grupos Prioritários
    if not df_top.empty:
        st.markdown("---")
        st.markdown("<div class='sub-header'>🎯 Top 50 Grupos Prioritários para Fiscalização</div>", unsafe_allow_html=True)
        
        df_top_50 = df_top.head(50).copy()
        df_top_50['ranking'] = range(1, len(df_top_50) + 1)
        df_top_50['cpf_formatado'] = df_top_50['cpf'].apply(formatar_cpf)
        df_top_50['credito_formatado'] = df_top_50['vl_ct_total'].apply(formatar_moeda)
        df_top_50['receita_formatada'] = df_top_50['receita_maxima'].apply(formatar_moeda)
        df_top_50['acao_badge'] = df_top_50['acao_principal'].apply(criar_badge_acao)
        
        st.dataframe(
            df_top_50[[
                'ranking', 'num_grupo', 'cpf_formatado', 'qte_cnpj', 'qte_socio',
                'empresas_grupo', 'empresas_sc', 'te_emitir',
                'credito_formatado', 'receita_formatada',
                'acao_badge', 'periodo_principal'
            ]].rename(columns={
                'ranking': '#',
                'num_grupo': 'Grupo',
                'cpf_formatado': 'CPF Sócio',
                'qte_cnpj': 'CNPJs',
                'qte_socio': 'Sócios',
                'empresas_grupo': 'Empresas',
                'empresas_sc': 'SC',
                'te_emitir': 'TEs',
                'credito_formatado': 'Crédito Total',
                'receita_formatada': 'Receita Máxima',
                'acao_badge': 'Ação',
                'periodo_principal': 'Período'
            }),
            use_container_width=True,
            height=600
        )
        
        # Download do Top 50
        csv = df_top_50.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            "📥 Download Top 50 (CSV)",
            csv,
            "genesis_top50_grupos.csv",
            "text/csv",
            key='download_top50'
        )
    
    # Recomendações
    st.markdown("---")
    st.markdown("<div class='sub-header'>💡 Recomendações</div>", unsafe_allow_html=True)
    
    st.markdown("""
    ### Plano de Ação Recomendado
    
    #### 1. Priorização Imediata
    
    - **Focar nos Top 50 grupos** com maior crédito tributário
    - **Priorizar ação "EXCLUSAO_COM_DEBITO"** para recuperação de crédito
    - **Empresas SC com TE pendente**: {0} casos
    
    #### 2. Etapas de Execução
    
    **Fase 1 - Notificação (30 dias)**
    - Emitir Termos de Exclusão para empresas identificadas
    - Notificar contribuintes sobre irregularidade
    - Abrir prazo para manifestação/defesa
    
    **Fase 2 - Verificação (60 dias)**
    - Analisar manifestações recebidas
    - Realizar auditorias nos casos prioritários
    - Confirmar valores de crédito tributário
    
    **Fase 3 - Cobrança (90 dias)**
    - Efetuar exclusão do Simples Nacional
    - Iniciar cobrança de créditos tributários
    - Inscrição em Dívida Ativa quando necessário
    
    #### 3. Monitoramento Contínuo
    
    - **Atualização mensal** da base de dados
    - **Acompanhamento de novos grupos** formados
    - **Análise de tendências** e padrões
    - **Relatório trimestral** de resultados
    
    #### 4. Critérios de Priorização
    
    1. **Valor do crédito tributário** (maior impacto fiscal)
    2. **Quantidade de empresas no grupo** (complexidade)
    3. **Localização em SC** (jurisdição direta)
    4. **Tempo de irregularidade** (urgência)
    5. **Histórico fiscal** (reincidência)
    
    #### 5. Indicadores de Sucesso
    
    - Taxa de recuperação de crédito tributário
    - Quantidade de exclusões efetivadas
    - Tempo médio de processamento
    - Percentual de recursos procedentes
    - Impacto na arrecadação estadual
    
    #### 6. Aspectos Legais
    
    - **Base Legal**: LC 123/2006, Art. 3º, § 4º, IV
    - **Prazo de Defesa**: 30 dias (ampla defesa)
    - **Efeitos**: A partir do mês seguinte ao fato gerador
    - **Recurso**: Possível em segunda instância
    """.format(resumo.get('emite_te_sc', 0)))
    
    # Gráfico Final - Impacto Fiscal
    st.markdown("---")
    st.markdown("<div class='sub-header'>💰 Impacto Fiscal Projetado</div>", unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Gráfico de crédito por ação
        if not df_acao.empty:
            fig_impacto = px.pie(
                df_acao,
                values='credito_total',
                names='acao',
                title='Distribuição do Crédito Tributário por Ação',
                template=filtros['tema'],
                color='acao',
                color_discrete_map={
                    'EXCLUSAO_COM_DEBITO': '#c62828',
                    'EXCLUSAO_SEM_DEBITO': '#ef6c00',
                    'SEM_INTERESSE': '#2e7d32'
                }
            )
            st.plotly_chart(fig_impacto, use_container_width=True)
    
    with col2:
        # Gráfico de empresas por UF (Top 10)
        if not df_uf.empty:
            df_uf_top10 = df_uf[df_uf['exclusao_debito'] > 0].head(10)
            
            fig_uf_exclusao = px.bar(
                df_uf_top10,
                x='uf',
                y='exclusao_debito',
                title='Top 10 Estados - Exclusões com Débito',
                template=filtros['tema'],
                text='exclusao_debito',
                color='credito_total',
                color_continuous_scale='Reds'
            )
            fig_uf_exclusao.update_traces(textposition='outside')
            st.plotly_chart(fig_uf_exclusao, use_container_width=True)
    
    # Conclusão
    st.markdown("---")
    st.markdown("""
    ### Conclusão
    
    O Sistema GENESIS identificou um conjunto significativo de grupos econômicos que ultrapassaram 
    o limite do Simples Nacional, representando uma oportunidade importante de regularização fiscal 
    e recuperação de crédito tributário para o Estado de Santa Catarina.
    
    A implementação do plano de ação recomendado, com foco nos grupos prioritários, permitirá:
    
    - Garantir a justiça fiscal e isonomia entre contribuintes
    - Recuperar créditos tributários devidos ao Estado
    - Regularizar o cadastro de contribuintes do Simples Nacional
    - Fortalecer o controle e fiscalização tributária
    
    **Receita Estadual de Santa Catarina**  
    Sistema GENESIS - Grupos Econômicos e Simples Nacional  
    {0}
    """.format(data_relatorio))

def base_cadastral(dados, filtros):
    """Estatísticas da base cadastral."""
    st.markdown("<h1 class='main-header'>📋 Base Cadastral</h1>", unsafe_allow_html=True)
    
    estat_cad = dados.get('estat_cadastral', {})
    estat_socios = dados.get('estat_socios', {})
    df_porte = dados.get('dist_porte', pd.DataFrame())
    df_natureza = dados.get('dist_natureza', pd.DataFrame())
    
    # KPIs Cadastrais
    st.markdown("<div class='sub-header'>📊 Estatísticas Gerais</div>", unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "CNPJs Cadastrados",
            f"{estat_cad.get('total_cnpj', 0):,}",
            help="Total de CNPJ Raiz na base"
        )
    
    with col2:
        st.metric(
            "CNPJs em SC",
            f"{estat_cad.get('cnpj_sc', 0):,}",
            delta=f"{estat_cad.get('cnpj_sc', 0) / max(estat_cad.get('total_cnpj', 1), 1) * 100:.1f}%",
            help="Empresas sediadas em SC"
        )
    
    with col3:
        st.metric(
            "CNPJs Ativos",
            f"{estat_cad.get('cnpj_ativo', 0):,}",
            help="Situação cadastral ativa"
        )
    
    with col4:
        st.metric(
            "Portes Distintos",
            f"{estat_cad.get('portes_distintos', 0):,}",
            help="Classificações de porte"
        )
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        capital_total = estat_cad.get('capital_total', 0)
        st.metric(
            "Capital Social Total",
            formatar_moeda(capital_total),
            help="Soma do capital de todas empresas"
        )
    
    with col2:
        capital_medio = estat_cad.get('capital_medio', 0)
        st.metric(
            "Capital Médio",
            formatar_moeda(capital_medio),
            help="Média de capital social"
        )
    
    with col3:
        total_socios = estat_socios.get('total_socios', 0)
        st.metric(
            "Sócios Cadastrados",
            f"{total_socios:,}",
            help="Total de CPFs únicos"
        )
    
    st.markdown("---")
    
    # Gráficos
    col1, col2 = st.columns(2)
    
    with col1:
        # Distribuição por Porte
        if not df_porte.empty:
            fig_porte = px.pie(
                df_porte,
                values='qtd_empresas',
                names='porte_empresa',
                title='Distribuição por Porte de Empresa',
                template=filtros['tema'],
                hole=0.4
            )
            st.plotly_chart(fig_porte, use_container_width=True)
    
    with col2:
        # Top Naturezas Jurídicas
        if not df_natureza.empty:
            fig_natureza = px.bar(
                df_natureza.head(10),
                x='qtd_empresas',
                y='natureza_juridica_desc',
                orientation='h',
                title='Top 10 Naturezas Jurídicas',
                template=filtros['tema'],
                text='qtd_empresas'
            )
            fig_natureza.update_traces(textposition='outside')
            st.plotly_chart(fig_natureza, use_container_width=True)
    
    # Estatísticas de Sócios
    st.markdown("<div class='sub-header'>👥 Estatísticas de Sócios</div>", unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total de Sócios",
            f"{estat_socios.get('total_socios', 0):,}"
        )
    
    with col2:
        st.metric(
            "Empresas com Sócios",
            f"{estat_socios.get('empresas_com_socios', 0):,}"
        )
    
    with col3:
        st.metric(
            "Total de Vínculos",
            f"{estat_socios.get('total_vinculos', 0):,}"
        )
    
    with col4:
        st.metric(
            "Vínculos SC",
            f"{estat_socios.get('vinculos_sc', 0):,}"
        )

# =============================================================================
# 9. FUNÇÃO PRINCIPAL
# =============================================================================

def main():
    # Sidebar
    st.sidebar.title("🏢 Sistema GENESIS v2.0")
    st.sidebar.caption("Grupos Econômicos e Simples Nacional")
    st.sidebar.markdown("---")
    
    # Conectar ao banco
    engine = get_impala_engine()
    
    if engine is None:
        st.error("❌ Falha na conexão com o banco de dados.")
        st.info("💡 Verifique suas credenciais em `.streamlit/secrets.toml`")
        return
    
    # Testar conexão
    st.sidebar.write("🔍 Testando conexão...")
    if not testar_conexao(engine):
        st.error("❌ Não foi possível conectar ao banco de dados Impala.")
        return
    
    st.sidebar.success("✅ Conexão estabelecida!")
    
    # Menu de navegação
    st.sidebar.subheader("📑 Navegação")
    
    paginas = [
        "📊 Dashboard Executivo",
        "🏆 Ranking de Grupos",
        "🔬 Análise de Grupo",
        "🔍 Análise de Empresa",
        "📄 Relatório Executivo",
        "📋 Base Cadastral"
    ]
    
    pagina_selecionada = st.sidebar.radio(
        "Selecione uma página",
        paginas,
        label_visibility="collapsed"
    )
    
    # Carregar dados agregados
    with st.spinner('🔄 Carregando dados do sistema...'):
        dados = {
            'resumo_geral': carregar_resumo_geral(engine),
            'dist_acao': carregar_distribuicao_acao(engine),
            'dist_periodo': carregar_distribuicao_periodo(engine),
            'dist_uf': carregar_distribuicao_uf(engine),
            'dist_qualificacao': carregar_distribuicao_qualificacao(engine),
            'top_grupos': carregar_top_grupos(engine, 100),
            'lista_grupos': carregar_lista_grupos(engine),
            'lista_empresas': carregar_lista_empresas(engine),
            'estat_cadastral': carregar_estatisticas_cadastrais(engine),
            'estat_socios': carregar_estatisticas_socios(engine),
            'dist_porte': carregar_distribuicao_porte(engine),
            'dist_natureza': carregar_distribuicao_natureza(engine)
        }
    
    # Info na sidebar
    resumo = dados.get('resumo_geral', {})
    if resumo:
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 📈 Indicadores")
        st.sidebar.metric("Grupos", f"{resumo.get('total_grupos', 0):,}")
        st.sidebar.metric("Empresas", f"{resumo.get('total_empresas', 0):,}")
        st.sidebar.metric("Crédito", formatar_moeda(resumo.get('credito_total', 0)))
        st.sidebar.metric("TEs a Emitir", f"{resumo.get('emite_te_sc', 0):,}")
    
    # Filtros visuais
    filtros = criar_filtros_sidebar()
    
    # Botão de limpar cache
    st.sidebar.markdown("---")
    if st.sidebar.button("🔄 Limpar Cache"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.sidebar.success("Cache limpo!")
        st.rerun()
    
    # Informações do sistema
    st.sidebar.markdown("---")
    st.sidebar.markdown("### ℹ️ Informações")
    st.sidebar.caption(f"Versão: 2.0")
    st.sidebar.caption(f"Database: {DATABASE}")
    st.sidebar.caption(f"Atualizado: {datetime.now().strftime('%d/%m/%Y')}")
    
    # Roteamento
    try:
        if pagina_selecionada == "📊 Dashboard Executivo":
            dashboard_executivo(dados, filtros)
        elif pagina_selecionada == "🏆 Ranking de Grupos":
            ranking_grupos(dados, filtros)
        elif pagina_selecionada == "🔬 Análise de Grupo":
            analise_detalhada_grupo(dados, filtros, engine)
        elif pagina_selecionada == "🔍 Análise de Empresa":
            analise_detalhada_empresa(dados, filtros, engine)
        elif pagina_selecionada == "📄 Relatório Executivo":
            relatorio_executivo(dados, filtros)
        elif pagina_selecionada == "📋 Base Cadastral":
            base_cadastral(dados, filtros)
    except Exception as e:
        st.error(f"❌ Erro ao carregar página: {str(e)}")
        st.exception(e)
    
    # Rodapé
    st.markdown("---")
    st.markdown(
        f"<div style='text-align: center; color: #666;'>"
        f"Sistema GENESIS v2.0 | Receita Estadual de SC<br>"
        f"Base Legal: LC 123/2006, Art. 3º, § 4º, IV | "
        f"Última atualização: {datetime.now().strftime('%d/%m/%Y %H:%M')}"
        f"</div>",
        unsafe_allow_html=True
    )

# =============================================================================
# 10. EXECUÇÃO
# =============================================================================

if __name__ == "__main__":
    main()