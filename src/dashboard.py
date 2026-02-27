import streamlit as st
import duckdb
import plotly.express as px

st.set_page_config(page_title="Emendas Parlamentares Gold", layout="wide")

# Connect to your Gold database
con = duckdb.connect('data/03_gold/Emendas_Parlamentares.db', read_only=True)

st.title("🏛️ Dashboard de Execução de Emendas")
st.markdown("Análise da camada **Gold** processada via DuckDB")

# --- SIDEBAR FILTERS ---
st.sidebar.header("Filtros")
anos = con.execute("SELECT DISTINCT ano_referencia FROM fato_execucao_orcamentaria ORDER BY 1 DESC").df()
ano_selecionado = st.sidebar.selectbox("Selecione o Ano", anos)

# --- QUERY DATA ---
# We do the aggregation in DuckDB (super fast) and bring only the result to Python
df_metrics = con.execute(f"""
    SELECT 
        SUM(valor_empenhado) as total_empenhado,
        SUM(valor_pago) as total_pago,
        (SUM(valor_pago) / SUM(valor_empenhado)) * 100 as perc_execucao
    FROM fato_execucao_orcamentaria
    WHERE ano_referencia = {ano_selecionado}
""").df()

# --- KPI METRICS ---
col1, col2, col3 = st.columns(3)
col1.metric("Total Empenhado", f"R$ {df_metrics['total_empenhado'][0]:,.2f}")
col2.metric("Total Pago", f"R$ {df_metrics['total_pago'][0]:,.2f}")
col3.metric("Execução %", f"{df_metrics['perc_execucao'][0]:.1f}%")

# --- CHARTS ---
st.divider()
c1, c2 = st.columns(2)

with c1:
    st.subheader("Top 10 Autores por Valor Pago")
    df_autores = con.execute(f"""
        SELECT a.nome_autor, SUM(f.valor_pago) as total 
        FROM fato_execucao_orcamentaria f
        JOIN dim_autor a ON f.autor_id = a.autor_id
        WHERE f.ano_referencia = {ano_selecionado}
        GROUP BY 1 ORDER BY 2 DESC LIMIT 10
    """).df()
    fig = px.bar(df_autores, x='total', y='nome_autor', orientation='h', color='total')
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.subheader("Distribuição por UF")
    df_uf = con.execute(f"""
        SELECT l.sigla_uf, SUM(f.valor_pago) as total 
        FROM fato_execucao_orcamentaria f
        JOIN dim_localidade l ON f.municipio_id = l.municipio_id
        WHERE f.ano_referencia = {ano_selecionado}
        GROUP BY 1 ORDER BY 2 DESC
    """).df()
    fig_pie = px.pie(df_uf, values='total', names='sigla_uf', hole=0.4)
    st.plotly_chart(fig_pie, use_container_width=True)

con.close()