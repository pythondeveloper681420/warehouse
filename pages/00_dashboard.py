import streamlit as st
import pandas as pd
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import urllib.parse
from pymongo import MongoClient
from bson.objectid import ObjectId
from typing import List, Dict, Any

def converter_objectid_para_str(documentos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converter MongoDB ObjectId para string para serialização JSON."""
    for documento in documentos:
        for chave, valor in documento.items():
            if isinstance(valor, ObjectId):
                documento[chave] = str(valor)
    return documentos

@st.cache_data
def buscar_dados_mongo(
    uri_mongo: str, 
    nome_bd: str, 
    nome_colecao: str, 
    campo_agrupamento: str,
    colunas_selecionadas: List[str]
) -> pl.DataFrame:
    """
    Buscar documentos da coleção MongoDB.
    """
    try:
        cliente = MongoClient(uri_mongo)
        bd = cliente[nome_bd]
        colecao = bd[nome_colecao]

        documentos = list(colecao.find())
        documentos = converter_objectid_para_str(documentos)

        return pl.DataFrame(documentos, infer_schema_length=1000) if documentos else pl.DataFrame()
    
    except Exception as e:
        st.error(f"Erro ao buscar dados do MongoDB: {e}")
        return pl.DataFrame()

def pre_processar_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pré-processar e transformar DataFrame com operações relacionadas a datas.
    """
    df['Data Emissao'] = pd.to_datetime(df['Data Emissao'], errors='coerce', utc=True)
    
    df['mes_ano'] = df['Data Emissao'].dt.strftime('%Y-%m')
    df['ano'] = df['Data Emissao'].dt.strftime('%Y')
    
    return df.sort_values(by=['Data Emissao'], ascending=[False])

def criar_graficos_temporais(df: pd.DataFrame, agrupamento: str):
    """
    Criar gráficos temporais com diferentes métricas.
    """
    # Preparar métricas para visualização
    metricas = [
        ('Valor Total Nota Fiscal', 'Valor Total das Notas', 'total_valor', 'Blues'),
        ('Total itens Nf', 'Total de Itens', 'total_itens', 'Greens')
    ]

    graficos = []
    for coluna, titulo, titulo_agregado, escala in metricas:
        # Agrupar dados
        df_agregado = df.groupby(agrupamento)[coluna].agg(['sum', 'count']).reset_index()
        df_agregado.columns = [agrupamento, titulo_agregado, 'contagem']
        
        # Gráfico de barras
        fig_bar = px.bar(
            df_agregado, 
            x=agrupamento, 
            y=titulo_agregado,
            title=f'{titulo} por {agrupamento}',
            color=titulo_agregado,
            color_continuous_scale=escala,
            hover_data={'contagem': ':.0f'}
        )
        graficos.append(fig_bar)

    return graficos

def criar_graficos_por_dimensao(df: pd.DataFrame, dimensao: str, agrupamento: str):
    """
    Criar gráficos agrupados por dimensão específica.
    """
    # Agregações por dimensão e período
    df_agregado = df.groupby([dimensao, agrupamento])['Valor Total Nota Fiscal'].agg(['sum', 'count']).reset_index()
    
    # Top 10 da dimensão
    top_10 = df_agregado.groupby(dimensao)['sum'].sum().nlargest(10).index

    # Filtrar apenas os top 10
    df_top_10 = df_agregado[df_agregado[dimensao].isin(top_10)]

    # Gráfico de linha para top dimensões
    fig_linha = px.line(
        df_top_10, 
        x=agrupamento, 
        y='sum', 
        color=dimensao,
        title=f'Top 10 {dimensao} - Valor Total por {agrupamento}'
    )

    # Gráfico de barra empilhada
    fig_barra_empilhada = px.bar(
        df_top_10, 
        x=agrupamento, 
        y='sum', 
        color=dimensao,
        title=f'Top 10 {dimensao} - Distribuição por {agrupamento}'
    )

    return [fig_linha, fig_barra_empilhada]

def main():
    st.set_page_config(page_title="Análise Temporal de Notas", page_icon="📊", layout="wide")
    
    st.markdown('## **📊 :rainbow[Painel de Análise Temporal de Notas Fiscais]**')

    # Recuperar credenciais do MongoDB
    username = st.secrets["MONGO_USERNAME"]
    password = st.secrets["MONGO_PASSWORD"]
    cluster = st.secrets["MONGO_CLUSTER"]
    db_name = st.secrets["MONGO_DB"]

    # Construir URI do MongoDB
    escaped_username = urllib.parse.quote_plus(username)
    escaped_password = urllib.parse.quote_plus(password)
    URI_MONGO = f"mongodb+srv://{escaped_username}:{escaped_password}@{cluster}/{db_name}?retryWrites=true&w=majority"

    with st.spinner("Carregando dados..."):
        # Buscar todos os dados necessários
        xml_chave_nfe = buscar_dados_mongo(
            URI_MONGO, db_name, 'xml', 
            'Chave NF-e', 
            ["Data Emissao", "Valor Total Nota Fiscal", "Total itens Nf", 
             "Nome Emitente", "Nota Fiscal", "Nome Material", 
             "Projeto Envio", "Projeto", "Minha Categoria"]
        )

        # Converter para pandas para processar
        df_processado = xml_chave_nfe.to_pandas()
        df_processado = pre_processar_dataframe(df_processado)

        if df_processado.empty:
            st.error("Nenhum dado disponível")
            return

    # Sidebar de Filtros
    st.sidebar.header("🔍 Filtros Avançados")
    
    with st.sidebar:
        # Opção de agrupamento temporal
        tipo_agrupamento = st.radio(
            "Agrupar por:", 
            ["Ano", "Mês/Ano"], 
            horizontal=True
        )

        # Definir coluna de agrupamento baseado na seleção
        coluna_agrupamento = 'ano' if tipo_agrupamento == "Ano" else 'mes_ano'

        # Filtros Múltiplos
        anos_disponiveis = sorted(df_processado['ano'].unique())
        ano_selecionado = st.multiselect("Selecione Anos", anos_disponiveis, default=anos_disponiveis)

        categorias_disponiveis = df_processado['Minha Categoria'].unique()
        categorias_selecionadas = st.multiselect("Selecione Categorias", categorias_disponiveis, default=categorias_disponiveis)

        # Dimensões para análise
        dimensoes_analise = [
            'Nome Emitente', 'Nota Fiscal', 'Nome Material', 
            'Projeto Envio', 'Projeto'
        ]
        dimensao_selecionada = st.selectbox("Selecione Dimensão para Análise", dimensoes_analise)

    # Aplicar filtros
    df_filtrado = df_processado[
        (df_processado['ano'].isin(ano_selecionado)) &
        (df_processado['Minha Categoria'].isin(categorias_selecionadas))
    ]

    # Métricas Principais
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total de Notas", len(df_filtrado))
    with col2:
        st.metric("Valor Total", f"R$ {df_filtrado['Valor Total Nota Fiscal'].sum():,.2f}")
    with col3:
        st.metric("Total de Itens", df_filtrado['Total itens Nf'].sum())
    with col4:
        st.metric("Categorias", len(categorias_selecionadas))

    # Abas de Visualização
    tab1, tab2, tab3 = st.tabs(["Visão Temporal", "Análise por Dimensão", "Dados Detalhados"])

    with tab1:
        # Gráficos temporais gerais
        graficos_temporais = criar_graficos_temporais(df_filtrado, coluna_agrupamento)
        for grafico in graficos_temporais:
            st.plotly_chart(grafico, use_container_width=True)

    with tab2:
        # Gráficos por dimensão selecionada
        graficos_dimensao = criar_graficos_por_dimensao(df_filtrado, dimensao_selecionada, coluna_agrupamento)
        for grafico in graficos_dimensao:
            st.plotly_chart(grafico, use_container_width=True)

    with tab3:
        # Tabela detalhada com filtros
        st.dataframe(df_filtrado[[
            'Data Emissao', 'Nota Fiscal', 'Nome Emitente', 
            'Nome Material', 'Projeto Envio', 'Projeto', 
            'Minha Categoria', 'Valor Total Nota Fiscal', 'Total itens Nf'
        ]])

if __name__ == "__main__":
    main()