import streamlit as st
import pandas as pd
import polars as pl
import plotly.express as px
import urllib.parse
from pymongo import MongoClient
from bson.objectid import ObjectId
from typing import List, Dict, Any

#CFOP Categoria

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
    Buscar documentos únicos da coleção MongoDB, agrupados por um campo específico.
    """
    try:
        cliente = MongoClient(uri_mongo)
        bd = cliente[nome_bd]
        colecao = bd[nome_colecao]

        etapa_agrupamento = {
            "$group": {
                "_id": f"${campo_agrupamento}",
                **{col: {"$first": f"${col}"} for col in colunas_selecionadas if col != campo_agrupamento}
            }
        }

        documentos = list(colecao.aggregate([etapa_agrupamento]))

        for doc in documentos:
            doc[campo_agrupamento] = doc.pop("_id")

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
    df['mes'] = df['Data Emissao'].dt.strftime('%m')
    
    return df.sort_values(by=['Data Emissao'], ascending=[False])

def main():
    st.set_page_config(
        page_title="Painel de Notas Fiscais",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="collapsed"
    )

    st.markdown('## **📊 :rainbow[Painel de Notas Fiscais]**')

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
        # Buscar dados únicos do MongoDB
        xml_cnpj_emitente = buscar_dados_mongo(
            URI_MONGO, db_name, 'xml', 'CNPJ Emitente', 
            ["Nome Emitente"]
        )

        xml_chave_nfe = buscar_dados_mongo(
            URI_MONGO, db_name, 'xml', 'Chave NF-e', 
            ["CNPJ Emitente", "Data Emissao", "Valor Total Nota Fiscal", "Total itens Nf", "CFOP Categoria"]
        )

        # Converter para pandas para mesclar e processar
        pandas_xml_cnpj_emitente = xml_cnpj_emitente.to_pandas()
        pandas_xml_chave_nfe = xml_chave_nfe.to_pandas()

        # Mesclar DataFrames
        df_mesclado = pd.merge(
            pandas_xml_chave_nfe, 
            pandas_xml_cnpj_emitente, 
            on="CNPJ Emitente", 
            how="left"
        )

        # Pré-processar DataFrame
        df_mesclado = pre_processar_dataframe(df_mesclado)

        # Garantir que temos dados
        if df_mesclado.empty:
            st.error("Nenhum dado disponível")
            return

        # Filtro por data
        with st.expander("**Filtros:**", expanded=False):
            col1, col2, col3 = st.columns(3, gap='small')

            # Obter valores únicos de mes_ano
            unique_mes_ano = sorted(df_mesclado['mes_ano'].unique())

            with col1:
                start_index = 0
                mes_ano_inicial = st.selectbox(
                    "Selecione o Mês/Ano Inicial:",
                    options=unique_mes_ano,
                    index=start_index,
                    key="mes_ano_inicial"
                )

            with col2:
                # Filtrar opções e definir padrão para o último mês/ano
                df_filtrado = df_mesclado[df_mesclado['mes_ano'] >= mes_ano_inicial]
                mes_ano_filtrados = sorted(df_filtrado['mes_ano'].unique())
                
                end_index = len(mes_ano_filtrados) - 1 if mes_ano_filtrados else 0
                
                mes_ano_final = st.selectbox(
                    "Selecione o Mês/Ano Final:",
                    options=mes_ano_filtrados,
                    index=end_index,
                    key="mes_ano_final"
                )

            with col3:
                # Opção para escolher base de agrupamento
                base_agrupamento = st.radio(
                    "Agrupar por:",
                    [ "Ano","Mês/Ano",],
                    horizontal=True
                )

            # Adicionar filtro para "CFOP Categoria"
            unique_categories = df_filtrado['CFOP Categoria'].unique()
            unique_categories = sorted(unique_categories)
            unique_categories.insert(0, "All")
            selected_category = st.selectbox(
                "Selecione CFOP Categoria:",
                options=unique_categories,
                index=0,  # Default to "All"
                key="selected_category"
            )

            # Aplicar filtragem final
            if selected_category == "All":
                df_filtrado_final = df_filtrado[
                    (df_filtrado['mes_ano'] >= mes_ano_inicial) & 
                    (df_filtrado['mes_ano'] <= mes_ano_final)
                ]
            else:
                df_filtrado_final = df_filtrado[
                    (df_filtrado['mes_ano'] >= mes_ano_inicial) & 
                    (df_filtrado['mes_ano'] <= mes_ano_final) &
                    (df_filtrado['CFOP Categoria'] == selected_category)
                ]

            # Garantir que temos dados filtrados
            if df_filtrado_final.empty:
                st.warning("Nenhum dado corresponde aos filtros selecionados")
                return

        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(label="Notas Fiscais", value=len(df_filtrado_final))

        # Agrupar dados para visualização
        if base_agrupamento == "Mês/Ano":
            coluna_agrupamento = 'mes_ano'
        else:
            coluna_agrupamento = 'ano'

        df_filtrado_final['total_valor'] = df_filtrado_final.groupby(coluna_agrupamento)['Valor Total Nota Fiscal'].transform('sum')
        df_filtrado_final['total_nfs'] = df_filtrado_final.groupby(coluna_agrupamento)['Chave NF-e'].transform('count')
        df_filtrado_final['total_itens'] = df_filtrado_final.groupby(coluna_agrupamento)['Total itens Nf'].transform('sum')

        # Preparar dados para gráficos
        valor_agrupado = df_filtrado_final[[coluna_agrupamento, 'total_valor']].drop_duplicates()
        nfs_agrupadas = df_filtrado_final[[coluna_agrupamento, 'total_nfs']].drop_duplicates()
        itens_agrupados = df_filtrado_final[[coluna_agrupamento, 'total_itens']].drop_duplicates()

        # Visualizações em abas
        col1, col2, col3 = st.columns([1,1,1])
        with col1:
            fig_valor = px.bar(
                valor_agrupado, 
                x=coluna_agrupamento, 
                y='total_valor',
                title='Valor das Notas por ' + ('Mês/Ano' if base_agrupamento == "Mês/Ano" else "Ano"),
                color='total_valor',
                color_continuous_scale='Blues'
            )
            st.plotly_chart(fig_valor, use_container_width=True)

        with col2:
            fig_nfs = px.line(
                nfs_agrupadas, 
                x=coluna_agrupamento, 
                y='total_nfs',
                title='Número de Notas por ' + ('Mês/Ano' if base_agrupamento == "Mês/Ano" else "Ano")
            )
            st.plotly_chart(fig_nfs, use_container_width=True)

        with col3:
            fig_itens = px.bar(
                itens_agrupados, 
                x=coluna_agrupamento, 
                y='total_itens',
                title='Total de Itens por ' + ('Mês/Ano' if base_agrupamento == "Mês/Ano" else "Ano"),
                color='total_itens',
                color_continuous_scale='Greens'
            )
            st.plotly_chart(fig_itens, use_container_width=True)

        df_filtrado_final['total_valor'] = df_filtrado_final.groupby('Nome Emitente')['Valor Total Nota Fiscal'].transform('sum')
        df_filtrado_final['total_nfs'] = df_filtrado_final.groupby('Nome Emitente')['Chave NF-e'].transform('count')
        df_filtrado_final['total_itens'] = df_filtrado_final.groupby('Nome Emitente')['Total itens Nf'].transform('sum')

        # Preparar dados para gráficos
        valor_agrupado = df_filtrado_final[['Nome Emitente', 'total_valor']].drop_duplicates()
        valor_agrupado=valor_agrupado.sort_values(by=['total_valor'], ascending=[False])
        nfs_agrupadas = df_filtrado_final[['Nome Emitente', 'total_nfs']].drop_duplicates()
        nfs_agrupadas=nfs_agrupadas.sort_values(by=['total_nfs'], ascending=[False])
        itens_agrupados = df_filtrado_final[['Nome Emitente', 'total_itens']].drop_duplicates()
        itens_agrupados=itens_agrupados.sort_values(by=['total_itens'], ascending=[False])

        # Visualizações em abas
        col1, col2, col3 = st.columns([1,1,1])
        with col1:
            fig_valor = px.bar(
                valor_agrupado.head(5), 
                x='Nome Emitente', 
                y='total_valor',
                title='Valor das Notas por ' + ('Mês/Ano' if base_agrupamento == "Mês/Ano" else "Ano"),
                color='total_valor',
                color_continuous_scale='Blues'
            )
            st.plotly_chart(fig_valor, use_container_width=True)

        with col2:
            fig_nfs = px.line(
                nfs_agrupadas.head(5), 
                x='Nome Emitente', 
                y='total_nfs',
                title='Número de Notas por ' + ('Mês/Ano' if base_agrupamento == "Mês/Ano" else "Ano")
            )
            st.plotly_chart(fig_nfs, use_container_width=True)

        with col3:
            fig_itens = px.bar(
                itens_agrupados.head(5), 
                x='Nome Emitente', 
                y='total_itens',
                title='Total de Itens por ' + ('Mês/Ano' if base_agrupamento == "Mês/Ano" else "Ano"),
                color='total_itens',
                color_continuous_scale='Greens'
            )
            st.plotly_chart(fig_itens, use_container_width=True)

            # Horizontal Bar Chart
    with st.container():
        st.subheader("Total Value by Category (Horizontal Bar Chart)")
        # Aggregate data by 'CFOP Categoria'
        category_value = df_filtrado_final.groupby('CFOP Categoria')['total_valor'].sum().reset_index()
        fig_bar_horizontal = px.bar(
            category_value,
            x='total_valor',
            y='CFOP Categoria',
            orientation='h',
            title='Total Value by Category',
            color='total_valor',
            color_continuous_scale='Blues'
        )
        st.plotly_chart(fig_bar_horizontal, use_container_width=True)

    # Pie Chart
    with st.container():
        st.subheader("Distribution of Total Value (Pie Chart)")
        # Use the same aggregated data
        fig_pie = px.pie(
            category_value,
            values='total_valor',
            names='CFOP Categoria',
            title='Distribution of Total Value by Category'
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    # Scatter Plot
    with st.container():
        st.subheader("Scatter Plot: Total Items vs. Total Value")
        # Use the filtered dataframe
        fig_scatter = px.scatter(
            df_filtrado_final,
            x='total_itens',
            y='total_valor',
            color='CFOP Categoria',
            title='Total Items vs. Total Value by Category',
            hover_data=['Nome Emitente']
        )
        st.plotly_chart(fig_scatter, use_container_width=True)        

        with st.container():
            col1, col2, col3 = st.columns(3)
            with col1:
                st.dataframe(valor_agrupado, use_container_width=True)
            with col2:
                st.dataframe(nfs_agrupadas, use_container_width=True)
            with col3:
                st.dataframe(itens_agrupados, use_container_width=True)

        # Visualização dos dados completos
        with st.expander("Dados Filtrados Completos"):
            st.dataframe(df_filtrado_final)

if __name__ == "__main__":
    main()