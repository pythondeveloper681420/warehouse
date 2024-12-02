import streamlit as st
import pandas as pd
import os
import xml.etree.ElementTree as ET
from datetime import date,datetime,timedelta
import re
import time
import pickle
import numpy as np
import io
import unicodedata

import polars as pl
from pymongo import MongoClient
import urllib.parse
from bson.objectid import ObjectId
import plotly.express as px

####

# Função para converter ObjectId para strings
def convert_objectid_to_str(documents):
    for document in documents:
        for key, value in document.items():
            if isinstance(value, ObjectId):
                document[key] = str(value)
    return documents

@st.cache_data
def mongo_collection_to_polars_with_unique_cnpj_emitente(mongo_uri, db_name, collection_xml, selected_columns):
    # Conectar ao MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_xml]

    # Construir o pipeline de agregação
    group_stage = {
        "$group": {
            "_id": "$CNPJ Emitente",  # Agrupar por "CNPJ Emitente"
            **{col: {"$first": f"${col}"} for col in selected_columns if col != "CNPJ Emitente"}  # Pegar o primeiro valor das outras colunas
        }
    }

    # Executar o pipeline de agregação
    documents = list(collection.aggregate([group_stage]))

    # Ajustar os resultados (renomear '_id' para "CNPJ Emitente")
    for doc in documents:
        doc["CNPJ Emitente"] = doc.pop("_id")

    # Converter ObjectId para strings
    documents = convert_objectid_to_str(documents)

    # Se não houver documentos, retornar um DataFrame vazio
    if not documents:
        return pl.DataFrame()

    # Converter documentos em DataFrame Polars
    try:
        polars_df = pl.DataFrame(documents, infer_schema_length=1000)
    except Exception as e:
        st.error(f"Erro ao criar DataFrame Polars: {e}")
        return pl.DataFrame()

    return polars_df

@st.cache_data
def mongo_collection_to_polars_with_unique_chave_nfe(mongo_uri, db_name, collection_xml, selected_columns):
    # Conectar ao MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_xml]

    # Construir o pipeline de agregação
    group_stage = {
        "$group": {
            "_id": "$Chave NF-e",  # Agrupar por "Chave NF-e"
            **{col: {"$first": f"${col}"} for col in selected_columns if col != "Chave NF-e"}  # Pegar o primeiro valor das outras colunas
        }
    }

    # Executar o pipeline de agregação
    documents = list(collection.aggregate([group_stage]))

    # Ajustar os resultados (renomear '_id' para "Chave NF-e")
    for doc in documents:
        doc["Chave NF-e"] = doc.pop("_id")

    # Converter ObjectId para strings
    documents = convert_objectid_to_str(documents)

    # Se não houver documentos, retornar um DataFrame vazio
    if not documents:
        return pl.DataFrame()

    # Converter documentos em DataFrame Polars
    try:
        polars_df = pl.DataFrame(documents, infer_schema_length=1000)
    except Exception as e:
        st.error(f"Erro ao criar DataFrame Polars: {e}")
        return pl.DataFrame()

    return polars_df

# Informações de conexão
username = st.secrets["MONGO_USERNAME"]
password = st.secrets["MONGO_PASSWORD"]
cluster = st.secrets["MONGO_CLUSTER"]
db_name = st.secrets["MONGO_DB"]  # Nome do banco de dados
collection_po = 'po'
collection_xml = 'xml'

# Escapar o nome de usuário e a senha
escaped_username = urllib.parse.quote_plus(username)
escaped_password = urllib.parse.quote_plus(password)

# Montar a string de conexão
MONGO_URI = f"mongodb+srv://{escaped_username}:{escaped_password}@{cluster}/{db_name}?retryWrites=true&w=majority"

# Configuração da página no Streamlit
st.set_page_config(
    page_title="Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Main title
st.markdown('## **📊 :rainbow[Dashboard]**')

# Carregar os dados do MongoDB
with st.spinner("Carregando dados..."):

    # Definir as colunas desejadas para o primeiro DataFrame
    selected_columns = ["Nome Emitente"]

    # Carregar o primeiro DataFrame com valores únicos de "Purchasing Document"
    polars_xml_cnpj_emitente = mongo_collection_to_polars_with_unique_cnpj_emitente(MONGO_URI, db_name, collection_xml, selected_columns)

        # Definir as colunas desejadas para o primeiro DataFrame
    selected_columns = ["CNPJ Emitente","Data Emissao","Valor Total Nota Fiscal","Total itens Nf","Minha Categoria"]

    # Carregar o primeiro DataFrame com valores únicos de "Purchasing Document"
    polars_xml_chave_nfe = mongo_collection_to_polars_with_unique_chave_nfe(MONGO_URI, db_name, collection_xml, selected_columns)
    
    pandas_xml_cnpj_emitente = polars_xml_cnpj_emitente.to_pandas()

    def convert_columns_to_xml_cnpj_emitente(pandas_xml_cnpj_emitente, columns):
        """Converte várias colunas para numérico, forçando erros para NaN."""
        for column in columns:
            pandas_xml_cnpj_emitente[column] = pd.to_numeric(pandas_xml_cnpj_emitente[column], errors='coerce')
        return pandas_xml_cnpj_emitente

    # Supondo que você queira converter as colunas 'po' e 'NFe'
    columns_to_convert = ["CNPJ Emitente"] 
    
    # Converter as colunas relevantes para numérico
    pandas_xml_cnpj_emitente = convert_columns_to_xml_cnpj_emitente(pandas_xml_cnpj_emitente, columns_to_convert)   

    ############

    pandas_xml_chave_nfe = polars_xml_chave_nfe.to_pandas()

    def convert_columns_to_xml_chave_nfe(pandas_xml_chave_nfe, columns):
        """Converte várias colunas para numérico, forçando erros para NaN."""
        for column in columns:
            pandas_xml_chave_nfe[column] = pd.to_numeric(pandas_xml_chave_nfe[column], errors='coerce')
        return pandas_xml_chave_nfe

    # Supondo que você queira converter as colunas 'po' e 'NFe'
    columns_to_convert = ["CNPJ Emitente"] 
    
    # Converter as colunas relevantes para numérico
    pandas_xml_chave_nfe = convert_columns_to_xml_chave_nfe(pandas_xml_chave_nfe, columns_to_convert)   

    ############

    # Mesclagem 1: po_polars com df
    df_merged = pd.merge(
        pandas_xml_chave_nfe, 
        pandas_xml_cnpj_emitente, 
        left_on="CNPJ Emitente", 
        right_on="CNPJ Emitente", 
        how="left"
    )

    df_merged['Data Emissao'] = pd.to_datetime(df_merged['Data Emissao'], errors='coerce', utc=True)

    # Agora, transformar a coluna 'Data Emissao' para string com o formato desejado
    df_merged['mes_ano'] = df_merged['Data Emissao'].dt.strftime('%Y-%m')

    # Agora, transformar a coluna 'Data Emissao' para string com o formato desejado
    df_merged['ano'] = df_merged['Data Emissao'].dt.strftime('%Y')

    df_merged['mes'] = df_merged['Data Emissao'].dt.strftime('%m')

    df_merged = df_merged.sort_values(by=['Data Emissao'], ascending=[False])

    # Obter valores únicos para 'mes_ano'
    unique_mes_ano = df_merged['mes_ano'].unique()

        # Texto em markdown
    texto = ('**Filtros:**')
        # Mostrar o texto com expander
    with st.expander(label=texto, expanded=False):
        col1, col2, col3 = st.columns([1,1,1],gap='small')
        with col1:
            # Seletor de Mês e Ano de Início
            start_mes_ano = st.selectbox(
                "Selecione o Mês e Ano de Início:",
                options=sorted(unique_mes_ano),  # Ordenar os valores
                index=0,  # Definir o valor padrão para o primeiro item
                key="start_mes_ano"  # Chave única para o selectbox
            )

            # Filtrar os dados com base no mês/ano de início
            filtered_df = df_merged[df_merged['mes_ano'] >= start_mes_ano]

            # Atualizar as opções de 'mes_ano' para o filtro de 'fim' com base no filtro de 'início'
            filtered_unique_mes_ano = filtered_df['mes_ano'].unique()

        with col2:
            # Seletor de Mês e Ano de Fim (com as opções filtradas)
            end_mes_ano = st.selectbox(
                "Selecione o Mês e Ano de Fim:",
                options=sorted(filtered_unique_mes_ano),  # Opções baseadas na seleção de início
                index=len(filtered_unique_mes_ano)-1,  # Definir o valor padrão para o último item
                key="end_mes_ano"  # Chave única para o selectbox
            )

            # Filtrar o DataFrame com base na seleção dos meses/anos
            final_filtered_df = filtered_df[
                (filtered_df['mes_ano'] >= start_mes_ano) & 
                (filtered_df['mes_ano'] <= end_mes_ano)
            ]
    # Exibir o DataFrame filtrado
    st.markdown("### **Resultados Filtrados**")
    col1, col2, col3 = st.columns(3)
                
    with col1:
        total_invoices = len(final_filtered_df)
        st.metric(label="### **Notas Fiscais**", value=total_invoices,delta="Notas Fiscais",delta_color='off',help='ok')
    
    # with col2:
    #     unique_issuers = final_filtered_df['emitNome'].nunique()
    #     st.metric(label="Número de Fornecedores", value=unique_issuers)
    
    # with col3:
    #     unique_issuers = final_filtered_df['nNf'].nunique()
    #     st.metric(label="Número de Notas Fiscais", value=unique_issuers)

    
    # Suponha que 'final_filtered_df' seja seu DataFrame já carregado com dados
    groupby_cols_nf = ['mes_ano']
    # Calcular a soma de 'Total itens Nf' por 'mes_ano' e adicionar a nova coluna 'total_itens'
    final_filtered_df['total_valor'] = final_filtered_df.groupby(groupby_cols_nf)['Valor Total Nota Fiscal'].transform('sum')
    # Contando o número de valores únicos de 'Produto' por 'Categoria'
    final_filtered_df['total_nfs'] = final_filtered_df.groupby(groupby_cols_nf)['Chave NF-e'].transform('count')

    # Obter os valores únicos de 'mes_ano' e 'total_itens'
    valor_mes_ano = final_filtered_df[['mes_ano', 'total_valor']].drop_duplicates()
    nfs_mes_ano = final_filtered_df[['mes_ano', 'total_nfs']].drop_duplicates()

    col1, col2, col3 = st.columns([1,1,1])
                
    with col2:
        tab1, tab2, tab3 = st.tabs(["Cat", "Dog", "Owl"])
        with tab1:
            df_merged['Data Emissao'] = pd.to_datetime(df_merged['Data Emissao'], errors='coerce', utc=True)

            df_merged['mes_ano'] = df_merged['Data Emissao'].dt.strftime('%Y-%m')

            df_merged['ano'] = df_merged['Data Emissao'].dt.strftime('%Y')

            df_merged['mes'] = df_merged['Data Emissao'].dt.strftime('%m')

            df_merged = df_merged.sort_values(by=['Data Emissao'], ascending=[False])

            # Obter valores únicos para 'mes_ano'
            unique_mes_ano = df_merged['mes'].unique()
            unique_ano = df_merged['ano'].unique()

                # Texto em markdown
            texto = ('**Filtros:**')
                # Mostrar o texto com expander
            with st.expander(label=texto, expanded=False):
                col1, col2 = st.columns([1,1],gap='small')
                with col1:
                    # Seletor de Mês e Ano de Início
                    s_ano = st.selectbox(
                        "Selecione o Mês e Ano de Início:",
                        options=sorted(unique_ano),  # Ordenar os valores
                        #index=0,  # Definir o valor padrão para o primeiro item
                        index=len(unique_ano)-1,  # Definir o valor padrão para o último item
                        key="s_ano"  # Chave única para o selectbox
                    )

                    # Filtrar os dados com base no mês/ano de início
                    filtered_df = df_merged[df_merged['ano'] >= s_ano]

                    # Atualizar as opções de 'ano' para o filtro de 'fim' com base no filtro de 'início'
                    filtered_unique_ano = filtered_df['ano'].unique()

                with col2:
                    # Seletor de Mês e Ano de Fim (com as opções filtradas)
                    e_ano = st.selectbox(
                        "Selecione o Mês e Ano de Fim:",
                        options=sorted(filtered_unique_ano),  # Opções baseadas na seleção de início
                        index=len(filtered_unique_ano)-1,  # Definir o valor padrão para o último item
                        key="e_ano"  # Chave única para o selectbox
                    )

                    # Filtrar o DataFrame com base na seleção dos meses/anos
                    final_filtered_df = filtered_df[
                        (filtered_df['ano'] >= s_ano) & 
                        (filtered_df['ano'] <= e_ano)
                    ]
                # Suponha que 'final_filtered_df' seja seu DataFrame já carregado com dados
            groupby_cols_nf = ['ano']
            final_filtered_df['total_itens'] = final_filtered_df.groupby(groupby_cols_nf)['Total itens Nf'].transform('sum')
            itens_mes_ano = final_filtered_df[['ano','mes','total_itens']].drop_duplicates()
            # Criar o gráfico de barras com Plotly Express e escala de cor azul
            fig_valor = px.bar(itens_mes_ano, 
                        x='ano'and'mes', 
                        y='total_itens',
                        orientation="h",
                        # x='ano_ano', 
                        # y='total_itens',
                        # orientation="v",
                        color='mes',
                        #color=contagem_fornecedor.values,  # Usar os valores da contagem como base para a cor
                        #color_continuous_scale='Blues',  # Define a escala de cor como azul
                        color_continuous_scale="reds",           
                        text='total_itens',
                        title='Contagem de fornecedores de serviço',
                        #labels={'color': 'Qtd.'}
                        )
            fig_valor.update_xaxes(title=None)
            fig_valor.update_yaxes(title=None)
            fig_valor.update_xaxes(showticklabels=False)
            #fig_valor.update_xaxes(showticklabels=True)
            #fig_valor.update_coloraxes(colorscale=[[0, 'rgb(0, 117, 190)'], [1, 'rgb(0, 117, 190)']])
            #fig_valor.update_coloraxes(colorscale=[[0, 'rgb(0, 58, 112)'], [1, 'rgb(0, 117, 190)']])
            #fig_valor.update_coloraxes(showscale=False)
            # fig_valor.update_layout(barmode='group')
            # fig_valor.update_layout(barmode='stack')
            # Exibir o gráfico usando o componente Streamlit plotly_chart
            st.plotly_chart(fig_valor, theme="streamlit",use_container_width=True)
        with tab2:
                # Suponha que 'final_filtered_df' seja seu DataFrame já carregado com dados
            groupby_cols_nf = ['ano']
            final_filtered_df['total_itens'] = final_filtered_df.groupby(groupby_cols_nf)['Total itens Nf'].transform('sum')
            itens_mes_ano = final_filtered_df[['ano','total_itens']].drop_duplicates()
            # Criar o gráfico de barras com Plotly Express e escala de cor azul
            fig_valor = px.bar(itens_mes_ano, 
                        y='ano', 
                        x='total_itens',
                        orientation="h",
                        # x='mes_ano', 
                        # y='total_itens',
                        # orientation="v",
                        color='ano',
                        #color=contagem_fornecedor.values,  # Usar os valores da contagem como base para a cor
                        color_continuous_scale='Blues',  # Define a escala de cor como azul           
                        text='total_itens',
                        title='Contagem de fornecedores de serviço',
                        #labels={'color': 'Qtd.'}
                        )
            fig_valor.update_xaxes(title=None)
            fig_valor.update_yaxes(title=None)
            fig_valor.update_xaxes(showticklabels=False)
            #fig_valor.update_xaxes(showticklabels=True)
            fig_valor.update_coloraxes(colorscale=[[0, 'rgb(0, 117, 190)'], [1, 'rgb(0, 58, 112)']])
            #fig_valor.update_coloraxes(colorscale=[[0, 'rgb(0, 58, 112)'], [1, 'rgb(0, 117, 190)']])
            #fig_valor.update_coloraxes(showscale=False)
            # fig_valor.update_layout(barmode='group')
            # fig_valor.update_layout(barmode='stack')
            # Exibir o gráfico usando o componente Streamlit plotly_chart
            st.plotly_chart(fig_valor, theme="streamlit",use_container_width=True)

        with tab3:
            st.dataframe(itens_mes_ano,use_container_width=True,hide_index=True)
            st.write(f"{len(itens_mes_ano)}") 
    st.dataframe(valor_mes_ano)
    st.write(f"{len(valor_mes_ano)}") 
    st.dataframe(nfs_mes_ano) 
    st.write(f"{len(nfs_mes_ano)}") 

    # Exibir o número total de registros filtrados
    st.dataframe(final_filtered_df)
    # Exibir o número total de registros filtrados
    st.write(f"Número total de registros filtrados: {len(final_filtered_df)}")