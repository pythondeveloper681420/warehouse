import streamlit as st
import pandas as pd
import polars as pl
from pymongo import MongoClient
from bson.objectid import ObjectId
import urllib.parse
import io
import re
import unicodedata

# Configuração da página
st.set_page_config(
    page_title="Processador de Dados",
    page_icon=":bar_chart:",
    layout="wide"
)

# Variáveis de colunas selecionadas
colunas_selecionadas = [
    "tags",
    "grupo",
    "subgrupo",
    # Adicione outras colunas conforme necessário
]

def converter_objectid_para_string(documentos):
    """
    Converte ObjectIds para strings para processamento adequado
    """
    for documento in documentos:
        if "_id" in documento:
            documento["ObjectId"] = str(documento.pop("_id"))
        for chave, valor in documento.items():
            if isinstance(valor, ObjectId):
                documento[chave] = str(valor)
    return documentos

@st.cache_data
def mongo_para_polars_categoria(uri_mongo, nome_bd, colecao_categoria, colunas_selecionadas):
    """
    Carrega dados da coleção de categorias para um DataFrame Polars
    """
    try:
        with MongoClient(uri_mongo) as cliente:
            bd = cliente[nome_bd]
            colecao = bd[colecao_categoria]
            
            projecao = {col: 1 for col in colunas_selecionadas}
            documentos = list(colecao.find({}, projecao))
            
            documentos = [{k: v for k, v in doc.items() if k != '_id'} for doc in documentos]
            documentos = converter_objectid_para_string(documentos)
            
            return pl.DataFrame(documentos, infer_schema_length=1000) if documentos else pl.DataFrame()
    except Exception as erro:
        st.error(f"Erro ao carregar dados: {erro}")
        return pl.DataFrame()

@st.cache_data
def mongo_para_polars_xml(uri_mongo, nome_bd, colecao_xml, colunas_selecionadas):
    """
    Carrega dados da coleção XML para um DataFrame Polars
    """
    try:
        with MongoClient(uri_mongo) as cliente:
            bd = cliente[nome_bd]
            colecao = bd[colecao_xml]
            
            documentos = list(colecao.find({}))
            documentos = converter_objectid_para_string(documentos)
            
            return pl.DataFrame(documentos, infer_schema_length=1000) if documentos else pl.DataFrame()
    except Exception as erro:
        st.error(f"Erro ao carregar dados XML: {erro}")
        return pl.DataFrame()

def gerar_slug(texto):
    """
    Converte texto para formato slug
    """
    if not isinstance(texto, str):
        texto = str(texto)
    
    texto = texto.lower()
    texto = unicodedata.normalize('NFKD', texto)
    texto = texto.encode('ascii', 'ignore').decode('utf-8')
    texto = re.sub(r'[^a-z0-9]+', '-', texto)
    texto = texto.strip('-')
    texto = re.sub(r'-+', '-', texto)
    
    return texto

def main():
    # Título e descrição
    st.title("🔍 Processador de Dados MongoDB")
    st.markdown("### Visualização e Download de Dados")

    # Configurações de conexão segura
    try:
        username = st.secrets["MONGO_USERNAME"]
        password = st.secrets["MONGO_PASSWORD"]
        cluster = st.secrets["MONGO_CLUSTER"]
        nome_bd = st.secrets["MONGO_DB"]
    except KeyError:
        st.error("Configurações de conexão não encontradas")
        return

    # Criar URI de conexão
    uri_mongo = f"mongodb+srv://{urllib.parse.quote_plus(username)}:{urllib.parse.quote_plus(password)}@{cluster}/{nome_bd}?retryWrites=true&w=majority"

    # Coleções
    colecao_categoria = 'category'
    colecao_xml = 'xml'

    # Carregar dados
    polars_cat = mongo_para_polars_categoria(uri_mongo, nome_bd, colecao_categoria, colunas_selecionadas)
    polars_xml = mongo_para_polars_xml(uri_mongo, nome_bd, colecao_xml, colunas_selecionadas)

    # Converter para Pandas
    pandas_cat = polars_cat.to_pandas() if not polars_cat.is_empty() else pd.DataFrame()
    pandas_xml = polars_xml.to_pandas() if not polars_xml.is_empty() else pd.DataFrame()

    # Mesclar DataFrames
    df_mesclado = pd.merge(pandas_xml, pandas_cat, on="tags", how="left")
    
    # Remover duplicatas
    df_mesclado.drop_duplicates(subset='unique', inplace=True)

    # Visualização dos dados
    if not df_mesclado.empty:
        # Guias para visualização
        tab1, tab2, tab3 = st.tabs(["📊 Dados", "📋 Resumo", "⬇️ Download"])

        with tab1:
            st.subheader("Visualização de Dados")
            st.dataframe(df_mesclado, use_container_width=True)

        with tab2:
            st.subheader("Resumo dos Dados")
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total de Registros", len(df_mesclado))
                st.metric("Número de Colunas", len(df_mesclado.columns))
            with col2:
                st.write("Colunas:")
                st.write(df_mesclado.columns.tolist())

        with tab3:
            st.subheader("Download dos Dados")
            # Botão de download em Excel
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                df_mesclado.to_excel(writer, index=False, sheet_name='Dados')
            
            st.download_button(
                label="📥 Baixar Arquivo Excel",
                data=buffer.getvalue(),
                file_name='dados_processados.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
    else:
        st.warning("Nenhum dado disponível para exibição.")

    # Rodapé
    st.markdown("---")
    st.markdown("*Processador de Dados MongoDB - Versão 1.0*")

if __name__ == "__main__":
    main()