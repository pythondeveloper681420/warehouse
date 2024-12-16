import streamlit as st
import pandas as pd
import polars as pl
from pymongo import MongoClient
from bson.objectid import ObjectId
import urllib.parse
import io
from typing import List, Dict

def converter_objectid_para_str(documentos: List[Dict]) -> List[Dict]:
    """
    Converte ObjectIds para strings para melhor manipulação de dados.
    
    Args:
        documentos (List[Dict]): Lista de documentos do MongoDB
    
    Returns:
        List[Dict]: Documentos com ObjectIds convertidos para strings
    """
    for documento in documentos:
        if "_id" in documento:
            documento["ObjectId"] = str(documento.pop("_id"))
        for chave, valor in list(documento.items()):
            if isinstance(valor, ObjectId):
                documento[chave] = str(valor)
    return documentos

@st.cache_data
def carregar_colecao_mongodb(
    uri_mongodb: str, 
    nome_bd: str, 
    nome_colecao: str, 
    colunas_selecionadas: List[str]
) -> pl.DataFrame:
    """
    Carrega uma coleção do MongoDB usando Polars para otimização.
    
    Args:
        uri_mongodb (str): URI de conexão do MongoDB
        nome_bd (str): Nome do banco de dados
        nome_colecao (str): Nome da coleção
        colunas_selecionadas (List[str]): Colunas a serem carregadas
    
    Returns:
        pl.DataFrame: DataFrame Polars com os dados
    """
    try:
        with MongoClient(uri_mongodb) as cliente:
            bd = cliente[nome_bd]
            colecao = bd[nome_colecao]
            
            # Projeção para buscar apenas as colunas necessárias
            projecao = {col: 1 for col in colunas_selecionadas}
            
            # Buscar documentos com projeção e converter
            documentos = list(colecao.find({}, projecao))
            #documentos = [{k: v for k, v in doc.items() if k != '_id'} for doc in documentos]
            documentos = converter_objectid_para_str(documentos)
            
            return pl.DataFrame(documentos, infer_schema_length=None)
    
    except Exception as e:
        st.error(f"Erro ao carregar a coleção {nome_colecao}: {e}")
        return pl.DataFrame()

def calcular_similaridade_tags(df_xml: pd.DataFrame, df_cat: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula similaridade de tags para preencher dados ausentes.
    
    Args:
        df_xml (pd.DataFrame): DataFrame de XML
        df_cat (pd.DataFrame): DataFrame de categorias
    
    Returns:
        pd.DataFrame: DataFrame com dados preenchidos
    """
    # Pré-processar tags
    df_xml['tags_processadas'] = df_xml['tags'].fillna('').str.lower()
    df_cat['tags_processadas'] = df_cat['tags'].fillna('').str.lower()
    
    # Dividir tags em conjuntos de palavras
    df_xml['palavras_tags'] = df_xml['tags_processadas'].str.split()
    df_cat['palavras_tags'] = df_cat['tags_processadas'].str.split()
    
    df_mesclado = df_xml.copy()
    
    # Máscara para linhas com grupo ou subgrupo ausentes
    mascara_ausente = df_mesclado['grupo'].isna() | df_mesclado['subgrupo'].isna()
    
    for idx in df_mesclado.index[mascara_ausente]:
        tags_linha = set(df_mesclado.loc[idx, 'palavras_tags'])
        
        # Computar similaridades
        similaridades = df_cat['palavras_tags'].apply(lambda x: len(set(x).intersection(tags_linha)))
        
        # Encontrar a melhor correspondência
        idx_melhor_correspondencia = similaridades.idxmax()
        melhor_correspondencia = df_cat.loc[idx_melhor_correspondencia]
        
        # Atualizar valores ausentes
        if pd.isna(df_mesclado.loc[idx, 'grupo']) and not pd.isna(melhor_correspondencia['grupo']):
            df_mesclado.loc[idx, 'grupo'] = melhor_correspondencia['grupo']
        
        if pd.isna(df_mesclado.loc[idx, 'subgrupo']) and not pd.isna(melhor_correspondencia['subgrupo']):
            df_mesclado.loc[idx, 'subgrupo'] = melhor_correspondencia['subgrupo']
    
    # Limpar colunas temporárias
    df_mesclado.drop(columns=['tags_processadas', 'palavras_tags'], inplace=True)
    
    return df_mesclado

def main():
    # Configuração da página
    st.set_page_config(
        page_title="Processamento de Dados MongoDB", 
        page_icon="📊", 
        layout="wide"
    )
    
    # Título e descrição
    st.title("🔍 Processador de Dados MongoDB")
    st.markdown("""
    ### Processamento Inteligente de Dados
    Este aplicativo carrega e processa dados de coleções do MongoDB, 
    preenchendo automaticamente informações ausentes usando similaridade de tags.
    """)

    # Colunas selecionadas
    colunas_selecionadas = [
        "Nome Material",
        "tags",
        "grupo",
        "subgrupo",
        "unique"
    ]

    # Parâmetros de conexão do MongoDB
    try:
        username = st.secrets["MONGO_USERNAME"]
        password = st.secrets["MONGO_PASSWORD"]
        cluster = st.secrets["MONGO_CLUSTER"]
        nome_bd = st.secrets["MONGO_DB"]
    except KeyError:
        st.error("Credenciais do MongoDB não configuradas.")
        st.stop()

    # Escapar credenciais
    username_escaped = urllib.parse.quote_plus(username)
    password_escaped = urllib.parse.quote_plus(password)

    # Construir URI de conexão
    URI_MONGODB = f"mongodb+srv://{username_escaped}:{password_escaped}@{cluster}/{nome_bd}?retryWrites=true&w=majority"

    # Botão para carregar dados
    if st.button("🚀 Carregar e Processar Dados"):
        with st.spinner("Carregando dados do MongoDB..."):
            # Carregar coleções
            polars_cat = carregar_colecao_mongodb(
                URI_MONGODB, nome_bd, 'category', colunas_selecionadas
            )
            polars_xml = carregar_colecao_mongodb(
                URI_MONGODB, nome_bd, 'xml', colunas_selecionadas
            )

            # Converter para Pandas
            pandas_cat = polars_cat.to_pandas() if not polars_cat.is_empty() else pd.DataFrame()
            pandas_xml = polars_xml.to_pandas() if not polars_xml.is_empty() else pd.DataFrame()

            # Verificar se há dados
            if pandas_cat.empty or pandas_xml.empty:
                st.warning("Não foi possível carregar os dados.")
                st.stop()

            # Mesclar DataFrames
            df_mesclado = pd.merge(
                pandas_xml, 
                pandas_cat, 
                on="tags", 
                how="left",
                suffixes=('_xml', '_cat')
            )

            # Preencher dados ausentes
            df_mesclado = calcular_similaridade_tags(df_mesclado, pandas_cat)

            # Remover duplicatas
            df_mesclado.drop_duplicates(subset='unique', inplace=True)
            df_mesclado.drop_duplicates(subset='tags', inplace=True)
            df_mesclado.drop(columns='unique', errors='ignore', inplace=True)

            # Exibir resultados
            st.success(f"✅ Dados processados com sucesso! Total de registros: {len(df_mesclado)}")
            
            # Visualização dos dados
            st.dataframe(df_mesclado)

            # Botão de download
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                df_mesclado.to_excel(writer, index=False, sheet_name='Dados')
            
            st.download_button(
                label="📥 Baixar Dados em Excel",
                data=buffer.getvalue(),
                file_name='dados_processados.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )

    # Rodapé
    st.markdown("---")
    st.markdown("*Desenvolvido com 💡 Processamento Inteligente de Dados*")

if __name__ == "__main__":
    main()