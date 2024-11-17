import streamlit as st
import pandas as pd
from pymongo import MongoClient
import urllib.parse
import unicodedata
import re
from bson.objectid import ObjectId

# Função para converter ObjectId para strings e tratar tipos de dados
def convert_document_for_pandas(doc):
    for key, value in doc.items():
        if isinstance(value, ObjectId):
            doc[key] = str(value)
        elif isinstance(value, dict):
            convert_document_for_pandas(value)
        elif isinstance(value, list):
            doc[key] = [str(item) if isinstance(item, ObjectId) else item for item in value]
    return doc

# Função para normalizar strings
def normalize_string(text):
    if not isinstance(text, str):
        return str(text)
    text = unicodedata.normalize('NFKD', text)
    text = re.sub(r'[^\w\s]', '', text)
    return text.lower()

# Função para carregar a coleção MongoDB
@st.cache_data
def load_collection_data(mongo_uri, db_name, collection_name):
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
        
        # Buscar documentos e converter para lista
        documents = list(collection.find())
        
        # Converter e tratar documentos
        documents = [convert_document_for_pandas(doc) for doc in documents]
        
        # Criar DataFrame
        if documents:
            df = pd.DataFrame(documents)
            return df
        else:
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Erro ao carregar dados: {str(e)}")
        return pd.DataFrame()

# Função para criar interface de filtros
def create_filters(df, collection_name):
    if df.empty:
        st.warning(f"Nenhum dado disponível na coleção {collection_name}")
        return None
    
    st.subheader(f"Filtros para {collection_name}")
    
    # Selecionar colunas para filtrar
    columns = df.columns.tolist()
    selected_columns = st.multiselect(
        "Selecione as colunas para filtrar:",
        columns,
        key=f"col_select_{collection_name}"
    )
    
    filtered_df = df.copy()
    applied_filters = False
    
    if selected_columns:
        st.write("Configure os filtros para cada coluna:")
        
        # Criar três colunas para layout
        cols = st.columns(3)
        
        # Para cada coluna selecionada
        for idx, column in enumerate(selected_columns):
            with cols[idx % 3]:
                st.write(f"**{column}**")
                
                # Verificar tipo de dados da coluna
                unique_values = df[column].unique()
                
                # Opção de tipo de filtro para texto
                if df[column].dtype == 'object':
                    filter_type = st.radio(
                        f"Tipo de filtro para {column}",
                        ["Texto", "Seleção Múltipla"],
                        key=f"filter_type_{collection_name}_{column}"
                    )
                    
                    if filter_type == "Texto":
                        search_text = st.text_input(
                            "Digite para buscar:",
                            key=f"text_{collection_name}_{column}"
                        )
                        if search_text:
                            applied_filters = True
                            mask = df[column].fillna("").astype(str).apply(normalize_string).str.contains(
                                normalize_string(search_text)
                            )
                            filtered_df = filtered_df[mask]
                    else:
                        selected_values = st.multiselect(
                            "Selecione os valores:",
                            unique_values,
                            key=f"multi_{collection_name}_{column}"
                        )
                        if selected_values:
                            applied_filters = True
                            filtered_df = filtered_df[filtered_df[column].isin(selected_values)]
                
                # Para colunas numéricas
                elif pd.api.types.is_numeric_dtype(df[column]):
                    selected_values = st.multiselect(
                        "Selecione os valores:",
                        sorted(unique_values),
                        key=f"num_{collection_name}_{column}"
                    )
                    if selected_values:
                        applied_filters = True
                        filtered_df = filtered_df[filtered_df[column].isin(selected_values)]
    
    # Exibir resultados
    if applied_filters:
        st.subheader("Resultados")
        if filtered_df.empty:
            st.warning("Nenhum resultado encontrado com os filtros aplicados.")
        else:
            st.success(f"Encontrados {len(filtered_df)} registros.")
            st.dataframe(filtered_df, use_container_width=True)
    else:
        st.info("Selecione os filtros desejados para visualizar os dados.")
        if not df.empty:
            st.dataframe(df, use_container_width=True)

# Configuração da página
st.set_page_config(
    page_title="Dashboard MongoDB",
    page_icon="📊",
    layout="wide"
)

# Título principal
st.title("📊 Dashboard MongoDB")

# Configurações de conexão
username = 'devpython86'
password = 'dD@pjl06081420'
cluster = 'cluster0.akbb8.mongodb.net'
db_name = 'warehouse'
collections = ['xml', 'po', 'nfspdf']

# String de conexão MongoDB
escaped_username = urllib.parse.quote_plus(username)
escaped_password = urllib.parse.quote_plus(password)
MONGO_URI = f"mongodb+srv://{escaped_username}:{escaped_password}@{cluster}/{db_name}?retryWrites=true&w=majority"

# Criar tabs para cada coleção
tabs = st.tabs([collection.upper() for collection in collections])

# Processar cada coleção em sua respectiva tab
for tab, collection_name in zip(tabs, collections):
    with tab:
        with st.spinner(f"Carregando dados da coleção {collection_name}..."):
            df = load_collection_data(MONGO_URI, db_name, collection_name)
            if not df.empty:
                st.success(f"✅ Dados da coleção {collection_name} carregados com sucesso!")
                create_filters(df, collection_name)
            else:
                st.error(f"Não foi possível carregar dados da coleção {collection_name}")

# Rodapé simples
st.divider()
st.caption("Dashboard de Dados MongoDB v1.0")