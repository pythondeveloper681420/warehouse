import streamlit as st
import pandas as pd
from pymongo import MongoClient
import urllib.parse
import numpy as np
from datetime import datetime

# Configuração da página Streamlit
st.set_page_config(page_title="Upload Excel para MongoDB", layout="centered")

# Configurações do MongoDB
USERNAME = urllib.parse.quote_plus('devpython86')
PASSWORD = urllib.parse.quote_plus('dD@pjl06081420')
CLUSTER = 'cluster0.akbb8.mongodb.net'
DB_NAME = 'warehouse'

def handle_date(value):
    """Função específica para tratar datas"""
    if pd.isna(value) or pd.isnull(value):
        return None
    if isinstance(value, pd.Timestamp):
        try:
            return value.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return None
    return value

def clean_dataframe(df):
    """Limpa e prepara o DataFrame para inserção no MongoDB"""
    df_clean = df.copy()
    
    for column in df_clean.columns:
        # Verifica se a coluna contém datas
        if df_clean[column].dtype in ['datetime64[ns]', 'datetime64[ns, UTC]']:
            df_clean[column] = df_clean[column].apply(handle_date)
        else:
            # Para outros tipos de dados
            df_clean[column] = df_clean[column].apply(lambda x: None if pd.isna(x) 
                                                    else int(x) if isinstance(x, np.integer) 
                                                    else float(x) if isinstance(x, np.floating)
                                                    else bool(x) if isinstance(x, np.bool_)
                                                    else str(x) if isinstance(x, np.datetime64)
                                                    else x)
    
    return df_clean

def connect_mongodb():
    """Função para conectar ao MongoDB"""
    try:
        connection_string = f"mongodb+srv://{USERNAME}:{PASSWORD}@{CLUSTER}/?retryWrites=true&w=majority"
        client = MongoClient(connection_string)
        client.server_info()
        return client[DB_NAME]
    except Exception as e:
        st.error(f"Erro ao conectar ao MongoDB: {e}")
        return None

def upload_to_mongodb(df, collection_name):
    """Função para fazer upload do DataFrame para o MongoDB"""
    db = connect_mongodb()
    if db is not None:
        try:
            # Limpa e prepara o DataFrame
            df_clean = clean_dataframe(df)
            
            # Converte para dicionário e remove índice se existir
            records = df_clean.to_dict('records')
            
            # Insere no MongoDB
            collection = db[collection_name]
            result = collection.insert_many(records)
            
            return True, len(result.inserted_ids)
            
        except Exception as e:
            return False, f"Erro ao fazer upload dos dados: {e}"
    return False, "Erro na conexão com o MongoDB"

def main():
    st.title("Upload de Excel para MongoDB")
    
    # Container para mensagens
    message_container = st.empty()
    
    # Campo para nome da collection
    collection_name = st.text_input(
        "Nome da Collection",
        placeholder="Digite o nome da collection"
    ).strip()
    
    # Upload do arquivo Excel
    uploaded_file = st.file_uploader(
        "Escolha um arquivo Excel",
        type=['xlsx', 'xls']
    )
    
    if uploaded_file is not None:
        try:
            # Lê o arquivo Excel
            df = pd.read_excel(uploaded_file)
            
            if not df.empty:
                # Mostra preview dos dados
                st.subheader("Preview dos Dados")
                st.dataframe(df.head())
                
                # Mostra informações sobre os tipos de dados
                st.subheader("Tipos de Dados")
                df_types = pd.DataFrame({
                    'Coluna': df.columns,
                    'Tipo': df.dtypes.values.astype(str)
                })
                st.dataframe(df_types)
                
                # Estatísticas básicas
                st.subheader("Informações Gerais")
                col1, col2 = st.columns(2)
                with col1:
                    st.info(f"Total de Linhas: {len(df)}")
                with col2:
                    st.info(f"Total de Colunas: {len(df.columns)}")
                
                # Botão para fazer upload
                if st.button("Fazer Upload para MongoDB", type="primary"):
                    if collection_name:
                        with st.spinner("Fazendo upload dos dados..."):
                            success, result = upload_to_mongodb(df, collection_name)
                            if success:
                                message_container.success(f"""
                                    ✅ Upload concluído com sucesso!
                                    - Collection: {collection_name}
                                    - Registros inseridos: {result}
                                """)
                            else:
                                message_container.error(result)
                    else:
                        message_container.warning("⚠️ Por favor, digite um nome para a collection.")
            else:
                message_container.error("O arquivo está vazio!")
                
        except Exception as e:
            message_container.error(f"Erro ao processar o arquivo: {str(e)}")

if __name__ == "__main__":
    main()