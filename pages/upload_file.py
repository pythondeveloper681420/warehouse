import streamlit as st
import pandas as pd
from pymongo import MongoClient
import urllib.parse
import numpy as np
from datetime import datetime, time

# Configuração da página
st.set_page_config(
    page_title="Processador MongoDB Pro",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# CSS personalizado para visual moderno
st.markdown("""
    <style>
        .stApp {
            margin: 0 auto;
            padding: 1rem;
        }
        .main > div {
            padding: 2rem;
            border-radius: 10px;
            background: white;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .st-emotion-cache-1y4p8pa {
            padding: 2rem;
            border-radius: 10px;
        }
        .st-emotion-cache-1v0mbdj {
            margin-top: 1rem;
        }
    </style>
""", unsafe_allow_html=True)
#st.title
# Configuração do MongoDB
USERNAME = urllib.parse.quote_plus('devpython86')
PASSWORD = urllib.parse.quote_plus('dD@pjl06081420')
CLUSTER = 'cluster0.akbb8.mongodb.net'
DB_NAME = 'warehouse'

def handle_date(value):
    """Função para tratar datas e horários"""
    if pd.isna(value) or pd.isnull(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(value, time):
        return value.strftime('%H:%M:%S')
    return value

def clean_dataframe(df):
    """Limpa e prepara o DataFrame para inserção no MongoDB"""
    df_clean = df.copy()
    
    for column in df_clean.columns:
        if df_clean[column].dtype in ['datetime64[ns]', 'datetime64[ns, UTC]']:
            df_clean[column] = df_clean[column].apply(handle_date)
        else:
            df_clean[column] = df_clean[column].apply(lambda x: 
                None if pd.isna(x) 
                else int(x) if isinstance(x, np.integer)
                else float(x) if isinstance(x, np.floating)
                else bool(x) if isinstance(x, np.bool_)
                else x.strftime('%H:%M:%S') if isinstance(x, time)
                else str(x) if isinstance(x, np.datetime64)
                else x
            )
    
    return df_clean

def connect_mongodb():
    """Conecta ao MongoDB"""
    try:
        connection_string = f"mongodb+srv://{USERNAME}:{PASSWORD}@{CLUSTER}/?retryWrites=true&w=majority"
        client = MongoClient(connection_string)
        client.server_info()
        return client[DB_NAME]
    except Exception as e:
        st.error(f"Erro de conexão com MongoDB: {e}")
        return None

def upload_to_mongodb(df, collection_name):
    """Faz upload do DataFrame para o MongoDB"""
    db = connect_mongodb()
    if db is not None:
        try:
            df_clean = clean_dataframe(df)
            records = df_clean.to_dict('records')
            collection = db[collection_name]
            result = collection.insert_many(records)
            return True, len(result.inserted_ids)
        except Exception as e:
            return False, f"Erro no upload: {e}"
    return False, "Erro de conexão com MongoDB"

def main():
    # Cabeçalho
    st.header("🚀 Processador MongoDB Pro")
    st.markdown("Faça upload de seus dados Excel para o MongoDB com facilidade")
    
    # Container principal
    with st.container():
        # Upload de arquivo e nome da collection em colunas
        col1, col2 = st.columns([2, 1])
        
        with col1:
            uploaded_file = st.file_uploader(
                "📂 Selecione o Arquivo Excel",
                type=['xlsx', 'xls'],
                help="Suporte para arquivos .xlsx e .xls"
            )
            
        with col2:
            collection_name = st.text_input(
                "Nome da Coleção",
                placeholder="Digite o nome da coleção",
                help="Nome para sua coleção no MongoDB"
            ).strip()

        # Container para mensagens
        message_container = st.empty()

        if uploaded_file is not None:
            try:
                df = pd.read_excel(uploaded_file)
                
                if not df.empty:
                    # Visualização dos dados
                    with st.expander("📊 Visualização dos Dados", expanded=False):
                        st.dataframe(
                            df.head(),
                            use_container_width=True,
                            hide_index=True
                        )
                    
                    # Informações dos dados em colunas
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total de Linhas", len(df))
                    with col2:
                        st.metric("Total de Colunas", len(df.columns))
                    with col3:
                        st.metric("Tamanho do Arquivo", f"{uploaded_file.size / 1024:.1f} KB")
                    
                    # Tipos de dados em um expander
                    with st.expander("📋 Tipos de Colunas"):
                        df_types = pd.DataFrame({
                            'Coluna': df.columns,
                            'Tipo': df.dtypes.values.astype(str)
                        })
                        st.dataframe(df_types, use_container_width=True, hide_index=True)
                    
                    # Botão de upload
                    if collection_name:
                        if st.button("📤 Enviar para MongoDB", type="primary", use_container_width=True):
                            with st.spinner("Processando upload..."):
                                success, result = upload_to_mongodb(df, collection_name)
                                if success:
                                    message_container.success(f"""
                                        ✅ Upload Concluído com Sucesso!
                                        • Coleção: {collection_name}
                                        • Registros Inseridos: {result}
                                    """)
                                else:
                                    message_container.error(result)
                    else:
                        st.info("👆 Por favor, insira um nome para a coleção para prosseguir", icon="ℹ️")
                else:
                    st.warning("⚠️ O arquivo enviado está vazio!")
                    
            except Exception as e:
                st.error(f"Erro ao processar arquivo: {str(e)}")
    
    # Rodapé
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center'>
            <p style='color: #888;'>Desenvolvido com ❤️ | Processador MongoDB Pro v1.0</p>
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()