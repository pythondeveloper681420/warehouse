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
            max-width: 1200px;
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
        .duplicates-warning {
            padding: 1rem;
            background-color: #fff3cd;
            border: 1px solid #ffeeba;
            border-radius: 5px;
            margin: 1rem 0;
        }
    </style>
""", unsafe_allow_html=True)

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

def check_mongodb_duplicates(db, collection_name, df, check_column):
    """Verifica duplicatas entre o DataFrame e a collection do MongoDB"""
    try:
        collection = db[collection_name]
        # Obtem valores únicos da coluna no DataFrame
        df_values = set(df[check_column].astype(str).unique())
        
        # Busca documentos no MongoDB que têm valores matching
        mongo_docs = collection.find({check_column: {"$in": list(df_values)}}, {check_column: 1})
        mongo_values = set(str(doc[check_column]) for doc in mongo_docs)
        
        # Encontra as interseções
        duplicates = df_values.intersection(mongo_values)
        
        return len(duplicates), list(duplicates)
    except Exception as e:
        st.error(f"Erro ao verificar duplicatas no MongoDB: {e}")
        return 0, []

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

def check_df_duplicates(df, column):
    """Verifica e retorna informações sobre duplicatas em uma coluna do DataFrame"""
    duplicates = df[df[column].duplicated()][column].values
    total_duplicates = len(duplicates)
    unique_duplicates = len(set(duplicates))
    return total_duplicates, unique_duplicates, duplicates

def main():
    # Cabeçalho
    st.title("🚀 Processador MongoDB Pro")
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
                    # Seleção de coluna para verificação de duplicatas
                    check_column = st.selectbox(
                        "📍 Selecione a coluna para verificar duplicatas",
                        options=df.columns,
                        help="Os registros serão filtrados com base nos valores únicos desta coluna"
                    )
                    
                    # Verifica duplicatas no DataFrame
                    total_df_duplicates, unique_df_duplicates, df_duplicate_values = check_df_duplicates(df, check_column)
                    
                    # Remove duplicatas do DataFrame
                    df_unique = df.drop_duplicates(subset=[check_column])
                    total_df_removed = len(df) - len(df_unique)
                    
                    # Verifica duplicatas no MongoDB
                    db = connect_mongodb()
                    if db is not None:
                        mongo_duplicates_count, mongo_duplicate_values = check_mongodb_duplicates(
                            db, collection_name, df_unique, check_column
                        )
                        
                        # Remove registros que já existem no MongoDB
                        if mongo_duplicates_count > 0:
                            df_unique = df_unique[~df_unique[check_column].astype(str).isin(mongo_duplicate_values)]
                    
                    # Mostra informações sobre duplicatas
                    if total_df_duplicates > 0 or mongo_duplicates_count > 0:
                        st.warning(f"""
                            ⚠️ Análise de Duplicatas na coluna "{check_column}":
                            • {total_df_removed} registros duplicados no arquivo Excel
                            • {mongo_duplicates_count} registros já existentes no MongoDB
                            • Total de registros únicos para upload: {len(df_unique)}
                        """)
                        
                        # Expander para valores duplicados no Excel
                        if total_df_duplicates > 0:
                            with st.expander("👀 Ver duplicatas no arquivo Excel"):
                                st.write(sorted(list(set(df_duplicate_values))))
                        
                        # Expander para valores duplicados no MongoDB
                        if mongo_duplicates_count > 0:
                            with st.expander("🔍 Ver registros já existentes no MongoDB"):
                                st.write(sorted(mongo_duplicate_values))
                    
                    # Visualização dos dados após remoção de duplicatas
                    with st.expander("📊 Visualização dos Dados (Após remoção de duplicatas)", expanded=True):
                        st.dataframe(
                            df_unique.head(),
                            use_container_width=True,
                            hide_index=True
                        )
                    
                    # Informações dos dados em colunas
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Registros para Upload", len(df_unique))
                    with col2:
                        st.metric("Duplicatas Excel", total_df_removed)
                    with col3:
                        st.metric("Duplicatas MongoDB", mongo_duplicates_count)
                    with col4:
                        st.metric("Total de Colunas", len(df_unique.columns))
                    
                    # Tipos de dados em um expander
                    with st.expander("📋 Tipos de Colunas"):
                        df_types = pd.DataFrame({
                            'Coluna': df_unique.columns,
                            'Tipo': df_unique.dtypes.values.astype(str)
                        })
                        st.dataframe(df_types, use_container_width=True, hide_index=True)
                    
                    # Botão de upload (apenas se houver registros únicos para enviar)
                    if collection_name and len(df_unique) > 0:
                        if st.button("📤 Enviar para MongoDB", type="primary", use_container_width=True):
                            with st.spinner("Processando upload..."):
                                success, result = upload_to_mongodb(df_unique, collection_name)
                                if success:
                                    message_container.success(f"""
                                        ✅ Upload Concluído com Sucesso!
                                        • Coleção: {collection_name}
                                        • Novos Registros Inseridos: {result}
                                        • Duplicatas Excel Removidas: {total_df_removed}
                                        • Duplicatas MongoDB Ignoradas: {mongo_duplicates_count}
                                    """)
                                else:
                                    message_container.error(result)
                    elif len(df_unique) == 0:
                        st.warning("⚠️ Não há registros únicos para enviar. Todos os registros já existem no MongoDB.")
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