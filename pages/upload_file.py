import streamlit as st
import pandas as pd
from pymongo import MongoClient, errors
import urllib.parse
import numpy as np
from datetime import datetime, time
import time as time_module
from contextlib import contextmanager

# Configuração da página
st.set_page_config(
    page_title="Processador MongoDB Pro",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

hide_streamlit_style = """
<style>
.main {
    overflow: auto;
}
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
.stApp [data-testid="stToolbar"]{
display:none;
.reportview-container {
    margin-top: -2em;
}
.stDeployButton {display:none;}
#stDecoration {display:none;}    
</style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True) 

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

# Configuração do MongoDB com retry
USERNAME = urllib.parse.quote_plus('devpython86')
PASSWORD = urllib.parse.quote_plus('dD@pjl06081420')
CLUSTER = 'cluster0.akbb8.mongodb.net'
DB_NAME = 'warehouse'
MAX_RETRIES = 3
RETRY_DELAY = 2

@contextmanager
def mongodb_connection():
    """Context manager para conexão MongoDB com retry"""
    client = None
    for attempt in range(MAX_RETRIES):
        try:
            connection_string = f"mongodb+srv://{USERNAME}:{PASSWORD}@{CLUSTER}/?retryWrites=true&w=majority&connectTimeoutMS=30000&socketTimeoutMS=30000"
            client = MongoClient(connection_string, serverSelectionTimeoutMS=30000)
            # Verifica a conexão
            client.server_info()
            yield client[DB_NAME]
            break
        except errors.ServerSelectionTimeoutError:
            if attempt == MAX_RETRIES - 1:
                raise
            time_module.sleep(RETRY_DELAY)
        except Exception as e:
            raise
        finally:
            if client:
                client.close()

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

def upload_to_mongodb(df, collection_name):
    """Faz upload do DataFrame para o MongoDB com retry"""
    try:
        with mongodb_connection() as db:
            df_clean = clean_dataframe(df)
            records = df_clean.to_dict('records')
            collection = db[collection_name]
            
            # Upload em lotes para evitar timeout
            batch_size = 1000
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                result = collection.insert_many(batch, ordered=False)
            
            return True, len(records)
    except errors.ServerSelectionTimeoutError:
        return False, "Erro de timeout na conexão com MongoDB. Por favor, tente novamente."
    except Exception as e:
        return False, f"Erro no upload: {str(e)}"

def get_collection_fields(collection_name):
    """Retorna os campos disponíveis em uma collection"""
    try:
        with mongodb_connection() as db:
            collection = db[collection_name]
            sample_doc = collection.find_one()
            if sample_doc:
                return list(sample_doc.keys())
            return []
    except Exception as e:
        st.error(f"Erro ao obter campos: {str(e)}")
        return []

def fast_remove_duplicates(collection_name, field_name):
    """Remove duplicatas de forma rápida usando pandas"""
    try:
        with mongodb_connection() as db:
            collection = db[collection_name]
            total_before = collection.count_documents({})
            
            # Converte collection para DataFrame em lotes
            batch_size = 10000
            df_list = []
            for batch in collection.find().batch_size(batch_size):
                df_list.append(pd.DataFrame([batch]))
            
            if not df_list:
                return False, "Collection vazia", 0
                
            df = pd.concat(df_list, ignore_index=True)
            
            # Remove duplicatas mantendo o primeiro registro
            df_clean = df.drop_duplicates(subset=field_name, keep='first')
            removed_count = len(df) - len(df_clean)
            
            if removed_count > 0:
                # Limpa a collection e insere os dados limpos em lotes
                collection.delete_many({})
                clean_data = df_clean.to_dict('records')
                
                for i in range(0, len(clean_data), batch_size):
                    batch = clean_data[i:i + batch_size]
                    collection.insert_many(batch)
                
            return True, removed_count, len(df_clean)
            
    except Exception as e:
        return False, str(e), 0

def batch_remove_duplicates(collection_name, field_name, batch_size=1000):
    """Remove duplicatas em lotes para coleções muito grandes"""
    try:
        with mongodb_connection() as db:
            collection = db[collection_name]
            total_before = collection.count_documents({})
            
            # Cria índice para o campo de agrupamento
            collection.create_index(field_name)
            
            # Processa em lotes
            unique_values = set()
            duplicates_removed = 0
            
            cursor = collection.find().batch_size(batch_size)
            for batch in cursor:
                value = batch.get(field_name)
                if value in unique_values:
                    collection.delete_one({'_id': batch['_id']})
                    duplicates_removed += 1
                else:
                    unique_values.add(value)
            
            total_after = collection.count_documents({})
            return True, duplicates_removed, total_after
                
    except Exception as e:
        return False, str(e), 0

def main():
    # Cabeçalho
    st.header("🚀 Processador MongoDB Pro")
    st.markdown("Faça upload de seus dados Excel para o MongoDB com facilidade")
    
    # Container principal
    with st.container():
        # Tabs para separar upload e limpeza
        tab1, tab2, tab3 = st.tabs(["📤 Upload de Dados", "🧹 Limpeza de Dados", " ❓Como Utilizar"])
        
        # Tab de Upload
        with tab1:
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
        
        # Tab de Limpeza
        with tab2:
            st.subheader("🧹 Limpeza de Duplicatas")
            
            # Seleção da collection e campo para limpeza
            clean_collection = st.text_input(
                "Nome da Coleção para Limpeza",
                placeholder="Digite o nome da coleção",
                help="Nome da coleção para remover duplicatas"
            ).strip()
            
            if clean_collection:
                fields = get_collection_fields(clean_collection)
                if fields:
                    selected_field = st.selectbox(
                        "Selecione o campo para identificar duplicatas",
                        options=fields,
                        help="Os documentos serão considerados duplicados se tiverem o mesmo valor neste campo"
                    )
                    
                    # Opção para escolher o método de limpeza
                    cleaning_method = st.radio(
                        "Método de Limpeza",
                        ["Rápido (Memória)", "Em Lotes (Menor uso de memória)"],
                        help="Escolha o método baseado no tamanho da sua collection"
                    )
                    
                    if st.button("🧹 Remover Duplicatas", type="primary", use_container_width=True):
                        with st.spinner("Removendo duplicatas..."):
                            if cleaning_method == "Rápido (Memória)":
                                success, removed_count, remaining_count = fast_remove_duplicates(
                                    clean_collection, selected_field
                                )
                            else:
                                success, removed_count, remaining_count = batch_remove_duplicates(
                                    clean_collection, selected_field
                                )
                                
                            if success:
                                st.success(f"""
                                    ✅ Limpeza Concluída com Sucesso!
                                    • Documentos removidos: {removed_count}
                                    • Documentos restantes: {remaining_count}
                                """)
                            else:
                                st.error(f"Erro ao remover duplicatas: {removed_count}")
                else:
                    st.warning("⚠️ Nenhum campo encontrado na coleção ou coleção vazia!")
            else:
                st.info("👆 Por favor, insira o nome da coleção para prosseguir", icon="ℹ️")
        with tab3:
                st.subheader("📖 Guia de Utilização")
                
                # Seção de Upload de Dados
                st.markdown("### 📤 Upload de Dados")
                st.markdown("""
                1. **Preparação do Arquivo**:
                - Prepare seu arquivo Excel (.xlsx ou .xls)
                - Certifique-se de que os dados estejam organizados em colunas
                - Verifique se não há caracteres especiais nos cabeçalhos
                
                2. **Processo de Upload**:
                - Clique no botão "Browse files" para selecionar seu arquivo
                - Digite um nome para sua coleção no MongoDB
                - Verifique a prévia dos dados exibida
                - Confirme os tipos de dados das colunas
                - Clique em "Enviar para MongoDB" para iniciar o upload
                
                3. **Verificação**:
                - Aguarde a mensagem de confirmação
                - Verifique o número de registros inseridos
                - Em caso de erro, leia a mensagem de erro para orientações
                """)
                
                # Seção de Limpeza de Dados
                st.markdown("### 🧹 Limpeza de Dados")
                st.markdown("""
                1. **Remoção de Duplicatas**:
                - Digite o nome da coleção que deseja limpar
                - Selecione o campo que será usado para identificar duplicatas
                - Escolha o método de limpeza:
                    * **Rápido**: Ideal para coleções menores (usa mais memória)
                    * **Em Lotes**: Recomendado para coleções grandes (mais lento, usa menos memória)
                
                2. **Processo de Limpeza**:
                - Confirme sua seleção
                - Clique em "Remover Duplicatas"
                - Aguarde o processo ser concluído
                - Verifique o número de documentos removidos e restantes
                """)
                
                # Seção de Dicas e Boas Práticas
                st.markdown("### 💡 Dicas e Boas Práticas")
                with st.expander("Expandir Dicas", expanded=False):
                    st.markdown("""
                    - **Preparação de Dados**:
                        * Limpe seus dados antes do upload
                        * Padronize os formatos de data
                        * Evite células vazias quando possível
                    
                    - **Performance**:
                        * Para arquivos grandes, prefira o upload em horários de menor uso
                        * Use o método de limpeza em lotes para grandes volumes de dados
                        * Mantenha backups antes de realizar limpezas
                    
                    - **Resolução de Problemas**:
                        * Em caso de timeout, tente novamente
                        * Verifique sua conexão com a internet
                        * Para erros persistentes, verifique o formato dos dados
                    """)
                
                # Seção de FAQ
                st.markdown("### ❓ Perguntas Frequentes")
                with st.expander("Expandir FAQ", expanded=False):
                    st.markdown("""
                    **P: Quais formatos de arquivo são aceitos?**  
                    R: Arquivos Excel (.xlsx e .xls)
                    
                    **P: Existe um limite de tamanho de arquivo?**  
                    R: Sim, o limite é determinado pela sua memória disponível e conexão
                    
                    **P: Como sei se meu upload foi bem-sucedido?**  
                    R: Uma mensagem de sucesso será exibida com o número de registros inseridos
                    
                    **P: Posso interromper um processo de limpeza?**  
                    R: Sim, você pode fechar a página, mas isso pode deixar dados parcialmente processados
                    
                    **P: Os dados são sobrescritos na limpeza?**  
                    R: Sim, ao remover duplicatas, os registros são atualizados permanentemente
                    """)
                
                # Seção de Contato/Suporte
                st.markdown("### 📞 Suporte")
                st.info("""
                Para suporte adicional ou relatar problemas:
                - Abra um ticket no sistema de suporte
                - Entre em contato com a equipe de desenvolvimento
                - Consulte a documentação técnica completa
                """)

    # Rodapé [Previous code remains the same...]
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