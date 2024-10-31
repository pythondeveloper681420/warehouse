import streamlit as st
import pandas as pd
from pymongo import MongoClient
import io

# Configuração da página Streamlit
st.set_page_config(page_title="Upload Excel para MongoDB", layout="centered")

def connect_mongodb():
    """Função para conectar ao MongoDB"""
    connection_string = f"mongodb+srv://devpython86:dD@pjl06081420@cluster0.akbb8.mongodb.net/?retryWrites=true&w=majority"
    try:
        client = MongoClient(connection_string)
        db = client['warehouse']
        return db
    except Exception as e:
        st.error(f"Erro ao conectar ao MongoDB: {e}")
        return None

def upload_to_mongodb(df, collection_name):
    """Função para fazer upload do DataFrame para o MongoDB"""
    db = connect_mongodb()
    if db is not None:
        try:
            collection = db[collection_name]
            records = df.to_dict('records')
            collection.insert_many(records)
            return True
        except Exception as e:
            st.error(f"Erro ao fazer upload dos dados: {e}")
            return False
    return False

def main():
    st.title("Upload de Excel para MongoDB")
    
    # Campo para nome da collection
    collection_name = st.text_input("Nome da Collection", 
                                  placeholder="Digite o nome da collection")
    
    # Upload do arquivo Excel
    uploaded_file = st.file_uploader("Escolha um arquivo Excel", 
                                   type=['xlsx', 'xls'])
    
    if uploaded_file is not None:
        try:
            # Lê o arquivo Excel
            df = pd.read_excel(uploaded_file)
            
            # Mostra preview dos dados
            st.subheader("Preview dos dados")
            st.dataframe(df.head())
            
            # Informações sobre o DataFrame
            st.subheader("Informações do DataFrame")
            st.write(f"Número de linhas: {df.shape[0]}")
            st.write(f"Número de colunas: {df.shape[1]}")
            
            # Botão para fazer upload
            if st.button("Fazer Upload para MongoDB"):
                if collection_name:
                    with st.spinner("Fazendo upload dos dados..."):
                        if upload_to_mongodb(df, collection_name):
                            st.success(f"Dados enviados com sucesso para a collection '{collection_name}'!")
                            
                            # Mostrar estatísticas básicas
                            st.subheader("Upload Concluído")
                            st.write(f"Total de registros enviados: {len(df)}")
                else:
                    st.warning("Por favor, digite um nome para a collection.")
                    
        except Exception as e:
            st.error(f"Erro ao processar o arquivo: {e}")

if __name__ == "__main__":
    main()