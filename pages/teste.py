import streamlit as st
import polars as pl
import pymongo
import urllib.parse

# Definindo as credenciais de conexão
MONGO_USERNAME = urllib.parse.quote_plus('devpython86')
MONGO_PASSWORD = urllib.parse.quote_plus('dD@pjl06081420')
MONGO_CLUSTER = 'cluster0.akbb8.mongodb.net'
MONGO_DB = 'warehouse'

# Conectando ao MongoDB
client = pymongo.MongoClient(
    f"mongodb+srv://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_CLUSTER}/{MONGO_DB}?retryWrites=true&w=majority"
)
db = client[MONGO_DB]
collection = db['xml']  # Altere para o nome da sua coleção

# Função para carregar dados com paginação
def load_data(page_size=100, page_num=0, filtro=None):
    query = filtro if filtro else {}
    cursor = collection.find(query, projection={'_id': False}).skip(page_size * page_num).limit(page_size)
    data = list(cursor)
    if data:
        df = pl.DataFrame(data)  # Converter para DataFrame Polars
        return df
    return pl.DataFrame()  # Retorna um DataFrame vazio se não houver dados

# Aplicativo Streamlit
st.set_page_config(layout="wide")
st.title("Aplicativo MongoDB com Streamlit e Polars")

# Input de filtro
filtro_input = st.text_area("Digite um filtro (JSON)", value="{}", height=100)
try:
    filtro = eval(filtro_input) if filtro_input else None
except:
    st.error("Formato de filtro inválido. Por favor, verifique o JSON.")
    filtro = None

# Configurar paginação
col1, col2 = st.columns(2)
with col1:
    page_size = st.number_input("Tamanho da página", min_value=10, max_value=1000, value=100, step=10)
with col2:
    page_num = st.number_input("Número da página", min_value=0, step=1, value=0)

# Carregar dados ao clicar no botão
if st.button("Carregar Dados"):
    data = load_data(page_size=page_size, page_num=page_num, filtro=filtro)
    if not data.height == 0:
        st.write(data)

        # Exibir número total de documentos
        total_count = collection.count_documents(filtro if filtro else {})
        st.write(f"Total de documentos: {total_count}")
        st.write(f"Exibindo {page_size} registros na página {page_num + 1}.")

        # Controles de navegação
        col3, col4, col5 = st.columns(3)
        with col3:
            if st.button("Página Anterior") and page_num > 0:
                page_num -= 1
        with col4:
            st.write("")
        with col5:
            if st.button("Próxima Página") and page_size * (page_num + 1) < total_count:
                page_num += 1
    else:
        st.write("Nenhum dado encontrado.")