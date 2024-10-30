import streamlit as st
import polars as pl
from pymongo import MongoClient
import urllib.parse
import unicodedata
import re
from bson.objectid import ObjectId


# Função para converter ObjectId para strings
def convert_objectid_to_str(documents):
    for document in documents:
        for key, value in document.items():
            if isinstance(value, ObjectId):
                document[key] = str(value)
    return documents

# Função para normalizar strings (remover acentos e caracteres especiais)
def normalize_string(text):
    text = unicodedata.normalize('NFKD', text)
    text = re.sub(r'[^\w\s]', '', text)  # Remove caracteres especiais
    return text

# Função para carregar a coleção MongoDB diretamente para um DataFrame Polars
@st.cache_data
def mongo_collection_to_polars(mongo_uri, db_name, collection_name):
    # Conectar ao MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # Obter todos os documentos da coleção
    documents = list(collection.find())

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

# Função para obter valores únicos das colunas, usando cache (com _ no argumento do DataFrame)
@st.cache_data
def get_unique_values(_df, column):
    return _df[column].unique().to_list()

# Informações de conexão
username = 'devpython86'
password = 'dD@pjl06081420'
cluster = 'cluster0.akbb8.mongodb.net'
db_name = 'warehouse'  # Nome do banco de dados
collection_name = 'xml'

# Escapar o nome de usuário e a senha
escaped_username = urllib.parse.quote_plus(username)
escaped_password = urllib.parse.quote_plus(password)

# Montar a string de conexão
MONGO_URI = f"mongodb+srv://{escaped_username}:{escaped_password}@{cluster}/{db_name}?retryWrites=true&w=majority"

# Configuração da página do Streamlit
st.set_page_config(
    page_title="Filtro de Dados Dinâmico",
    page_icon=":bar_chart:",
    layout="wide",
)

st.title('📊 Filtro de Dados MongoDB com Polars')

# Carregar os dados do MongoDB
with st.spinner("Carregando dados..."):
    polars_df = mongo_collection_to_polars(MONGO_URI, db_name, collection_name)

if polars_df.is_empty():
    st.error("Nenhum dado encontrado na coleção.")
else:
    st.success("Dados carregados com sucesso!")

    # Exibir as colunas disponíveis
    colunas = polars_df.columns
    st.write("### Passo 1: Escolha as colunas que deseja filtrar:")

    # Multi-select para escolher quais colunas filtrar
    colunas_selecionadas = st.multiselect(
        "Selecione as colunas para aplicar filtros:",
        colunas
    )

    # Se o usuário escolheu alguma coluna, mostrar as opções de filtro
    if colunas_selecionadas:
        st.write("### Passo 2: Defina os filtros para as colunas selecionadas:")

        # Dicionário para armazenar os filtros
        filtros = {}

        # Dividir as colunas em blocos de até 3 filtros por linha
        col_container = st.container()
        col_count = 0
        columns = col_container.columns(3)  # Até 3 colunas por linha

        # Iterar pelas colunas selecionadas e permitir que o usuário escolha como filtrar cada uma
        for idx, coluna in enumerate(colunas_selecionadas):
            # Se ultrapassar 3 colunas, criar uma nova linha de colunas
            if idx % 3 == 0 and idx != 0:
                columns = col_container.columns(3)

            # Obter o tipo da coluna no DataFrame
            tipo_coluna = polars_df[coluna].dtype

            # Dentro da coluna apropriada, exibir os filtros
            with columns[idx % 3]:  # Alternar entre colunas
                st.write(f"**Coluna: {coluna}**")

                # Se a coluna for string, permitir escolher entre Input e Multi-select
                if tipo_coluna == pl.Utf8:
                    # Permitir que o usuário escolha o tipo de filtro (input ou multi-select)
                    filtro_tipo = st.radio(
                        f"Escolha o tipo de filtro para a coluna `{coluna}`:",
                        options=["Input", "Multi-select"],
                        key=f"filtro_tipo_{coluna}",
                        horizontal=True
                    )

                    # Se for input e a coluna for do tipo string
                    if filtro_tipo == "Input":
                        valor_input = st.text_input(f"Digite o valor para filtrar na coluna `{coluna}`:", key=f"input_{coluna}")
                        if valor_input:
                            filtros[coluna] = ("input", valor_input)

                    # Se for multi-select, listar os valores únicos da coluna e permitir múltiplas seleções
                    elif filtro_tipo == "Multi-select":
                        try:
                            # Usar cache para obter os valores únicos da coluna
                            valores_unicos = get_unique_values(polars_df, coluna)
                            valores_selecionados = st.multiselect(f"Selecione os valores para a coluna `{coluna}`:", valores_unicos, key=f"multi_{coluna}")
                            if valores_selecionados:
                                filtros[coluna] = ("multi", valores_selecionados)
                        except Exception as e:
                            st.error(f"Erro ao obter valores únicos para a coluna {coluna}: {e}")
                
                # Se a coluna for numérica, apenas permitir multi-select
                elif tipo_coluna in [pl.Int64, pl.Float64]:
                    try:
                        valores_unicos = get_unique_values(polars_df, coluna)
                        valores_selecionados = st.multiselect(f"Selecione os valores para a coluna `{coluna}`:", valores_unicos, key=f"multi_{coluna}")
                        if valores_selecionados:
                            filtros[coluna] = ("multi", valores_selecionados)
                    except Exception as e:
                        st.error(f"Erro ao obter valores únicos para a coluna {coluna}: {e}")

        # Botão para aplicar os filtros
        if st.button("Aplicar Filtros"):
            # Aplicar os filtros no DataFrame
            df_filtrado = polars_df

            for coluna, (filtro_tipo, valor) in filtros.items():
                tipo_coluna = polars_df[coluna].dtype

                if filtro_tipo == "input" and tipo_coluna == pl.Utf8:
                    # Normalizar o valor de entrada
                    valor_normalizado = normalize_string(valor.strip().lower())
                    
                    # Construir um padrão regex para garantir que todas as palavras estejam presentes (sem acentos, case insensitive)
                    palavras = valor_normalizado.split()
                    pattern = ".*" + ".*".join(palavras) + ".*"  # Padrão regex para buscar as palavras em qualquer lugar

                    # Normalizar a coluna e aplicar o filtro de regex
                    df_filtrado = df_filtrado.with_columns([
                        pl.col(coluna).str.to_lowercase().str.replace_all(r'[^\w\s]', '', literal=False).alias(f"{coluna}_normalized")
                    ])
                    df_filtrado = df_filtrado.filter(
                        pl.col(f"{coluna}_normalized").str.contains(pattern)
                    )
                
                elif filtro_tipo == "multi":
                    # Aplicar filtro por multi-select (um dos valores selecionados)
                    df_filtrado = df_filtrado.filter(pl.col(coluna).is_in(valor))

            # Verificar se ainda há dados após os filtros
            if df_filtrado.is_empty():
                st.warning("Nenhum dado encontrado com os filtros aplicados.")
            else:
                st.write("### Resultados Filtrados")
                st.dataframe(df_filtrado.to_pandas())  # Converter para Pandas e exibir no Streamlit
