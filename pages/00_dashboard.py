import streamlit as st
import pandas as pd
import polars as pl
from pymongo import MongoClient
import urllib.parse
from bson.objectid import ObjectId

# Função para converter ObjectId para strings
def convert_objectid_to_str(documents):
    for document in documents:
        for key, value in document.items():
            if isinstance(value, ObjectId):
                document[key] = str(value)
    return documents

# Função para realizar join entre collections
@st.cache_data
def mongo_collection_join(mongo_uri, db_name, collection_po, collection_xml):
    # Conectar ao MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection_po_db = db[collection_po]
    collection_xml_db = db[collection_xml]

    # Pipeline de agregação para join
    join_pipeline = [
        # Estágio de junção (lookup)
        {
            "$lookup": {
                "from": collection_po,  # Nome da collection de Purchase Orders
                "localField": "PO",  # Campo na collection XML
                "foreignField": "Purchasing Document",  # Campo na collection PO
                "as": "po_details"
            }
        },
        # Desenredar os resultados do lookup
        {
            "$unwind": {
                "path": "$po_details",
                "preserveNullAndEmptyArrays": True  # Manter documentos XML mesmo sem correspondência em PO
            }
        },
        # Projeção para incluir TODOS os campos
        {
            "$project": {
                # Campos da collection XML
                **{k: 1 for k in collection_xml_db.find_one().keys()},
                # Campos da collection PO prefixados
                **{f"po_{k}": f"$po_details.{k}" for k in collection_po_db.find_one().keys()}
            }
        }
    ]

    # Executar o pipeline de agregação
    documents = list(collection_xml_db.aggregate(join_pipeline))

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

# Configuração do Streamlit
st.set_page_config(
    page_title="Dashboard MongoDB",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# CSS para personalização
st.markdown("""
    <style>
    .main {overflow: auto;}
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stApp [data-testid="stToolbar"] {display: none;}
    .stDeployButton {display: none;}
    #stDecoration {display: none;}
    [data-testid="collapsedControl"] {display: none;}
    </style>
""", unsafe_allow_html=True)

# Título principal
st.header("📊 Dashboard de Documentos MongoDB")

# Configurações de conexão do MongoDB
try:
    # Usar secrets do Streamlit para credenciais
    username = st.secrets["MONGO_USERNAME"]
    password = st.secrets["MONGO_PASSWORD"]
    cluster = st.secrets["MONGO_CLUSTER"]
    db_name = st.secrets["MONGO_DB"]

    # Escapar credenciais
    escaped_username = urllib.parse.quote_plus(username)
    escaped_password = urllib.parse.quote_plus(password)

    # Montar string de conexão
    MONGO_URI = f"mongodb+srv://{escaped_username}:{escaped_password}@{cluster}/{db_name}?retryWrites=true&w=majority"

    # Coleções
    collection_po = 'po'
    collection_xml = 'xml'

    # Carregamento dos dados
    with st.spinner("Carregando dados..."):
        # Realizar join entre as collections
        polars_join = mongo_collection_join(
            MONGO_URI, 
            db_name, 
            collection_po, 
            collection_xml
        )

        # Renderização dos dados
        if not polars_join.is_empty():
            # Título da seção de dados
            st.subheader("Dados Combinados")

            po_polars = polars_join.to_pandas()
                        # Remove duplicates based on the slugified unique column
            po_polars.drop_duplicates(subset=['po_Purchasing Document'], inplace=True)

            # No bloco de renderização
            st.dataframe(
                po_polars, 
                use_container_width=True,
                hide_index=True,
                height=600  # Altura ajustável
            )

            # Informações detalhadas
            st.write("Número total:", len(po_polars))
            st.write("Número total:", len(po_polars))
            st.write("Número total de colunas:", len(po_polars.columns))
            st.write("Colunas:", list(po_polars.columns))

        else:
            st.warning("Nenhum documento encontrado.")

except Exception as e:
    st.error(f"Erro na conexão ou processamento: {e}")