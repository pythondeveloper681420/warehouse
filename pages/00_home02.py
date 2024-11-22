import streamlit as st
import polars as pl
from pymongo import MongoClient
import urllib.parse
import unicodedata
import re
from bson.objectid import ObjectId
import math
from datetime import datetime

def normalize_string(text):
    """Normalize string for case-insensitive search"""
    if not isinstance(text, str):
        return str(text)
    text = unicodedata.normalize('NFKD', text)
    text = re.sub(r'[^\w\s]', '', text)
    return text.lower()

def convert_document_for_polars(docs):
    """Convert MongoDB documents to Polars-compatible format"""
    if not docs:
        return []
    
    all_keys = set()
    for doc in docs:
        all_keys.update(doc.keys())
    
    converted_docs = []
    for doc in docs:
        converted_doc = {}
        for key in all_keys:
            value = doc.get(key)
            
            if isinstance(value, ObjectId):
                converted_doc[key] = str(value)
            elif isinstance(value, (dict, list)):
                converted_doc[key] = str(value)
            elif value is None:
                converted_doc[key] = None
            else:
                converted_doc[key] = str(value)
        
        converted_docs.append(converted_doc)
    
    return converted_docs

@st.cache_data(ttl=300)
def get_collection_stats(mongo_uri, db_name, collection_name, filters=None):
    """Get collection statistics with optional filters"""
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
        query = filters if filters else {}
        return collection.count_documents(query)
    except Exception as e:
        st.error(f"Erro ao obter estatísticas: {str(e)}")
        return 0

@st.cache_data(ttl=300)
def get_unique_values(mongo_uri, db_name, collection_name, field):
    """Get unique values for a field"""
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
        return list(collection.distinct(field))
    except Exception as e:
        st.error(f"Erro ao obter valores únicos: {str(e)}")
        return []

def load_paginated_data(mongo_uri, db_name, collection_name, skip, limit, filters=None):
    """Load paginated data from MongoDB with optional filters"""
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
        
        query = filters if filters else {}
        
        documents = list(collection.find(query).skip(skip).limit(limit))
        
        if documents:
            converted_docs = convert_document_for_polars(documents)
            df = pl.DataFrame(
                converted_docs,
                schema_overrides={col: pl.String for col in converted_docs[0].keys()},
                infer_schema_length=None
            )
            return df
        else:
            return pl.DataFrame()
            
    except Exception as e:
        st.error(f"Erro ao carregar dados: {str(e)}")
        return pl.DataFrame()

def create_mongodb_filter(column, value, filter_type="text"):
    """Create MongoDB filter based on filter type"""
    if filter_type == "text":
        return {column: {"$regex": value, "$options": "i"}}
    elif filter_type == "multi":
        return {column: {"$in": value}}
    return {}

def create_filters(collection_name, mongo_uri, db_name):
    """Create filter interface with MongoDB integration"""
    applied_filters = {}
    
    with st.container():
        st.subheader("🔍 Filtros")
        
        try:
            client = MongoClient(mongo_uri)
            db = client[db_name]
            collection = db[collection_name]
            sample_doc = collection.find_one()
            
            if sample_doc:
                columns = list(sample_doc.keys())
                selected_columns = st.multiselect(
                    "Selecione as colunas:",
                    columns,
                    key=f"col_select_{collection_name}"
                )
                
                for column in selected_columns:
                    st.write(f"**{column}**")
                    
                    filter_type = st.radio(
                        f"Tipo de filtro para {column}",
                        ["Texto", "Seleção Múltipla"],
                        key=f"filter_type_{collection_name}_{column}",
                        horizontal=True
                    )
                    
                    if filter_type == "Texto":
                        search_text = st.text_input(
                            "Digite para buscar:",
                            key=f"text_{collection_name}_{column}"
                        )
                        if search_text:
                            applied_filters.update(
                                create_mongodb_filter(column, search_text, "text")
                            )
                    else:
                        unique_values = get_unique_values(
                            mongo_uri, db_name, collection_name, column
                        )
                        selected_values = st.multiselect(
                            "Selecione os valores:",
                            unique_values,
                            key=f"multi_{collection_name}_{column}"
                        )
                        if selected_values:
                            applied_filters.update(
                                create_mongodb_filter(column, selected_values, "multi")
                            )
                    
                    st.divider()
                    
        except Exception as e:
            st.error(f"Erro ao criar filtros: {str(e)}")
    
    return applied_filters

def handle_pagination(total_records, records_per_page, key_prefix):
    """Handle pagination controls and state"""
    total_pages = math.ceil(total_records / records_per_page)
    
    # Initialize or update page state
    if f"{key_prefix}_page" not in st.session_state:
        st.session_state[f"{key_prefix}_page"] = 1
    
    current_page = st.session_state[f"{key_prefix}_page"]
    
    # Ensure current page is within valid range
    current_page = max(1, min(current_page, total_pages))
    
    col1, col2, col3, col4 = st.columns([2, 1, 1, 2])
    
    with col1:
        if st.button("⏮️", key=f"{key_prefix}_first", disabled=current_page == 1):
            st.session_state[f"{key_prefix}_page"] = 1
            st.rerun()
            
    with col2:
        if st.button("◀️", key=f"{key_prefix}_prev", disabled=current_page == 1):
            st.session_state[f"{key_prefix}_page"] = current_page - 1
            st.rerun()
    
    with col3:
        if st.button("▶️", key=f"{key_prefix}_next", disabled=current_page == total_pages):
            st.session_state[f"{key_prefix}_page"] = current_page + 1
            st.rerun()
            
    with col4:
        if st.button("⏭️", key=f"{key_prefix}_last", disabled=current_page == total_pages):
            st.session_state[f"{key_prefix}_page"] = total_pages
            st.rerun()

    st.write(f"Página {current_page} de {total_pages}")
    
    # Calculate slice indexes
    start_idx = (current_page - 1) * records_per_page
    
    return start_idx, records_per_page

def main():
    st.set_page_config(
        page_title="Dashboard MongoDB",
        page_icon="📊",
        layout="wide"
    )

    st.title("📊 Dashboard MongoDB")

    # Configurações
    username = 'devpython86'
    password = 'dD@pjl06081420'
    cluster = 'cluster0.akbb8.mongodb.net'
    db_name = 'warehouse'
    collections = ['xml', 'po', 'nfspdf']
    RECORDS_PER_PAGE = 50

    escaped_username = urllib.parse.quote_plus(username)
    escaped_password = urllib.parse.quote_plus(password)
    MONGO_URI = f"mongodb+srv://{escaped_username}:{escaped_password}@{cluster}/{db_name}?retryWrites=true&w=majority"

    tabs = st.tabs([collection.upper() for collection in collections])

    for tab, collection_name in zip(tabs, collections):
        with tab:
            col1, col2 = st.columns([1, 2])
            
            with col1:
                # Create filters
                filters = create_filters(collection_name, MONGO_URI, db_name)
            
            with col2:
                st.subheader("📋 Dados")
                
                # Get filtered record count
                total_records = get_collection_stats(MONGO_URI, db_name, collection_name, filters)
                
                if total_records > 0:
                    st.success(f"✅ Total de registros: {total_records}")
                    
                    # Handle pagination and get current page slice
                    skip, limit = handle_pagination(
                        total_records, 
                        RECORDS_PER_PAGE, 
                        f"pagination_{collection_name}"
                    )
                    
                    # Load paginated data with filters
                    with st.spinner("Carregando dados..."):
                        df = load_paginated_data(
                            MONGO_URI, 
                            db_name, 
                            collection_name,
                            skip,
                            limit,
                            filters
                        )
                        
                        if not df.is_empty():
                            st.dataframe(
                                df.to_pandas(),
                                use_container_width=True,
                                #height=600
                            )
                        else:
                            st.warning("Nenhum resultado encontrado.")
                else:
                    st.error(f"Não foi possível carregar dados da coleção {collection_name}")

    st.divider()
    st.caption("Dashboard de Dados MongoDB v1.0 (Polars)")

if __name__ == "__main__":
    main()