import streamlit as st
import pandas as pd
from pymongo import MongoClient
import urllib.parse
import unicodedata
import re
from bson.objectid import ObjectId
import io
import math

def normalize_string(text):
    if not isinstance(text, str):
        return str(text)
    text = unicodedata.normalize('NFKD', text)
    text = re.sub(r'[^\w\s]', '', text)
    return text.lower()

def convert_document_for_pandas(doc):
    converted_doc = {}
    for key, value in doc.items():
        if isinstance(value, ObjectId):
            converted_doc[key] = str(value)
        elif isinstance(value, dict):
            converted_doc[key] = convert_document_for_pandas(value)
        elif isinstance(value, list):
            converted_doc[key] = [str(item) if isinstance(item, ObjectId) else item for item in value]
        else:
            converted_doc[key] = value
    return converted_doc

@st.cache_resource
def get_mongodb_client():
    username = 'devpython86'
    password = 'dD@pjl06081420'
    cluster = 'cluster0.akbb8.mongodb.net'
    db_name = 'warehouse'
    
    escaped_username = urllib.parse.quote_plus(username)
    escaped_password = urllib.parse.quote_plus(password)
    MONGO_URI = f"mongodb+srv://{escaped_username}:{escaped_password}@{cluster}/{db_name}?retryWrites=true&w=majority"
    
    return MongoClient(MONGO_URI)

@st.cache_data
def get_collection_columns(collection_name):
    client = get_mongodb_client()
    db = client.warehouse
    collection = db[collection_name]
    
    total_docs = collection.count_documents({})
    sample_doc = collection.find_one()
    
    columns = []
    if sample_doc:
        columns = [col for col in sample_doc.keys() if col != '_id']
    
    return total_docs, columns

@st.cache_data
def get_unique_values_from_db(collection_name, column):
    """Get unique values directly from database with caching"""
    client = get_mongodb_client()
    db = client.warehouse
    collection = db[collection_name]
    
    pipeline = [
        {"$group": {"_id": f"${column}"}},
        {"$sort": {"_id": 1}},
        {"$limit": 1000}
    ]
    
    try:
        unique_values = [doc["_id"] for doc in collection.aggregate(pipeline) if doc["_id"] is not None]
        return sorted(unique_values)
    except Exception as e:
        st.error(f"Erro ao obter valores únicos para {column}: {str(e)}")
        return []

def build_mongo_query(filters):
    query = {}
    
    for column, filter_info in filters.items():
        filter_type = filter_info['type']
        filter_value = filter_info['value']
        
        if not filter_value:
            continue
            
        if filter_type == 'text':
            query[column] = {'$regex': filter_value, '$options': 'i'}
        elif filter_type == 'multi':
            if len(filter_value) > 0:
                query[column] = {'$in': filter_value}
    
    return query

def load_paginated_data(collection_name, page, page_size, filters=None):
    client = get_mongodb_client()
    db = client.warehouse
    collection = db[collection_name]
    
    query = build_mongo_query(filters) if filters else {}
    skip = (page - 1) * page_size
    
    try:
        total_filtered = collection.count_documents(query)
        cursor = collection.find(query).skip(skip).limit(page_size)
        documents = [convert_document_for_pandas(doc) for doc in cursor]
        
        if documents:
            df = pd.DataFrame(documents)
            if '_id' in df.columns:
                df = df.drop('_id', axis=1)
        else:
            df = pd.DataFrame()
            
        return df, total_filtered
        
    except Exception as e:
        st.error(f"Erro ao carregar dados: {str(e)}")
        return pd.DataFrame(), 0

def create_filters_interface(collection_name, columns):
    filters = {}
    
    with st.expander("🔍 Filtros", expanded=False):
        selected_columns = st.multiselect(
            "Selecione as colunas para filtrar:",
            columns,
            key=f"filter_cols_{collection_name}"
        )
        
        if selected_columns:
            cols = st.columns(2)
            for idx, column in enumerate(selected_columns):
                with cols[idx % 2]:
                    st.markdown(f"#### {column}")
                    
                    filter_type_key = f"filter_type_{collection_name}_{column}"
                    filter_type = st.radio(
                        "Tipo de filtro:",
                        ["Texto", "Seleção Múltipla"],
                        key=filter_type_key,
                        horizontal=True
                    )
                    
                    if filter_type == "Texto":
                        value = st.text_input(
                            "Buscar:",
                            key=f"text_filter_{collection_name}_{column}"
                        )
                        if value:
                            filters[column] = {'type': 'text', 'value': value}
                    else:
                        unique_vals = get_unique_values_from_db(collection_name, column)
                        if unique_vals:
                            selected = st.multiselect(
                                "Selecione os valores:",
                                options=unique_vals,
                                key=f"multi_filter_{collection_name}_{column}",
                                help="Selecione um ou mais valores para filtrar"
                            )
                            if selected:
                                filters[column] = {'type': 'multi', 'value': selected}
                    
                    st.markdown("---")
    
    return filters

def display_data_page(collection_name):
    total_docs, columns = get_collection_columns(collection_name)
    
    if total_docs == 0:
        st.error(f"Nenhum documento encontrado na coleção {collection_name}")
        return
    
    filters = create_filters_interface(collection_name, columns)
    
    with st.container():
        
        
        col1, col2,col3 = st.columns([1,1,1])
        with col1:
            c1, c2 = st.columns([1, 1])
            c1.write('Registros por página:')
            page_size = c2.selectbox(
                "Registros por página:",
                options=[10, 25, 50, 100, 1000],
                index=1,
                key=f"page_size_{collection_name}",label_visibility='collapsed'
            )
        
        if f'page_{collection_name}' not in st.session_state:
            st.session_state[f'page_{collection_name}'] = 1
        current_page = st.session_state[f'page_{collection_name}']
        
        df, total_filtered = load_paginated_data(collection_name, current_page, page_size, filters)
        
        total_pages = math.ceil(total_filtered / page_size) if total_filtered > 0 else 1
        current_page = min(current_page, total_pages)
        
        with col2:
            st.write(f"Total: {total_filtered} registros | Página {current_page} de {total_pages}")
        
        with col3:
            cols = st.columns(4)
            if cols[0].button("⏪", key=f"first_{collection_name}"):
                st.session_state[f'page_{collection_name}'] = 1
                st.rerun()
                
            if cols[1].button("◀️", key=f"prev_{collection_name}"):
                if current_page > 1:
                    st.session_state[f'page_{collection_name}'] = current_page - 1
                    st.rerun()
                    
            if cols[2].button("▶️", key=f"next_{collection_name}"):
                if current_page < total_pages:
                    st.session_state[f'page_{collection_name}'] = current_page + 1
                    st.rerun()
                    
            if cols[3].button("⏩", key=f"last_{collection_name}"):
                st.session_state[f'page_{collection_name}'] = total_pages
                st.rerun()
            
    if not df.empty:
        alt_df = (len(df) * 36 - len(df)-1.5)

        # Arredondando o valor para o número inteiro mais próximo
        alt_df_arredondado = round(alt_df)

        # Ou, se preferir sempre arredondar para baixo
        alt_df_arredondado_para_baixo = math.floor(alt_df)

        # Ou, se preferir sempre arredondar para cima
        alt_df_arredondado_para_cima = math.ceil(alt_df)
        st.dataframe(df, 
                     use_container_width=True, 
                     height=alt_df_arredondado_para_baixo,
                     hide_index=True
                     )
        
        
        if st.button("📥 Baixar dados filtrados", key=f"download_{collection_name}"):
            progress_text = "Preparando download..."
            progress_bar = st.progress(0, text=progress_text)
            
            all_data = []
            batch_size = 1000
            total_pages_download = math.ceil(total_filtered / batch_size)
            
            for page in range(1, total_pages_download + 1):
                progress = page / total_pages_download
                progress_bar.progress(progress, text=f"{progress_text} ({page}/{total_pages_download})")
                
                page_df, _ = load_paginated_data(collection_name, page, batch_size, filters)
                all_data.append(page_df)
            
            full_df = pd.concat(all_data, ignore_index=True)
            
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                full_df.to_excel(writer, index=False, sheet_name='Dados')
            
            st.download_button(
                label="💾 Clique para baixar Excel",
                data=buffer.getvalue(),
                file_name=f'{collection_name}_dados.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            
            progress_bar.empty()
    else:
        st.warning("Nenhum dado encontrado com os filtros aplicados")

def main():
    st.set_page_config(
        page_title="Dashboard MongoDB",
        page_icon="📊",
        layout="wide"
    )

    st.subheader("📊 Visualização dos Dados")
    
    collections = ['xml', 'po', 'nfspdf']
    tabs = st.tabs([collection.upper() for collection in collections])
    
    for tab, collection_name in zip(tabs, collections):
        with tab:
            display_data_page(collection_name)
    
    st.divider()
    st.caption("Dashboard de Dados MongoDB v1.0")

if __name__ == "__main__":
    main()