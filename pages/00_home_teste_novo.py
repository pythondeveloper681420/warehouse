import streamlit as st
import polars as pl
from pymongo import MongoClient
import urllib.parse
import unicodedata
import re
from bson.objectid import ObjectId
from datetime import datetime, timedelta
import pytz

# Set page config as the first Streamlit command
st.set_page_config(page_title="Dashboard MongoDB", page_icon="📊", layout="wide")

def slugify(text):
    if not isinstance(text, str):
        text = str(text)
    text = text.lower()
    text = unicodedata.normalize('NFKD', text)
    text = text.encode('ascii', 'ignore').decode('utf-8')
    text = re.sub(r'[^a-z0-9]+', '-', text)
    text = text.strip('-')
    text = re.sub(r'-+', '-', text)
    return text

def normalize_string(text):
    if not isinstance(text, str):
        return str(text)
    text = unicodedata.normalize('NFKD', text)
    text = re.sub(r'[^\w\s]', '', text)
    return text

def convert_objectid_to_str(documents):
    for document in documents:
        for key, value in document.items():
            if isinstance(value, ObjectId):
                document[key] = str(value)
    return documents

def infer_and_convert_types(documents):
    if not documents:
        return documents
    
    sample_size = min(100, len(documents))
    sample_docs = documents[:sample_size]
    type_mapping = {}
    
    for doc in sample_docs:
        for key, value in doc.items():
            if key not in type_mapping:
                type_mapping[key] = set()
            if value is not None:
                type_mapping[key].add(type(value))
    
    for doc in documents:
        for key, type_set in type_mapping.items():
            if key not in doc or doc[key] is None:
                continue
            
            if datetime in type_set:
                if isinstance(doc[key], str):
                    try:
                        doc[key] = datetime.fromisoformat(doc[key].replace('Z', '+00:00'))
                    except ValueError:
                        formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y %H:%M:%S', '%d/%m/%Y']
                        for fmt in formats:
                            try:
                                doc[key] = datetime.strptime(doc[key], fmt)
                                break
                            except ValueError:
                                continue
            
            elif str in type_set and any(t in type_set for t in (int, float)):
                try:
                    if isinstance(doc[key], str):
                        cleaned = ''.join(c for c in doc[key] if c.isdigit() or c == '.')
                        if cleaned:
                            if '.' in cleaned:
                                doc[key] = float(cleaned)
                            else:
                                doc[key] = int(cleaned)
                except ValueError:
                    pass
    
    return documents

@st.cache_data
def mongo_collection_to_polars(mongo_uri, db_name, collection_name):
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
        
        documents = list(collection.find())
        documents = convert_objectid_to_str(documents)
        documents = infer_and_convert_types(documents)
        
        if not documents:
            return pl.DataFrame()
        
        return pl.DataFrame(documents, infer_schema_length=None)
    
    except Exception as e:
        st.error(f"Error loading collection {collection_name}: {str(e)}")
        return pl.DataFrame()

def get_unique_values(df, column):
    try:
        return df[column].unique().to_list()
    except:
        return []

class DataFilterApp:
    def __init__(self):
        self.username = 'devpython86'
        self.password = 'dD@pjl06081420'
        self.cluster = 'cluster0.akbb8.mongodb.net'
        self.db_name = 'warehouse'
        self.collections = ['merged_data', 'merged_nfspdf', 'po']
        self.mongo_uri = self._get_mongo_uri()
        
        # Initialize session state
        if 'init' not in st.session_state:
            self._initialize_session_state()
        
        self.dataframes = {}
        self._init_data()

    def _initialize_session_state(self):
        st.session_state.init = True
        st.session_state.last_refresh = datetime.now()
        st.session_state.auto_refresh = False
        st.session_state.filters = {collection: {} for collection in self.collections}
        
        # Initialize filter states for each collection
        for collection in self.collections:
            st.session_state[f'selected_columns_{collection}'] = []
            st.session_state[f'filter_states_{collection}'] = {}

    def _get_mongo_uri(self):
        escaped_username = urllib.parse.quote_plus(self.username)
        escaped_password = urllib.parse.quote_plus(self.password)
        return f"mongodb+srv://{escaped_username}:{escaped_password}@{self.cluster}/{self.db_name}?retryWrites=true&w=majority"

    def _init_data(self):
        current_time = datetime.now()
        if (st.session_state.auto_refresh and 
            current_time - st.session_state.last_refresh >= timedelta(minutes=10)):
            self._load_and_merge_collections()
            st.session_state.last_refresh = current_time
        elif not self.dataframes:
            self._load_and_merge_collections()

    def _load_and_merge_collections(self):
        with st.spinner("Loading and merging data..."):
            try:
                # Load collections
                xml_df = mongo_collection_to_polars(self.mongo_uri, self.db_name, 'xml')
                po_df = mongo_collection_to_polars(self.mongo_uri, self.db_name, 'po')
                nfspdf_df = mongo_collection_to_polars(self.mongo_uri, self.db_name, 'nfspdf')
                
                if not xml_df.is_empty() and not po_df.is_empty():
                    # Prepare XML data
                    xml_df = xml_df.with_columns([
                        pl.col('po').cast(pl.Utf8)
                        .str.replace_all(r'\D', '')
                        .str.replace(r'\.0$', '')
                        .alias('po_cleaned')
                    ])
                    
                    # Prepare PO data
                    po_df = po_df.unique(subset=['Purchasing Document'])
                    po_df = po_df.with_columns([
                        pl.col('Purchasing Document')
                        .cast(pl.Utf8)
                        .str.replace_all(r'\D', '')
                        .alias('Purchasing Document_cleaned')
                    ])
                    
                    # Select specific columns from PO
                    po_cols = ['Purchasing Document_cleaned', 'Project Code', 
                             'Andritz WBS Element', 'Cost Center']
                    po_df = po_df.select(po_cols)
                    
                    # Merge XML and PO
                    self.dataframes['merged_data'] = xml_df.join(
                        po_df,
                        left_on='po_cleaned',
                        right_on='Purchasing Document_cleaned',
                        how='left'
                    ).sort(['dtEmi', 'nNf', 'itemNf'], descending=[True, False, False])
                    
                    # Merge NFSPDF and PO if possible
                    if not nfspdf_df.is_empty():
                        nfspdf_df = nfspdf_df.with_columns([
                            pl.col('po').cast(pl.Utf8)
                            .str.replace_all(r'\D', '')
                            .alias('po_cleaned')
                        ])
                        
                        self.dataframes['merged_nfspdf'] = nfspdf_df.join(
                            po_df,
                            left_on='po_cleaned',
                            right_on='Purchasing Document_cleaned',
                            how='left'
                        ).sort('Data Emissão', descending=True)
                    
                    self.dataframes['po'] = po_df
                
            except Exception as e:
                st.error(f"Error loading data: {str(e)}")
                self.dataframes = {col: pl.DataFrame() for col in self.collections}

    def _create_filters(self, df, collection_name):
        if df.is_empty():
            return {}
        
        filters = {}
        filter_states = st.session_state[f'filter_states_{collection_name}']
        
        # Column selection
        selected_columns = st.multiselect(
            "Select columns to filter:",
            df.columns,
            key=f'cols_select_{collection_name}',
            default=st.session_state[f'selected_columns_{collection_name}']
        )
        st.session_state[f'selected_columns_{collection_name}'] = selected_columns
        
        for column in selected_columns:
            st.markdown(f"### {column}")
            column_type = df[column].dtype
            
            filter_key = f'{collection_name}_{column}'
            
            if column_type == pl.Utf8:
                # Initialize filter type if not exists
                if filter_key not in filter_states:
                    filter_states[filter_key] = {'type': 'Input', 'value': None}
                
                filter_type = st.radio(
                    "Filter type:",
                    ["Input", "Multi-select"],
                    key=f'type_{filter_key}',
                    index=0 if filter_states[filter_key]['type'] == 'Input' else 1,
                    horizontal=True
                )
                
                if filter_type == "Input":
                    value = st.text_input(
                        "Enter value:",
                        key=f'input_{filter_key}',
                        value=filter_states[filter_key].get('value', '')
                    )
                    if value:
                        filters[column] = ("input", value)
                        filter_states[filter_key] = {'type': 'Input', 'value': value}
                else:
                    unique_values = get_unique_values(df, column)
                    values = st.multiselect(
                        "Select values:",
                        unique_values,
                        key=f'multi_{filter_key}',
                        default=filter_states[filter_key].get('value', [])
                    )
                    if values:
                        filters[column] = ("multi", values)
                        filter_states[filter_key] = {'type': 'Multi-select', 'value': values}
            
            elif column_type in [pl.Int64, pl.Float64]:
                unique_values = sorted(df[column].unique().to_list())
                values = st.multiselect(
                    f"Select values for {column}:",
                    unique_values,
                    key=f'numeric_{filter_key}',
                    default=filter_states.get(filter_key, {}).get('value', [])
                )
                if values:
                    filters[column] = ("multi", values)
                    filter_states[filter_key] = {'value': values}
        
        return filters

    def _apply_filters(self, df, filters):
        if not filters:
            return df
        
        filtered_df = df
        
        for column, (filter_type, value) in filters.items():
            if column not in filtered_df.columns:
                continue
            
            if filter_type == "input" and filtered_df[column].dtype == pl.Utf8:
                normalized_value = normalize_string(value.strip().lower())
                words = normalized_value.split()
                pattern = ".*" + ".*".join(words) + ".*"
                
                filtered_df = filtered_df.with_columns([
                    pl.col(column)
                    .cast(pl.Utf8)
                    .str.to_lowercase()
                    .str.replace_all(r'[^\w\s]', '')
                    .alias('_temp')
                ])
                filtered_df = filtered_df.filter(pl.col('_temp').str.contains(pattern))
                filtered_df = filtered_df.drop('_temp')
            
            elif filter_type == "multi":
                filtered_df = filtered_df.filter(pl.col(column).is_in(value))
        
        return filtered_df

    def run(self):
        st.title("📊 MongoDB Dashboard")
        
        # Refresh controls
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("🔄 Refresh Data"):
                self._load_and_merge_collections()
                st.session_state.last_refresh = datetime.now()
        
        with col2:
            st.session_state.auto_refresh = st.toggle(
                "Auto-refresh (10 min)",
                value=st.session_state.auto_refresh
            )
        
        with col3:
            st.caption(f"Last refresh: {st.session_state.last_refresh.strftime('%Y-%m-%d %H:%M:%S')}")
        
        st.divider()
        
        # Create tabs
        tabs = st.tabs(["🆕 Merged Data", "🗃️ NFSPDF", "📄 PO"])
        
        for tab, collection_name in zip(tabs, self.collections):
            with tab:
                df = self.dataframes.get(collection_name, pl.DataFrame())
                
                if df.is_empty():
                    st.error(f"No data found in collection {collection_name}")
                    continue
                
                col1, col2 = st.columns([1, 4])
                
                with col1:
                    st.metric("Total Records", len(df))
                    filters = self._create_filters(df, collection_name)
                    
                    if filters:
                        filtered_df = self._apply_filters(df, filters)
                        st.metric("Filtered Records", len(filtered_df))
                    else:
                        filtered_df = df
                
                with col2:
                    st.dataframe(
                        filtered_df.to_pandas(),
                        use_container_width=True
                    )

if __name__ == "__main__":
    app = DataFilterApp()
    app.run()