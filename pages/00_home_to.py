import streamlit as st
import polars as pl
from pymongo import MongoClient
import urllib.parse
import unicodedata
import re
from bson.objectid import ObjectId

def slugify(text):
    """
    Convert a text string into a slug format.
    """
    if not isinstance(text, str):
        text = str(text)
    
    text = text.lower()
    text = unicodedata.normalize('NFKD', text)
    text = text.encode('ascii', 'ignore').decode('utf-8')
    text = re.sub(r'[^a-z0-9]+', '-', text)
    text = text.strip('-')
    text = re.sub(r'-+', '-', text)
    
    return text

def convert_objectid_to_str(documents):
    for document in documents:
        for key, value in document.items():
            if isinstance(value, ObjectId):
                document[key] = str(value)
    return documents

def normalize_string(text):
    if not isinstance(text, str):
        return str(text)
    text = unicodedata.normalize('NFKD', text)
    text = re.sub(r'[^\w\s]', '', text)
    return text

def standardize_po_number(df, po_column):
    """
    Standardize PO numbers to Int64 format by:
    1. Removing non-numeric characters
    2. Converting to Int64
    3. Handling null values
    """
    try:
        return df.with_columns([
            pl.when(pl.col(po_column).is_null())
            .then(None)
            .otherwise(
                pl.col(po_column)
                .cast(pl.Utf8)
                .str.replace_all(r'[^\d]', '')  # Remove non-numeric chars
                .str.replace(r'\.0$', '')       # Remove .0 suffix
                .cast(pl.Int64)                 # Convert to Int64
            )
            .alias(po_column)
        ])
    except Exception as e:
        st.warning(f"Warning: Could not standardize column {po_column}: {e}")
        return df

@st.cache_data
def mongo_collection_to_polars(mongo_uri, db_name, collection_name, page_size, page_num, filters=None):
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
        
        if filters:
            query = filters
        else:
            query = {}
        
        total_count = collection.count_documents(query)
        
        cursor = collection.find(query)
        cursor = cursor.skip(page_size * page_num).limit(page_size)
        documents = list(cursor)
        documents = convert_objectid_to_str(documents)
        
        if not documents:
            return pl.DataFrame(), 0, 0
        
        polars_df = pl.DataFrame(documents, infer_schema_length=1000)
        return polars_df, page_size, total_count
    except Exception as e:
        st.error(f"Error loading collection {collection_name}: {e}")
        return pl.DataFrame(), 0, 0

@st.cache_data
def get_unique_values(_df, column):
    try:
        return _df[column].unique().to_list()
    except:
        return []

class DataFilterApp:
    def __init__(self):
        self.username = 'devpython86'
        self.password = 'dD@pjl06081420'
        self.cluster = 'cluster0.akbb8.mongodb.net'
        self.db_name = 'warehouse'
        self.collections = ['xml', 'nfspdf', 'po']
        
        self.mongo_uri = self._get_mongo_uri()
        self._setup_page()
        
        if 'current_filters' not in st.session_state:
            st.session_state.current_filters = {}
        
        self.dataframes = {}
        self.page_sizes = {}
        self.total_counts = {}
        self._load_and_merge_collections()

    def _get_mongo_uri(self):
        escaped_username = urllib.parse.quote_plus(self.username)
        escaped_password = urllib.parse.quote_plus(self.password)
        return f"mongodb+srv://{escaped_username}:{escaped_password}@{self.cluster}/{self.db_name}?retryWrites=true&w=majority"

    def _setup_page(self):
        st.set_page_config(
            page_title="Dashboard MongoDB",
            page_icon="📊",
            layout="wide",
            initial_sidebar_state="collapsed"
        )
        
    def _load_and_merge_collections(self):
        with st.spinner("Loading and merging data..."):
            try:
                # Load XML collection
                st.write("Loading XML collection...")
                try:
                    xml_df, page_size_xml, total_count_xml = mongo_collection_to_polars(self.mongo_uri, self.db_name, 'xml', 100, 0)
                    st.write(f"XML collection loaded. Page size: {page_size_xml}, Total count: {total_count_xml}")
                    self.dataframes['merged_data'] = xml_df
                    self.page_sizes['merged_data'] = page_size_xml
                    self.total_counts['merged_data'] = total_count_xml
                except Exception as e:
                    st.error(f"Error loading XML collection: {str(e)}")
                    self.dataframes['merged_data'] = pl.DataFrame()
                    self.page_sizes['merged_data'] = 0
                    self.total_counts['merged_data'] = 0

                # Load NFSPDF collection
                st.write("Loading NFSPDF collection...")
                try:
                    nfspdf_df, page_size_nfspdf, total_count_nfspdf = mongo_collection_to_polars(self.mongo_uri, self.db_name, 'nfspdf', 100, 0)
                    st.write(f"NFSPDF collection loaded. Page size: {page_size_nfspdf}, Total count: {total_count_nfspdf}")
                    self.dataframes['merged_nfspdf'] = nfspdf_df
                    self.page_sizes['merged_nfspdf'] = page_size_nfspdf
                    self.total_counts['merged_nfspdf'] = total_count_nfspdf
                except Exception as e:
                    st.error(f"Error loading NFSPDF collection: {str(e)}")
                    self.dataframes['merged_nfspdf'] = pl.DataFrame()
                    self.page_sizes['merged_nfspdf'] = 0
                    self.total_counts['merged_nfspdf'] = 0

                # Load PO collection
                st.write("Loading PO collection...")
                try:
                    po_df, page_size_po, total_count_po = mongo_collection_to_polars(self.mongo_uri, self.db_name, 'po', 100, 0)
                    st.write(f"PO collection loaded. Page size: {page_size_po}, Total count: {total_count_po}")
                    self.dataframes['po'] = po_df
                    self.page_sizes['po'] = page_size_po
                    self.total_counts['po'] = total_count_po
                except Exception as e:
                    st.error(f"Error loading PO collection: {str(e)}")
                    self.dataframes['po'] = pl.DataFrame()
                    self.page_sizes['po'] = 0
                    self.total_counts['po'] = 0

            except Exception as e:
                st.error(f"Error loading and merging collections: {str(e)}")
                self.dataframes['merged_data'] = pl.DataFrame()
                self.dataframes['merged_nfspdf'] = pl.DataFrame()
                self.dataframes['po'] = pl.DataFrame()
                self.page_sizes['merged_data'] = 0
                self.page_sizes['merged_nfspdf'] = 0
                self.page_sizes['po'] = 0
                self.total_counts['merged_data'] = 0
                self.total_counts['merged_nfspdf'] = 0
                self.total_counts['po'] = 0

    def _create_filters(self, df, collection_name):
        if df.is_empty():
            st.error("No data available to filter")
            return {}
        
        filters = {}
        columns = df.columns
        
        selected_columns = st.multiselect(
            "Select columns to filter:",
            columns,
            key=f"cols_{collection_name}"
        )
        
        st.session_state[f"selected_columns_{collection_name}"] = selected_columns
        
        for column in selected_columns:
            st.markdown(f"### {column}")
            column_type = df[column].dtype
            
            if column_type == pl.Utf8:
                filter_type = st.radio(
                    "Filter type:",
                    ["Input", "Multi-select"],
                    key=f"type_{collection_name}_{column}",
                    horizontal=True
                )
                
                if filter_type == "Input":
                    value = st.text_input(
                        "Enter value:",
                        key=f"input_{collection_name}_{column}"
                    )
                    if value:
                        filters[column] = ("input", value)
                else:
                    unique_values = get_unique_values(df, column)
                    values = st.multiselect(
                        "Select values:",
                        unique_values,
                        key=f"multi_{collection_name}_{column}"
                    )
                    if values:
                        filters[column] = ("multi", values)
            
            elif column_type in [pl.Int64, pl.Float64]:
                unique_values = df[column].unique().to_list()
                values = st.multiselect(
                    f"Select values for {column}:",
                    unique_values,
                    key=f"range_{collection_name}_{column}"
                )
                if values:
                    filters[column] = ("multi", values)
        
        return filters

    def _apply_filters(self, df, filters):
        if not filters:
            return df
        
        filtered_df = df
        
        for column, (filter_type, value) in filters.items():
            if column not in filtered_df.columns:
                continue
                
            column_type = filtered_df[column].dtype
            
            if filter_type == "input" and column_type == pl.Utf8:
                normalized_value = normalize_string(value.strip().lower())
                words = normalized_value.split()
                pattern = ".*" + ".*".join(words) + ".*"
                
                filtered_df = filtered_df.with_columns([
                    pl.col(column).cast(pl.Utf8).str.to_lowercase()
                    .str.replace_all(r'[^\w\s]', '', literal=False)
                    .alias(f"{column}_normalized")
                ])
                filtered_df = filtered_df.filter(
                    pl.col(f"{column}_normalized").str.contains(pattern)
                )
            
            elif filter_type == "multi":
                filtered_df = filtered_df.filter(pl.col(column).is_in(value))
        
        return filtered_df

    def run(self):
        st.title("📊 MongoDB Dashboard")


        tabs = st.tabs(["🆕 Merged Data", "🗃️ NFSPDF", "📄 PO"])
        
        for tab, collection_name in zip(tabs, self.collections):
            with tab:
                df = self.dataframes[collection_name]
                
                if df.is_empty():
                    st.error(f"No data found in collection {collection_name}")
                    continue
                
                page_size = self.page_sizes[collection_name]
                total_count = self.total_counts[collection_name]
                
                col1, col2, col3 = st.columns([1, 1, 4])
                
                with col1:
                    st.metric("Total Records", total_count)
                with col2:
                    page_num = st.number_input(
                        "Page Number",
                        min_value=0,
                        step=1,
                        value=0,
                        key=f"page_num_{collection_name}"
                    )
                with col3:
                    filters = self._create_filters(df, collection_name)
                    if filters:
                        filtered_df, _, _ = mongo_collection_to_polars(
                            self.mongo_uri,
                            self.db_name,
                            collection_name,
                            page_size,
                            page_num,
                            filters
                        )
                        st.metric("Filtered Records", len(filtered_df))
                    else:
                        filtered_df, _, _ = mongo_collection_to_polars(
                            self.mongo_uri,
                            self.db_name,
                            collection_name,
                            page_size,
                            page_num
                        )
                    
                    st.dataframe(
                        filtered_df.to_pandas(),
                        use_container_width=True,
                        height=600
                    )

if __name__ == "__main__":
    app = DataFilterApp()
    app.run()