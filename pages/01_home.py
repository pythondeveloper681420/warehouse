import streamlit as st
import polars as pl
from pymongo import MongoClient
import urllib.parse
import unicodedata
import re
from bson.objectid import ObjectId

#infer_and_convert_types

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
    
import streamlit as st
import polars as pl
from pymongo import MongoClient
from datetime import datetime
import pytz

def infer_and_convert_types(documents):
    """
    Infer and standardize types for MongoDB documents before converting to Polars DataFrame.
    """
    if not documents:
        return documents
    
    # Sample a subset of documents for type inference
    sample_size = min(100, len(documents))
    sample_docs = documents[:sample_size]
    
    # Initialize type mapping
    type_mapping = {}
    
    # Analyze sample documents to infer types
    for doc in sample_docs:
        for key, value in doc.items():
            if key not in type_mapping:
                type_mapping[key] = set()
            if value is not None:
                type_mapping[key].add(type(value))
    
    # Convert documents based on inferred types
    for doc in documents:
        for key, type_set in type_mapping.items():
            if key not in doc or doc[key] is None:
                continue
                
            # Handle datetime fields
            if datetime in type_set:
                if isinstance(doc[key], str):
                    try:
                        # Try parsing with timezone
                        doc[key] = datetime.fromisoformat(doc[key].replace('Z', '+00:00'))
                    except ValueError:
                        try:
                            # Try common datetime formats
                            formats = [
                                '%Y-%m-%d %H:%M:%S',
                                '%Y-%m-%d',
                                '%d/%m/%Y %H:%M:%S',
                                '%d/%m/%Y'
                            ]
                            for fmt in formats:
                                try:
                                    doc[key] = datetime.strptime(doc[key], fmt)
                                    break
                                except ValueError:
                                    continue
                        except Exception:
                            # If parsing fails, keep as string
                            pass
            
            # Handle numeric fields
            elif str in type_set and any(t in type_set for t in (int, float)):
                try:
                    if isinstance(doc[key], str):
                        # Remove any non-numeric characters except decimal point
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
        
        # Fetch documents
        documents = list(collection.find())
        
        # Convert ObjectIds to strings
        documents = convert_objectid_to_str(documents)
        
        # Infer and convert types
        documents = infer_and_convert_types(documents)
        
        if not documents:
            return pl.DataFrame()
        
        # Create DataFrame with increased schema inference length
        polars_df = pl.DataFrame(
            documents,
            infer_schema_length=None  # Use all rows for schema inference
        )
        
        return polars_df
    
    except Exception as e:
        st.error(f"Error loading collection {collection_name}: {str(e)}")
        return pl.DataFrame()
    
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
        self.collections = ['merged_data', 'merged_nfspdf', 'po']
        
        self.mongo_uri = self._get_mongo_uri()
        self._setup_page()
        
        if 'current_filters' not in st.session_state:
            st.session_state.current_filters = {}
        
        self.dataframes = {}
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
        
    def _clean_po_number(self, df, po_column):
        """Clean PO numbers by removing non-numeric characters and .0 suffix"""
        try:
            return df.with_columns([
                pl.when(pl.col(po_column).is_null())
                .then(pl.lit(""))
                .otherwise(
                    pl.col(po_column).cast(pl.Utf8)
                    .str.replace_all(r'[^\d]', '')
                    .str.replace(r'\.0$', '')
                )
                .alias(f"{po_column}_cleaned")
            ])
        except Exception as e:
            st.warning(f"Warning: Could not clean column {po_column}: {e}")
            # If cleaning fails, create an empty cleaned column
            return df.with_columns(pl.lit("").alias(f"{po_column}_cleaned"))

    def _load_and_merge_collections(self):
        with st.spinner("Loading and merging data..."):
            try:
                # Load XML collection
                xml_df = mongo_collection_to_polars(self.mongo_uri, self.db_name, 'xml')
                # Load PO collection
                po_df = mongo_collection_to_polars(self.mongo_uri, self.db_name, 'po')
                po_df = po_df.unique(subset=['Purchasing Document'])

                # Check if XML and PO have data
                if not xml_df.is_empty() and not po_df.is_empty():
                    # Standardize 'po' column in XML
                    xml_df = xml_df.with_columns([
                        pl.col('po').cast(pl.Utf8).str.replace_all(r'\D', '').alias('po_cleaned')
                    ])
                    # Remove '.0' from the end of 'po' values after converting to string
                    xml_df = xml_df.with_columns([
                        pl.col('po').cast(pl.Utf8)  # Convert to string
                        .str.replace(r'\.0$', '')    # Remove the '.0' at the end of values
                        .alias('po_cleaned')         # Column for the join
                    ])
                    # Standardize 'Purchasing Document' column in PO
                    po_df = po_df.with_columns([
                        pl.col('Purchasing Document').cast(pl.Utf8).str.replace_all(r'\D', '').alias('Purchasing Document_cleaned')
                    ])
                    
                    # Filter specific columns from PO for the join
                    po_df = po_df.select([
                        'Purchasing Document_cleaned',
                        'Project Code',
                        'Andritz WBS Element',
                        'Cost Center'
                    ])

                    # Perform the join between XML and PO using the standardized columns
                    merged_xml_po = xml_df.join(
                        po_df,
                        left_on='po_cleaned',
                        right_on='Purchasing Document_cleaned',
                        how='left'
                    ).sort(by=['dtEmi', 'nNf', 'itemNf'], descending=[True, False, False])

                    self.dataframes['merged_data'] = merged_xml_po

                else:
                    st.error("XML or PO is empty.")
                    self.dataframes['merged_data'] = pl.DataFrame()

                # Load NFSPDF collection
                nfspdf_df = mongo_collection_to_polars(self.mongo_uri, self.db_name, 'nfspdf')
                if not nfspdf_df.is_empty() and not po_df.is_empty():
                    # Standardize 'po' column in NFSPDF
                    po_column_name = 'po' if 'po' in nfspdf_df.columns else None
                    if po_column_name:
                        nfspdf_df = nfspdf_df.with_columns([
                            pl.col(po_column_name).cast(pl.Utf8).str.replace_all(r'\D', '').alias('po_cleaned')
                        ])

                        # Perform the join between NFSPDF and PO using the standardized columns
                        merged_nfspdf_po = nfspdf_df.join(
                            po_df,
                            left_on='po_cleaned',
                            right_on='Purchasing Document_cleaned',
                            how='left'
                        ).sort(by=['Data Emissão'], descending=True)

                        self.dataframes['merged_nfspdf'] = merged_nfspdf_po

                    else:
                        st.warning("PO column not found in the NFSPDF collection.")
                        self.dataframes['merged_nfspdf'] = nfspdf_df
                
                # Load PO collection
                self.dataframes['po'] = po_df

            except Exception as e:
                st.error(f"Error loading and merging collections: {str(e)}")
                self.dataframes['merged_data'] = pl.DataFrame()
                self.dataframes['merged_nfspdf'] = pl.DataFrame()
                self.dataframes['po'] = pl.DataFrame()

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
                        use_container_width=True,
                        #hide_index=True,
                    )
                # with col2:    
                #         st.dataframe(
                #         filtered_df.to_pandas()
                #         .set_index(filtered_df.columns[0]),
                #         use_container_width=True,
                #         hide_index=True,
                #     )    
                     
if __name__ == "__main__":
    app = DataFilterApp()
    app.run()