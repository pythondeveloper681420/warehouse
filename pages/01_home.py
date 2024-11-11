import streamlit as st
import polars as pl
from pymongo import MongoClient
import urllib.parse
from typing import Dict, Optional, List

class MongoDBDashboard:
    def __init__(self):
        self.username = 'devpython86'
        self.password = 'dD@pjl06081420'
        self.cluster = 'cluster0.akbb8.mongodb.net'
        self.db_name = 'warehouse'
        self.collections = ['merged_data', 'merged_nfspdf', 'po']
        
        self._setup_page()
        self._initialize_state()
        self.mongo_uri = self._get_mongo_uri()

    def _get_mongo_uri(self) -> str:
        return f"mongodb+srv://{urllib.parse.quote_plus(self.username)}:{urllib.parse.quote_plus(self.password)}@{self.cluster}/{self.db_name}?retryWrites=true&w=majority"

    def _setup_page(self):
        st.set_page_config(
            page_title="Dashboard MongoDB",
            page_icon="📊",
            layout="wide",
            initial_sidebar_state="collapsed"
        )
        
            # Header
        col1, col2 = st.columns([6, 1])
        with col1:
            st.title("📊 MongoDB Dashboard")

    def _initialize_state(self):
        if 'dataframes' not in st.session_state:
            st.session_state.dataframes = {}
        if 'filters' not in st.session_state:
            st.session_state.filters = {}

    @staticmethod
    @st.cache_data(ttl=300)
    def _fetch_mongodb_data(mongo_uri: str, db_name: str, collection_name: str) -> Optional[pl.DataFrame]:
        try:
            client = MongoClient(mongo_uri)
            db = client[db_name]
            collection = db[collection_name]
            documents = list(collection.find())
            
            if not documents:
                return None
            
            # Convert ObjectId to string
            for doc in documents:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])
            
            return pl.DataFrame(documents)
        except Exception as e:
            st.error(f"Error loading {collection_name}: {str(e)}")
            return None
        finally:
            client.close()

    def _standardize_po_numbers(self, df: pl.DataFrame, po_column: str) -> pl.DataFrame:
        if po_column not in df.columns:
            return df
        
        return df.with_columns([
            pl.col(po_column).cast(pl.Utf8)
            .str.replace_all(r'[^\d]', '')
            .str.replace(r'\.0$', '')
            .alias(f"{po_column}_cleaned")
        ])

    def _merge_collections(self) -> Dict[str, pl.DataFrame]:
        results = {}
        
        # Load PO data
        po_df = self._fetch_mongodb_data(self.mongo_uri, self.db_name, 'po')
        if po_df is not None:
            po_df = po_df.unique(subset=['Purchasing Document'])
            po_df = self._standardize_po_numbers(po_df, 'Purchasing Document')
            po_df = po_df.select([
                'Purchasing Document_cleaned',
                'Project Code',
                'Andritz WBS Element',
                'Cost Center'
            ])
            results['po'] = po_df

        # Load and merge XML data
        xml_df = self._fetch_mongodb_data(self.mongo_uri, self.db_name, 'xml')
        if xml_df is not None:
            xml_df = self._standardize_po_numbers(xml_df, 'po')
            if po_df is not None:
                xml_df = xml_df.join(
                    po_df,
                    left_on='po_cleaned',
                    right_on='Purchasing Document_cleaned',
                    how='left'
                ).sort(['dtEmi', 'nNf', 'itemNf'], descending=[True, False, False])
            results['merged_data'] = xml_df

        # Load and merge NFSPDF data
        nfspdf_df = self._fetch_mongodb_data(self.mongo_uri, self.db_name, 'nfspdf')
        if nfspdf_df is not None:
            if 'po' in nfspdf_df.columns:
                nfspdf_df = self._standardize_po_numbers(nfspdf_df, 'po')
                if po_df is not None:
                    nfspdf_df = nfspdf_df.join(
                        po_df,
                        left_on='po_cleaned',
                        right_on='Purchasing Document_cleaned',
                        how='left'
                    ).sort(['Data Emissão'], descending=True)
            results['merged_nfspdf'] = nfspdf_df

        return results

    def _create_filter_ui(self, df: pl.DataFrame, collection_name: str) -> Dict:
        if df.is_empty():
            return {}

        filters = {}
        selected_columns = st.multiselect(
            "Select columns to filter:",
            df.columns,
            key=f"cols_{collection_name}"
        )

        for column in selected_columns:
            st.markdown(f"### {column}")
            col_type = df[column].dtype

            if col_type == pl.Utf8:
                filter_type = st.radio(
                    "Filter type:",
                    ["Input", "Multi-select"],
                    key=f"type_{collection_name}_{column}",
                    horizontal=True
                )

                if filter_type == "Input":
                    value = st.text_input("Enter value:", key=f"input_{collection_name}_{column}")
                    if value:
                        filters[column] = ("input", value.lower())
                else:
                    unique_vals = df[column].unique().sort().to_list()
                    values = st.multiselect("Select values:", unique_vals, key=f"multi_{collection_name}_{column}")
                    if values:
                        filters[column] = ("multi", values)
            
            elif col_type in [pl.Int64, pl.Float64]:
                unique_vals = df[column].unique().sort().to_list()
                values = st.multiselect(f"Select values for {column}:", unique_vals, key=f"range_{collection_name}_{column}")
                if values:
                    filters[column] = ("multi", values)

        return filters

    def _apply_filters(self, df: pl.DataFrame, filters: Dict) -> pl.DataFrame:
        if not filters:
            return df

        filtered_df = df.clone()
        for column, (filter_type, value) in filters.items():
            if column not in filtered_df.columns:
                continue

            if filter_type == "input":
                filtered_df = filtered_df.filter(
                    pl.col(column).cast(pl.Utf8).str.to_lowercase().str.contains(value)
                )
            elif filter_type == "multi":
                filtered_df = filtered_df.filter(pl.col(column).is_in(value))

        return filtered_df

    def refresh_data(self):
        st.cache_data.clear()
        st.session_state.dataframes = self._merge_collections()

    def run(self):
        # Header
        
        if st.button("🔄 Refresh Data", use_container_width=True):
            self.refresh_data()

        # Load data if not already loaded
        if not st.session_state.dataframes:
            with st.spinner("Loading data..."):
                st.session_state.dataframes = self._merge_collections()

        # Create tabs
        tabs = st.tabs(["🆕 Merged Data", "🗃️ NFSPDF", "📄 PO"])
        
        for tab, collection_name in zip(tabs, self.collections):
            with tab:
                df = st.session_state.dataframes.get(collection_name)
                if df is None:
                    st.warning(f"No data available for {collection_name}")
                    continue

                col1, col2 = st.columns([1, 4])
                
                with col1:
                    st.metric("Total Records", len(df))
                    filters = self._create_filter_ui(df, collection_name)
                    if filters:
                        filtered_df = self._apply_filters(df, filters)
                        st.metric("Filtered Records", len(filtered_df))
                    else:
                        filtered_df = df

                with col2:
                    st.dataframe(
                        filtered_df.to_pandas(),
                        use_container_width=True,
                        height=600
                    )

if __name__ == "__main__":
    dashboard = MongoDBDashboard()
    dashboard.run()