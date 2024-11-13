import streamlit as st
import polars as pl
from pymongo import MongoClient
import urllib.parse
import unicodedata
import re
from bson.objectid import ObjectId
from datetime import datetime, timedelta
import pytz

def setup_page():
    """Configure a página do Streamlit"""
    st.set_page_config(
        page_title="Dashboard MongoDB",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="collapsed"
    )

def normalize_string(text):
    """Normaliza uma string removendo acentos e caracteres especiais"""
    if not isinstance(text, str):
        return str(text)
    text = unicodedata.normalize('NFKD', text)
    return re.sub(r'[^\w\s]', '', text)

def convert_objectid_to_str(documents):
    """Converte ObjectId para string em documentos MongoDB"""
    for doc in documents:
        for key, value in doc.items():
            if isinstance(value, ObjectId):
                doc[key] = str(value)
    return documents

@st.cache_data
def load_mongodb_data(mongo_uri, db_name, collection_name):
    """Carrega dados do MongoDB e converte para DataFrame Polars"""
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
        
        documents = list(collection.find())
        documents = convert_objectid_to_str(documents)
        
        if not documents:
            return pl.DataFrame()
        
        return pl.DataFrame(documents)
    except Exception as e:
        st.error(f"Erro ao carregar coleção {collection_name}: {str(e)}")
        return pl.DataFrame()

class FilterManager:
    """Gerencia a criação e aplicação de filtros nos dados"""
    
    def __init__(self, collection_name):
        self.collection_name = collection_name
        self._init_session_state()
    
    def _init_session_state(self):
        """Inicializa variáveis de estado da sessão"""
        base_key = f"filter_state_{self.collection_name}"
        if base_key not in st.session_state:
            st.session_state[base_key] = {
                'selected_columns': [],
                'filters': {}
            }
    
    def _get_filter_key(self, column, filter_type):
        """Gera uma chave única para cada filtro"""
        return f"filter_{self.collection_name}_{column}_{filter_type}"
    
    def create_filters(self, df):
        """Cria interface de filtros para o DataFrame"""
        if df.is_empty():
            st.error("Sem dados disponíveis para filtrar")
            return {}
        
        state = st.session_state[f"filter_state_{self.collection_name}"]
        
        # Seleção de colunas
        selected_columns = st.multiselect(
            "Selecione as colunas para filtrar:",
            df.columns,
            default=state['selected_columns'],
            key=f"cols_{self.collection_name}"
        )
        state['selected_columns'] = selected_columns
        
        filters = {}
        for column in selected_columns:
            st.markdown(f"### {column}")
            column_type = df[column].dtype
            
            if column_type == pl.Utf8:
                filter_type = st.radio(
                    "Tipo de filtro:",
                    ["Texto", "Multi-seleção"],
                    key=self._get_filter_key(column, "type"),
                    horizontal=True
                )
                
                if filter_type == "Texto":
                    value = st.text_input(
                        "Digite o valor:",
                        key=self._get_filter_key(column, "text")
                    )
                    if value:
                        filters[column] = ("text", value)
                else:
                    unique_values = df[column].unique().sort().to_list()
                    values = st.multiselect(
                        "Selecione os valores:",
                        unique_values,
                        key=self._get_filter_key(column, "multi")
                    )
                    if values:
                        filters[column] = ("multi", values)
            
            elif column_type in [pl.Int64, pl.Float64]:
                unique_values = sorted(df[column].unique().to_list())
                values = st.multiselect(
                    f"Selecione os valores para {column}:",
                    unique_values,
                    key=self._get_filter_key(column, "numeric")
                )
                if values:
                    filters[column] = ("multi", values)
        
        state['filters'] = filters
        return filters
    
    def apply_filters(self, df, filters):
        """Aplica os filtros ao DataFrame"""
        if not filters:
            return df
        
        filtered_df = df
        for column, (filter_type, value) in filters.items():
            if column not in filtered_df.columns:
                continue
            
            if filter_type == "text":
                normalized_value = normalize_string(value.strip().lower())
                words = normalized_value.split()
                pattern = ".*" + ".*".join(words) + ".*"
                
                filtered_df = filtered_df.with_columns([
                    pl.col(column).cast(pl.Utf8)
                    .str.to_lowercase()
                    .str.replace_all(r'[^\w\s]', '')
                    .alias(f"{column}_normalized")
                ])
                filtered_df = filtered_df.filter(
                    pl.col(f"{column}_normalized").str.contains(pattern)
                )
                filtered_df = filtered_df.drop(f"{column}_normalized")
            
            elif filter_type == "multi":
                filtered_df = filtered_df.filter(pl.col(column).is_in(value))
        
        return filtered_df

class DataMerger:
    """Gerencia a mesclagem de dados entre coleções"""
    
    def __init__(self, mongo_uri, db_name):
        self.mongo_uri = mongo_uri
        self.db_name = db_name
    
    def merge_collections(self):
        """Mescla as coleções XML, PO e NFSPDF"""
        # Carrega coleções
        xml_df = load_mongodb_data(self.mongo_uri, self.db_name, 'xml')
        po_df = load_mongodb_data(self.mongo_uri, self.db_name, 'po')
        nfspdf_df = load_mongodb_data(self.mongo_uri, self.db_name, 'nfspdf')
        
        results = {}
        
        # Processa e mescla XML com PO
        if not xml_df.is_empty() and not po_df.is_empty():
            po_df = po_df.unique(subset=['Purchasing Document'])
            
            # Padroniza colunas para join
            xml_df = xml_df.with_columns([
                pl.col('po').cast(pl.Utf8)
                .str.replace_all(r'\D', '')
                .str.replace(r'\.0$', '')
                .alias('po_cleaned')
            ])
            
            po_df = po_df.with_columns([
                pl.col('Purchasing Document').cast(pl.Utf8)
                .str.replace_all(r'\D', '')
                .alias('po_cleaned')
            ])
            
            # Seleciona colunas relevantes do PO
            po_join_df = po_df.select([
                'po_cleaned',
                'Project Code',
                'Andritz WBS Element',
                'Cost Center'
            ])
            
            # Realiza o join
            merged_xml_po = xml_df.join(
                po_join_df,
                left_on='po_cleaned',
                right_on='po_cleaned',
                how='left'
            ).sort(['dtEmi', 'nNf', 'itemNf'], descending=[True, False, False])
            
            results['merged_data'] = merged_xml_po
        else:
            results['merged_data'] = pl.DataFrame()
        
        # Processa e mescla NFSPDF com PO
        if not nfspdf_df.is_empty() and not po_df.is_empty():
            if 'po' in nfspdf_df.columns:
                nfspdf_df = nfspdf_df.with_columns([
                    pl.col('po').cast(pl.Utf8)
                    .str.replace_all(r'\D', '')
                    .alias('po_cleaned')
                ])
                
                merged_nfspdf_po = nfspdf_df.join(
                    po_join_df,
                    left_on='po_cleaned',
                    right_on='po_cleaned',
                    how='left'
                ).sort(['Data Emissão'], descending=True)
                
                results['merged_nfspdf'] = merged_nfspdf_po
            else:
                results['merged_nfspdf'] = nfspdf_df
        else:
            results['merged_nfspdf'] = pl.DataFrame()
        
        results['po'] = po_df
        return results

class Dashboard:
    """Classe principal do Dashboard"""
    
    def __init__(self):
        self.username = 'devpython86'
        self.password = 'dD@pjl06081420'
        self.cluster = 'cluster0.akbb8.mongodb.net'
        self.db_name = 'warehouse'
        self.collections = ['merged_data', 'merged_nfspdf', 'po']
        
        self.mongo_uri = self._get_mongo_uri()
        self._init_session_state()
        
        self.data_merger = DataMerger(self.mongo_uri, self.db_name)
        self.filter_managers = {
            collection: FilterManager(collection)
            for collection in self.collections
        }
        
        self.dataframes = {}
    
    def _get_mongo_uri(self):
        """Gera URI de conexão do MongoDB"""
        escaped_username = urllib.parse.quote_plus(self.username)
        escaped_password = urllib.parse.quote_plus(self.password)
        return f"mongodb+srv://{escaped_username}:{escaped_password}@{self.cluster}/{self.db_name}?retryWrites=true&w=majority"
    
    def _init_session_state(self):
        """Inicializa variáveis de estado da sessão"""
        if 'last_refresh' not in st.session_state:
            st.session_state.last_refresh = datetime.now()
        if 'auto_refresh' not in st.session_state:
            st.session_state.auto_refresh = False
    
    def _create_refresh_controls(self):
        """Cria controles de atualização dos dados"""
        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
        
        with col1:
            if st.button("🔄 Atualizar Dados", use_container_width=True):
                self.refresh_data()
        
        with col2:
            st.session_state.auto_refresh = st.toggle(
                "Auto-atualização (10 min)",
                value=st.session_state.auto_refresh
            )
        
        with col3:
            last_refresh_str = st.session_state.last_refresh.strftime("%Y-%m-%d %H:%M:%S")
            st.caption(f"Última atualização: {last_refresh_str}")
        
        with col4:
            if st.session_state.auto_refresh:
                next_refresh = st.session_state.last_refresh + timedelta(minutes=10)
                next_refresh_str = next_refresh.strftime("%Y-%m-%d %H:%M:%S")
                st.caption(f"Próxima atualização: {next_refresh_str}")
    
    def refresh_data(self):
        """Atualiza os dados do dashboard"""
        with st.spinner("Atualizando dados..."):
            self.dataframes = self.data_merger.merge_collections()
            st.session_state.last_refresh = datetime.now()
            st.success("Dados atualizados com sucesso!")
    
    def _check_auto_refresh(self):
        """Verifica se é necessário atualizar os dados automaticamente"""
        if (st.session_state.auto_refresh and 
            datetime.now() - st.session_state.last_refresh >= timedelta(minutes=10)):
            self.refresh_data()
    
    def run(self):
        """Executa o dashboard"""
        setup_page()
        st.title("📊 Dashboard MongoDB")
        
        self._create_refresh_controls()
        st.divider()
        
        # Inicializa ou atualiza dados
        if not self.dataframes:
            self.refresh_data()
        self._check_auto_refresh()
        
        # Cria tabs para cada coleção
        tabs = st.tabs(["🆕 Merged Data", "🗃️ NFSPDF", "📄 PO"])
        
        for tab, collection_name in zip(tabs, self.collections):
            with tab:
                df = self.dataframes[collection_name]
                
                if df.is_empty():
                    st.error(f"Nenhum dado encontrado na coleção {collection_name}")
                    continue
                
                col1, col2 = st.columns([1, 4])
                
                with col1:
                    st.metric("Total de Registros", len(df))
                    
                    filter_manager = self.filter_managers[collection_name]
                    filters = filter_manager.create_filters(df)
                    
                    if filters:
                        filtered_df = filter_manager.apply_filters(df, filters)
                        st.metric("Registros Filtrados", len(filtered_df))
                    else:
                        filtered_df = df
                
                with col2:
                    st.dataframe(
                        filtered_df.to_pandas().set_index(filtered_df.columns[0]),
                        use_container_width=True,
                        hide_index=True
                    )

if __name__ == "__main__":
    dashboard = Dashboard()
    dashboard.run()