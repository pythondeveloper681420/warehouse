import streamlit as st
import polars as pl
from pymongo import MongoClient
import urllib.parse
from datetime import datetime
from bson.objectid import ObjectId

# Configuração da página
st.set_page_config(
    page_title="Dashboard MongoDB",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Inicialização do session state
if 'filters' not in st.session_state:
    st.session_state.filters = {}

def convert_objectid_to_str(documents):
    """Converte ObjectId para string no documento MongoDB"""
    for doc in documents:
        for key, value in doc.items():
            if isinstance(value, ObjectId):
                doc[key] = str(value)
    return documents

def infer_and_convert_types(documents):
    """Infere e converte tipos dos documentos"""
    if not documents:
        return documents
    
    for doc in documents:
        for key, value in doc.items():
            if isinstance(value, str):
                # Tenta converter datas
                try:
                    doc[key] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                except ValueError:
                    pass
                    
                # Tenta converter números
                try:
                    if value.replace('.', '', 1).isdigit():
                        doc[key] = float(value) if '.' in value else int(value)
                except ValueError:
                    pass
    
    return documents

@st.cache_data
def get_mongo_data(collection_name):
    """Busca dados do MongoDB"""
    try:
        username = 'devpython86'
        password = 'dD@pjl06081420'
        cluster = 'cluster0.akbb8.mongodb.net'
        db_name = 'warehouse'
        
        # Conecta ao MongoDB
        mongo_uri = f"mongodb+srv://{urllib.parse.quote_plus(username)}:{urllib.parse.quote_plus(password)}@{cluster}/{db_name}?retryWrites=true&w=majority"
        client = MongoClient(mongo_uri)
        db = client[db_name]
        
        # Busca documentos
        documents = list(db[collection_name].find())
        documents = convert_objectid_to_str(documents)
        documents = infer_and_convert_types(documents)
        
        # Converte para DataFrame
        if documents:
            return pl.DataFrame(documents)
        return pl.DataFrame()
        
    except Exception as e:
        st.error(f"Erro ao carregar dados: {str(e)}")
        return pl.DataFrame()

def merge_collections():
    """Realiza o merge das coleções"""
    try:
        # Carrega dados
        xml_df = get_mongo_data('xml')
        po_df = get_mongo_data('po')
        nfspdf_df = get_mongo_data('nfspdf')
        
        # Processa PO
        if not po_df.is_empty():
            po_df = po_df.unique(subset=['Purchasing Document'])
            po_df = po_df.with_columns([
                pl.col('Purchasing Document')
                .cast(pl.Utf8)
                .str.replace_all(r'\D', '')
                .alias('Purchasing Document_cleaned')
            ])
            po_df = po_df.select([
                'Purchasing Document_cleaned',
                'Project Code',
                'Andritz WBS Element',
                'Cost Center'
            ])
        
        # Merge XML + PO
        if not xml_df.is_empty() and not po_df.is_empty():
            xml_df = xml_df.with_columns([
                pl.col('po')
                .cast(pl.Utf8)
                .str.replace_all(r'\D', '')
                .str.replace(r'\.0$', '')
                .alias('po_cleaned')
            ])
            
            merged_xml_po = xml_df.join(
                po_df,
                left_on='po_cleaned',
                right_on='Purchasing Document_cleaned',
                how='left'
            ).sort(by=['dtEmi', 'nNf', 'itemNf'], descending=[True, False, False])
        else:
            merged_xml_po = pl.DataFrame()
            
        # Merge NFSPDF + PO
        if not nfspdf_df.is_empty() and not po_df.is_empty():
            if 'po' in nfspdf_df.columns:
                nfspdf_df = nfspdf_df.with_columns([
                    pl.col('po')
                    .cast(pl.Utf8)
                    .str.replace_all(r'\D', '')
                    .alias('po_cleaned')
                ])
                
                merged_nfspdf_po = nfspdf_df.join(
                    po_df,
                    left_on='po_cleaned',
                    right_on='Purchasing Document_cleaned',
                    how='left'
                ).sort(by=['Data Emissão'], descending=True)
            else:
                merged_nfspdf_po = nfspdf_df
        else:
            merged_nfspdf_po = pl.DataFrame()
            
        return {
            'merged_data': merged_xml_po,
            'merged_nfspdf': merged_nfspdf_po,
            'po': po_df
        }
        
    except Exception as e:
        st.error(f"Erro ao realizar merge: {str(e)}")
        return {
            'merged_data': pl.DataFrame(),
            'merged_nfspdf': pl.DataFrame(),
            'po': pl.DataFrame()
        }

def create_filters(df, collection_name):
    """Cria filtros para o DataFrame"""
    if df.is_empty():
        return {}
    
    # Seleciona colunas para filtrar
    columns = st.multiselect(
        "Selecione as colunas para filtrar:",
        df.columns,
        key=f"cols_{collection_name}"
    )
    
    filters = {}
    
    # Cria filtros para cada coluna selecionada
    for column in columns:
        st.markdown(f"### {column}")
        
        # Estado do filtro
        filter_key = f"filter_{collection_name}_{column}"
        if filter_key not in st.session_state:
            st.session_state[filter_key] = {"type": "contains", "values": []}
        
        # Opções de filtro
        filter_type = st.radio(
            "Tipo de filtro:",
            ["contains", "equals", "range"],
            key=f"type_{collection_name}_{column}",
            horizontal=True
        )
        
        # Atualiza o tipo de filtro no session state
        st.session_state[filter_key]["type"] = filter_type
        
        # Cria o filtro apropriado baseado no tipo selecionado
        if filter_type == "contains":
            value = st.text_input(
                "Digite o valor para buscar:",
                key=f"contains_{collection_name}_{column}"
            )
            if value:
                filters[column] = ("contains", value)
        
        elif filter_type == "equals":
            unique_values = df[column].unique().to_list()
            try:
                unique_values = sorted(unique_values)
            except:
                pass
            
            values = st.multiselect(
                "Selecione os valores exatos:",
                unique_values,
                key=f"equals_{collection_name}_{column}",
                default=st.session_state[filter_key].get("values", [])
            )
            if values:
                filters[column] = ("equals", values)
                st.session_state[filter_key]["values"] = values
        
        elif filter_type == "range":
            try:
                min_val = df[column].min()
                max_val = df[column].max()
                
                if isinstance(min_val, (int, float)):
                    range_values = st.slider(
                        "Selecione o intervalo:",
                        min_value=float(min_val),
                        max_value=float(max_val),
                        value=(float(min_val), float(max_val)),
                        key=f"range_{collection_name}_{column}"
                    )
                    if range_values != (min_val, max_val):
                        filters[column] = ("range", range_values)
            except:
                st.warning("Filtro de intervalo não disponível para esta coluna")
    
    return filters

def apply_filters(df, filters):
    """Aplica filtros ao DataFrame"""
    if not filters:
        return df
    
    filtered_df = df
    
    for column, (filter_type, value) in filters.items():
        try:
            if filter_type == "contains":
                filtered_df = filtered_df.filter(
                    pl.col(column).cast(pl.Utf8).str.contains(str(value), case_sensitive=False)
                )
            elif filter_type == "equals":
                filtered_df = filtered_df.filter(pl.col(column).is_in(value))
            elif filter_type == "range":
                min_val, max_val = value
                filtered_df = filtered_df.filter(
                    (pl.col(column) >= min_val) & (pl.col(column) <= max_val)
                )
        except Exception as e:
            st.error(f"Erro ao aplicar filtro na coluna {column}: {str(e)}")
    
    return filtered_df

def main():
    st.title("📊 MongoDB Dashboard")
    
    # Carrega e merge as coleções
    dataframes = merge_collections()
    
    # Cria tabs
    tabs = st.tabs(["🆕 Merged Data", "🗃️ NFSPDF", "📄 PO"])
    collections = ['merged_data', 'merged_nfspdf', 'po']
    
    # Processa cada tab
    for tab, collection_name in zip(tabs, collections):
        with tab:
            df = dataframes[collection_name]
            
            if df.is_empty():
                st.error(f"Nenhum dado encontrado na coleção {collection_name}")
                continue
            
            # Layout em colunas
            col1, col2 = st.columns([1, 4])
            
            with col1:
                st.metric("Total de Registros", len(df))
                
                # Cria e aplica filtros
                filters = create_filters(df, collection_name)
                
                if filters:
                    filtered_df = apply_filters(df, filters)
                    st.metric("Registros Filtrados", len(filtered_df))
                else:
                    filtered_df = df
                
                # Botão para limpar filtros
                if st.button("Limpar Filtros", key=f"clear_{collection_name}"):
                    for key in list(st.session_state.keys()):
                        if key.startswith(f"filter_{collection_name}"):
                            del st.session_state[key]
                    st.rerun()
            
            with col2:
                st.dataframe(
                    filtered_df.to_pandas(),
                    use_container_width=True
                )

if __name__ == "__main__":
    main()