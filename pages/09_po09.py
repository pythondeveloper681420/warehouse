import pandas as pd
import streamlit as st
import time
from datetime import datetime
import io
import gc
import logging
from typing import List, Tuple, Optional, Dict, Any
import numpy as np
import base64

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
MAX_UPLOAD_SIZE_MB = 200
BYTES_PER_MB = 1024 * 1024
CHUNK_SIZE = 10000

# Colunas selecionadas para salvar no arquivo final
SELECTED_COLUMNS = [
    'Purchasing Document', 
    'Item', 
    'Document Date', 
    'PO Creation Date',
    'valor_unitario_formatted', 
    'total_valor_po_liquido_formatted', 
    'total_valor_po_com_impostos_formatted',
    'Order Quantity',
    'total_itens_po'
]

def clear_all_cache():
    """Clear all cache and session state"""
    # Clear session state
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    # Force garbage collection
    gc.collect()
    # Clear Streamlit cache
    st.cache_data.clear()
    st.cache_resource.clear()

@st.cache_data
def process_chunk(_df: pd.DataFrame) -> pd.DataFrame:
    """Process a chunk of data with caching"""
    try:
        chunk_processed = _df.copy()
        
        # Convert numeric columns
        numeric_columns = ['Net order value', 'Order Quantity', 'PBXX Condition Amount']
        for col in numeric_columns:
            if col in chunk_processed.columns:
                chunk_processed[col] = pd.to_numeric(chunk_processed[col], errors='coerce')
                chunk_processed[col] = chunk_processed[col].fillna(0)
        
        # Calculate unit value
        chunk_processed['valor_unitario'] = chunk_processed.apply(
            lambda row: row['Net order value'] / row['Order Quantity'] if row['Order Quantity'] != 0 else 0,
            axis=1
        )
        
        # Calculate value with taxes
        chunk_processed['valor_item_com_impostos'] = (
            chunk_processed['PBXX Condition Amount'] * chunk_processed['Order Quantity']
        )
        
        return chunk_processed
        
    except Exception as e:
        logger.error(f"Error processing chunk: {str(e)}")
        raise

@st.cache_data
def format_currency(value: float) -> str:
    """Format value as Brazilian currency with caching"""
    try:
        if pd.isna(value) or value == '':
            return "R$ 0,00"
        
        if isinstance(value, str):
            value = float(value.replace('.', '').replace(',', '.'))
        
        value = float(value)
        integer_part = int(value)
        decimal_part = int((value - integer_part) * 100)
        
        formatted_integer = '{:,}'.format(integer_part).replace(',', '.')
        return f"R$ {formatted_integer},{decimal_part:02d}"
    except Exception as e:
        logger.warning(f"Error formatting currency value {value}: {str(e)}")
        return "R$ 0,00"

def process_dataframe(df: pd.DataFrame, progress_bar: Any) -> pd.DataFrame:
    """Process the complete DataFrame with progress tracking"""
    try:
        chunk_size = CHUNK_SIZE
        num_chunks = len(df) // chunk_size + 1
        processed_chunks = []
        
        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, len(df))
            chunk = df.iloc[start_idx:end_idx]
            
            processed_chunk = process_chunk(chunk)
            processed_chunks.append(processed_chunk)
            
            progress = (i + 1) / num_chunks
            progress_bar.progress(progress)
        
        df_processed = pd.concat(processed_chunks, ignore_index=True)
        
        # Process groupby operations
        groupby_cols = ['Purchasing Document']
        df_processed['total_valor_po_liquido'] = df_processed.groupby(groupby_cols)['Net order value'].transform('sum')
        df_processed['total_valor_po_com_impostos'] = df_processed.groupby(groupby_cols)['valor_item_com_impostos'].transform('sum')
        df_processed['total_itens_po'] = df_processed.groupby(groupby_cols)['Item'].transform('count')
        
        # Create concatenated key and remove duplicates
        df_processed['concatenada'] = df_processed['Purchasing Document'].astype(str) + df_processed['Item'].astype(str)
        df_processed = df_processed.drop_duplicates(subset=['concatenada'])
        
        # Process Purchasing Document
        df_processed['Purchasing Document'] = pd.to_numeric(df_processed['Purchasing Document'], errors='coerce')
        df_processed = df_processed.dropna(subset=['Purchasing Document'])
        df_processed['Purchasing Document'] = df_processed['Purchasing Document'].astype(int)
        df_processed = df_processed.sort_values(by='Purchasing Document', ascending=False)
        df_processed['PO Creation Date'] = pd.to_datetime(df_processed['Document Date'], dayfirst=True)
        
        # Format currency columns
        currency_columns = [
            'valor_unitario', 'valor_item_com_impostos', 'Net order value',
            'total_valor_po_liquido', 'total_valor_po_com_impostos'
        ]
        
        for col in currency_columns:
            df_processed[f'{col}_formatted'] = df_processed[col].apply(format_currency)
        
        # Process date columns
        date_columns = [
            'Document Date', 'Delivery date', 'Last FUP', 
            'Stat.-Rel. Del. Date', 'Delivery Date', 
            'Requisition Date', 'Inspection Request Date',
            'First Delivery Date', 'Purchase Requisition Delivery Date'
        ]
        
        for col in date_columns:
            if col in df_processed.columns:
                df_processed[col] = pd.to_datetime(
                    df_processed[col],
                    format='%d/%m/%Y',
                    dayfirst=True,
                    errors='coerce'
                )
                df_processed[col] = df_processed[col].dt.strftime('%d/%m/%Y')   
        
        # Select only the desired columns
        df_processed = df_processed[SELECTED_COLUMNS]
        
        return df_processed
    
    except Exception as e:
        logger.error(f"Error in process_dataframe: {str(e)}")
        raise

def to_excel(df: pd.DataFrame) -> str:
    """Convert DataFrame to Excel file and return as base64 string"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    excel_data = output.getvalue()
    return base64.b64encode(excel_data).decode()

def get_download_link(b64_data: str, filename: str) -> str:
    """Generate HTML download link with auto-reset functionality"""
    href = f'data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64_data}'
    return f'''
        <a href="{href}" 
           download="{filename}" 
           class="downloadButton" 
           id="downloadButton"
           onclick="setTimeout(function() {{ 
               window.location.href = window.location.pathname; 
           }}, 1000);">
           📥 Baixar Arquivo Excel Processado
        </a>
        <script>
            document.addEventListener('DOMContentLoaded', function() {{
                const downloadButton = document.getElementById('downloadButton');
                downloadButton.addEventListener('click', function() {{
                    setTimeout(function() {{
                        window.location.reload();
                    }}, 1000);
                }});
            }});
        </script>
    '''

def main():
    """Main application function"""
    st.set_page_config(
        page_title="Sistema de Processamento de PO",
        page_icon="📊",
        layout="wide"
    )
    
    # Custom CSS
    st.markdown("""
        <style>
        .downloadButton {
            background-color: #0075be;
            color: white !important;
            padding: 0.5em 1em;
            text-decoration: none;
            border-radius: 5px;
            border: none;
            display: inline-block;
            width: 100%;
            text-align: center;
            margin: 1em 0;
            font-weight: 500;
        }
        .downloadButton:hover {
            background-color: #4098ce;
            color: white !important;
            text-decoration: none;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Initialize session state if needed
    if 'processed' not in st.session_state:
        st.session_state.processed = False
        st.session_state.download_ready = False
    
    st.title("📊 Sistema de Processamento de Pedidos de Compra")
    
    # File upload
    uploaded_files = st.file_uploader(
        "Selecione os arquivos Excel para processar",
        type=['xlsx'],
        accept_multiple_files=True,
        help="Você pode selecionar múltiplos arquivos Excel (.xlsx)"
    )
    
    if uploaded_files:
        total_size = sum(file.size for file in uploaded_files) / BYTES_PER_MB
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("📦 Tamanho total", f"{total_size:.1f}MB")
        with col2:
            st.metric("📄 Arquivos", len(uploaded_files))
        
        if st.button("🚀 Processar Arquivos", use_container_width=True):
            try:
                with st.spinner("Processando arquivos..."):
                    progress_bar = st.progress(0)
                    start_time = time.time()
                    
                    # Process files
                    all_dfs = []
                    for idx, file in enumerate(uploaded_files):
                        df = pd.read_excel(file, engine='openpyxl')
                        if not df.empty:
                            all_dfs.append(df)
                        progress_bar.progress((idx + 1) / len(uploaded_files))
                    
                    if all_dfs:
                        # Process data
                        df_final = pd.concat(all_dfs, ignore_index=True)
                        df_processed = process_dataframe(df_final, progress_bar)
                        
                        # Generate download data
                        timestamp = datetime.now().strftime("%d%m%Y_%H%M%S")
                        filename = f'processamento_po_{timestamp}.xlsx'
                        excel_data = to_excel(df_processed)
                        
                        # Show success message and metrics
                        elapsed_time = time.time() - start_time
                        st.success("✅ Processamento concluído!")
                        
                        col1, col2, col3 = st.columns(3)
                        col1.metric("⏱️ Tempo", f"{elapsed_time:.2f}s")
                        col2.metric("📊 Registros", len(df_processed))
                        col3.metric("📁 Arquivos", len(uploaded_files))
                        
                        # Show download button
                        st.markdown(get_download_link(excel_data, filename), unsafe_allow_html=True)
                        
                        # Set session state
                        st.session_state.processed = True
                        st.session_state.download_ready = True
                    
                    else:
                        st.warning("⚠️ Nenhum dado encontrado para processar!")
                
            except Exception as e:
                logger.error(f"Error: {str(e)}")
                st.error(f"❌ Erro no processamento: {str(e)}")
                clear_all_cache()
    
    # Add manual reset button
    if st.session_state.processed:
        if st.button("🔄 Recomeçar", use_container_width=True):
            clear_all_cache()
            st.rerun()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        st.error("❌ Erro inesperado. Por favor, tente novamente.")
        clear_all_cache()