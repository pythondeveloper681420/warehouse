import pandas as pd
import streamlit as st
import time
from datetime import datetime
import io
import gc
import logging
from typing import List, Tuple, Optional, Dict, Any
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
MAX_UPLOAD_SIZE_MB = 200
BYTES_PER_MB = 1024 * 1024
CHUNK_SIZE = 10000  # Number of rows to process at once

# Page configuration and styling
def setup_page():
    """Configure page settings and styling"""
    st.set_page_config(
        page_title="Sistema de Processamento de PO",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.markdown("""
        <style>
            .stButton button {
                width: 100%;
                height: 3em;
                background-color: #4CAF50;
                color: white;
            }
            .stButton button:hover {
                background-color: #45a049;
            }
            .block-container {
                padding-top: 2rem;
            }
            .file-uploader {
                background-color: #f8f9fa;
                padding: 2rem;
                border-radius: 0.5rem;
                border: 2px dashed #dee2e6;
            }
            .status-box {
                padding: 1rem;
                border-radius: 0.5rem;
                margin: 1rem 0;
            }
            .success-box {
                background-color: #d4edda;
                border: 1px solid #c3e6cb;
                color: #155724;
            }
            .warning-box {
                background-color: #fff3cd;
                border: 1px solid #ffeeba;
                color: #856404;
            }
            .error-box {
                background-color: #f8d7da;
                border: 1px solid #f5c6cb;
                color: #721c24;
            }
            .metrics-container {
                background-color: #f8f9fa;
                padding: 1rem;
                border-radius: 0.5rem;
                margin: 1rem 0;
            }
            .header-style {
                font-size: 2.5rem;
                font-weight: bold;
                color: #1a237e;
                margin-bottom: 2rem;
            }
            .subheader-style {
                font-size: 1.5rem;
                color: #283593;
                margin: 1.5rem 0;
            }
        </style>
    """, unsafe_allow_html=True)

class DataProcessor:
    """Class to handle all data processing operations"""
    
    @staticmethod
    def format_currency(value: float) -> str:
        """Format value as Brazilian currency"""
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

    @staticmethod
    def safe_division(x: float, y: float) -> float:
        """Safely perform division handling zero division"""
        try:
            return x / y if y != 0 else 0
        except:
            return 0

    @staticmethod
    def process_chunk(df: pd.DataFrame) -> pd.DataFrame:
        """Process a chunk of data"""
        try:
            # Create a copy to avoid chained assignment warnings
            chunk_processed = df.copy()
            
            # Convert numeric columns
            numeric_columns = ['Net order value', 'Order Quantity', 'PBXX Condition Amount']
            for col in numeric_columns:
                if col in chunk_processed.columns:
                    chunk_processed[col] = pd.to_numeric(chunk_processed[col], errors='coerce')
                    chunk_processed[col] = chunk_processed[col].fillna(0)
            
            # Calculate values using vectorized operations
            chunk_processed['valor_unitario'] = chunk_processed.apply(
                lambda row: DataProcessor.safe_division(row['Net order value'], row['Order Quantity']),
                axis=1
            )
            
            chunk_processed['valor_item_com_impostos'] = (
                chunk_processed['PBXX Condition Amount'] * chunk_processed['Order Quantity']
            )
            
            return chunk_processed
            
        except Exception as e:
            logger.error(f"Error processing chunk: {str(e)}")
            raise

    @staticmethod
    def process_dataframe(df: pd.DataFrame, progress_bar: Any) -> pd.DataFrame:
        """Process the complete DataFrame with progress tracking"""
        try:
            # Process in chunks
            chunk_size = CHUNK_SIZE
            num_chunks = len(df) // chunk_size + 1
            processed_chunks = []
            
            for i in range(num_chunks):
                start_idx = i * chunk_size
                end_idx = min((i + 1) * chunk_size, len(df))
                chunk = df.iloc[start_idx:end_idx]
                
                # Process chunk
                processed_chunk = DataProcessor.process_chunk(chunk)
                processed_chunks.append(processed_chunk)
                
                # Update progress
                progress = (i + 1) / num_chunks
                progress_bar.progress(progress)
                
            # Combine processed chunks
            df_processed = pd.concat(processed_chunks, ignore_index=True)
            
            # Calculate PO totals
            groupby_cols = ['Purchasing Document']
            df_processed['total_valor_po_liquido'] = df_processed.groupby(groupby_cols)['Net order value'].transform('sum')
            df_processed['total_valor_po_com_impostos'] = df_processed.groupby(groupby_cols)['valor_item_com_impostos'].transform('sum')
            df_processed['total_itens_po'] = df_processed.groupby(groupby_cols)['Item'].transform('count')
            
            # Handle concatenation and duplicates
            df_processed['concatenada'] = (
                df_processed['Purchasing Document'].astype(str) + 
                df_processed['Item'].astype(str)
            )
            df_processed = df_processed.drop_duplicates(subset=['concatenada'])
            
            # Process Purchasing Document
            df_processed['Purchasing Document'] = pd.to_numeric(df_processed['Purchasing Document'], errors='coerce')
            df_processed = df_processed.dropna(subset=['Purchasing Document'])
            df_processed['Purchasing Document'] = df_processed['Purchasing Document'].astype(int)
            df_processed = df_processed.sort_values(by='Purchasing Document', ascending=False)
            
            # Format currency columns
            currency_columns = [
                'valor_unitario', 'valor_item_com_impostos', 'Net order value',
                'total_valor_po_liquido', 'total_valor_po_com_impostos'
            ]
            
            for col in currency_columns:
                df_processed[f'{col}_formatted'] = df_processed[col].apply(DataProcessor.format_currency)
            
            # Process dates
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
            
            return df_processed
            
        except Exception as e:
            logger.error(f"Error in process_dataframe: {str(e)}")
            raise

class FileHandler:
    """Class to handle file operations"""
    
    @staticmethod
    def calculate_total_size(files: List[Any]) -> float:
        """Calculate total size of uploaded files in MB"""
        return sum(file.size for file in files) / BYTES_PER_MB

    @staticmethod
    def to_excel(df: pd.DataFrame) -> io.BytesIO:
        """Convert DataFrame to Excel file in memory"""
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        output.seek(0)
        return output

    @staticmethod
    def read_excel_file(file: Any) -> Optional[pd.DataFrame]:
        """Safely read Excel file"""
        try:
            return pd.read_excel(file, engine='openpyxl')
        except Exception as e:
            logger.error(f"Error reading file {file.name}: {str(e)}")
            return None

def main():
    """Main application function"""
    setup_page()
    
    # Initialize session state
    if 'processed_data' not in st.session_state:
        st.session_state.processed_data = None
    if 'download_filename' not in st.session_state:
        st.session_state.download_filename = None
    
    # Header
    st.markdown('<h1 class="header-style">📊 Sistema de Processamento de Pedidos de Compra</h1>', unsafe_allow_html=True)
    
    # File upload section
    st.markdown('<h2 class="subheader-style">📁 Seleção de Arquivos</h2>', unsafe_allow_html=True)
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        uploaded_files = st.file_uploader(
            "Selecione os arquivos Excel para processar",
            type=['xlsx'],
            accept_multiple_files=True,
            help="Você pode selecionar múltiplos arquivos Excel (.xlsx)"
        )
    
    with col2:
        if uploaded_files:
            total_size = FileHandler.calculate_total_size(uploaded_files)
            remaining_size = MAX_UPLOAD_SIZE_MB - total_size
            
            st.markdown(
                f"""
                <div class="metrics-container">
                    <p>📦 Espaço utilizado: {total_size:.1f}MB</p>
                    <p>⚡ Espaço disponível: {remaining_size:.1f}MB</p>
                </div>
                """,
                unsafe_allow_html=True
            )
    
    # Process button
    if st.button("🚀 Iniciar Processamento", type="primary", disabled=not uploaded_files):
        try:
            with st.spinner("Processando arquivos..."):
                progress_bar = st.progress(0)
                status_container = st.empty()
                
                start_time = time.time()
                all_dfs = []
                
                # Process each file
                for idx, uploaded_file in enumerate(uploaded_files):
                    status_container.info(f"Processando: {uploaded_file.name}")
                    df_temp = FileHandler.read_excel_file(uploaded_file)
                    
                    if df_temp is not None and not df_temp.empty:
                        all_dfs.append(df_temp)
                    
                    progress_bar.progress((idx + 1) / len(uploaded_files))
                
                # Combine and process all data
                if all_dfs:
                    df_final = pd.concat(all_dfs, ignore_index=True)
                    df_processed = DataProcessor.process_dataframe(df_final, progress_bar)
                    
                    # Store processed data
                    st.session_state.processed_data = df_processed
                    timestamp = datetime.now().strftime("%d%m%Y_%H%M%S")
                    st.session_state.download_filename = f'processamento_po_{timestamp}.xlsx'
                    
                    # Show success message
                    elapsed_time = time.time() - start_time
                    st.markdown(
                        f"""
                        <div class="status-box success-box">
                            <h3>✅ Processamento concluído com sucesso!</h3>
                            <p>Tempo total: {elapsed_time:.2f} segundos</p>
                            <p>Arquivos processados: {len(uploaded_files)}</p>
                            <p>Registros processados: {len(df_processed)}</p>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
                else:
                    st.markdown(
                        '<div class="status-box warning-box">⚠️ Nenhum dado encontrado para processar!</div>',
                        unsafe_allow_html=True
                    )
                
                # Clear memory
                gc.collect()
        
        except Exception as e:
            logger.error(f"Error during processing: {str(e)}")
            st.markdown(
                f'<div class="status-box error-box">❌ Erro durante o processamento: {str(e)}</div>',
                unsafe_allow_html=True
            )
    
    # Download section
    if st.session_state.processed_data is not None:
        st.markdown('<h2 class="subheader-style">📥 Download do Arquivo Processado</h2>', unsafe_allow_html=True)
        
        excel_data = FileHandler.to_excel(st.session_state.processed_data)
        
        st.download_button(
            label="📥 Baixar Arquivo Excel Processado",
            data=excel_data,
            file_name=st.session_state.download_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            help="Clique para baixar o arquivo Excel processado",
            use_container_width=True,
        )

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Application error: {str(e)}")
        st.error("Ocorreu um erro inesperado. Por favor, tente novamente.")