import pandas as pd
import streamlit as st
import time
from datetime import datetime
import io

# Constants
MAX_UPLOAD_SIZE_MB = 200  # Maximum upload size in MB
BYTES_PER_MB = 1024 * 1024

# Page configuration
st.set_page_config(
    page_title="Purchase Order Processing",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for styling
st.markdown("""
    <style>
        .stButton button {
            width: 100%;
        }
        .block-container {
            padding-top: 2rem;
        }
        .file-uploader {
            background-color: #f0f2f6;
            padding: 1rem;
            border-radius: 0.5rem;
            margin: 0.5rem 0;
        }
        .download-button {
            margin-top: 1rem;
        }
        .upload-status {
            margin-top: 0.5rem;
            font-size: 0.9em;
            color: #666;
        }
        iframe {
            display: none;
        }
    </style>
""", unsafe_allow_html=True)

def format_currency(value):
    """Format value as Brazilian currency without using locale"""
    try:
        if pd.isna(value) or value == '':
            return "R$ 0,00"
        
        # Convert to float if string
        if isinstance(value, str):
            value = float(value.replace('.', '').replace(',', '.'))
            
        # Format the number
        value = float(value)
        integer_part = int(value)
        decimal_part = int((value - integer_part) * 100)
        
        # Convert to Brazilian format
        formatted_integer = '{:,}'.format(integer_part).replace(',', '.')
        formatted_value = f"R$ {formatted_integer},{decimal_part:02d}"
        
        return formatted_value
    except:
        return "R$ 0,00"

def process_dataframe(df):
    """Process DataFrame with all necessary calculations"""
    df_processed = df.copy()
    
    # Convert numeric columns
    numeric_columns = ['Net order value', 'Order Quantity', 'PBXX Condition Amount']
    for col in numeric_columns:
        if col in df_processed.columns:
            df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce')
            df_processed[col].fillna(0, inplace=True)
    
    # Calculate values
    df_processed['valor_unitario'] = df_processed.apply(
        lambda row: row['Net order value'] / row['Order Quantity'] if row['Order Quantity'] != 0 else 0,
        axis=1
    )
    
    df_processed['valor_item_com_impostos'] = df_processed['PBXX Condition Amount'] * df_processed['Order Quantity']
    
    # Calculate PO totals
    df_processed['total_valor_po_liquido'] = df_processed.groupby('Purchasing Document')['Net order value'].transform('sum')
    df_processed['total_valor_po_com_impostos'] = df_processed.groupby('Purchasing Document')['valor_item_com_impostos'].transform('sum')
    df_processed['total_itens_po'] = df_processed.groupby('Purchasing Document')['Item'].transform('count')
    
    # Concatenate columns and create a new column
    df_processed['concatenada'] = df_processed['Purchasing Document'].astype(str) + df_processed['Item'].astype(str)
    df_processed = df_processed.drop_duplicates(subset=['concatenada'])
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
        df_processed[f'{col}_formatted'] = df_processed[col].apply(format_currency)
    
    # Process dates
    date_columns = [
        'Document Date', 'Delivery date', 'Last FUP', 
        'Stat.-Rel. Del. Date', 'Delivery Date', 
        'Requisition Date', 'Inspection Request Date',
        'First Delivery Date', 'Purchase Requisition Delivery Date'
    ]
    
    for col in date_columns:
        if col in df_processed.columns:
            df_processed[col] = pd.to_datetime(df_processed[col], errors='coerce')
            df_processed[col] = df_processed[col].dt.strftime('%d/%m/%Y')
    
    return df_processed

def to_excel(df):
    """Convert DataFrame to Excel file in memory"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return output

def calculate_total_size(files):
    """Calculate total size of uploaded files in MB"""
    return sum(file.size for file in files) / BYTES_PER_MB

def main():
    st.title("📊 Processamento de Pedidos de Compra")
    
    # Initialize session state
    if 'processed_data' not in st.session_state:
        st.session_state.processed_data = None
    if 'download_filename' not in st.session_state:
        st.session_state.download_filename = None
    if 'total_size' not in st.session_state:
        st.session_state.total_size = 0
    
    # File upload section with size tracking
    st.subheader("Seleção de Arquivos")
    
    # Create two columns for the upload area
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
            total_size = calculate_total_size(uploaded_files)
            st.session_state.total_size = total_size
            remaining_size = MAX_UPLOAD_SIZE_MB - total_size
            
            st.info(f"""
            📦 Espaço utilizado: {total_size:.1f}MB
            ⚡ Espaço disponível: {remaining_size:.1f}MB
            """)
    
    # Main process button
    if st.button("🚀 Iniciar Processamento", type="primary", disabled=not uploaded_files):
        try:
            with st.spinner("Processando dados..."):
                progress_bar = st.progress(0)
                status_container = st.empty()
                
                start_time = time.time()
                all_dfs = []
                total_files = len(uploaded_files)
                processed_files = 0
                
                for uploaded_file in uploaded_files:
                    try:
                        status_container.info(f"Processando: {uploaded_file.name}")
                        df_temp = pd.read_excel(uploaded_file, engine='openpyxl')
                        if not df_temp.empty:
                            all_dfs.append(df_temp)
                    except Exception as e:
                        st.error(f"Erro ao processar {uploaded_file.name}: {str(e)}")
                        time.sleep(1)
                    
                    processed_files += 1
                    progress_bar.progress(processed_files / total_files)
                
                if all_dfs:
                    df_final = pd.concat(all_dfs, ignore_index=True)
                    df_processed = process_dataframe(df_final)
                    st.session_state.processed_data = df_processed
                    
                    timestamp = datetime.now().strftime("%d%m%Y_%H%M%S")
                    st.session_state.download_filename = f'master_store_{timestamp}.xlsx'
                    
                    elapsed_time = time.time() - start_time
                    st.success(f"""
                    ✅ Processamento concluído com sucesso!
                    
                    **Detalhes:**
                    - Tempo total: {elapsed_time:.2f} segundos
                    - Arquivos processados: {processed_files}
                    """)
                else:
                    st.warning("⚠️ Nenhum dado encontrado para processar!")
        
        except Exception as e:
            st.error(f"❌ Erro durante o processamento: {str(e)}")
    
    # Download section with no-reload functionality
    if st.session_state.processed_data is not None:
        st.subheader("Download do Arquivo Processado")
        
        excel_data = to_excel(st.session_state.processed_data)
        
        # Create a container for the download button
        download_container = st.container()
        
        with download_container:
            st.download_button(
                label="📥 Baixar Arquivo Excel",
                data=excel_data,
                file_name=st.session_state.download_filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                help="Clique para baixar o arquivo Excel processado",
                use_container_width=True,
            )

if __name__ == "__main__":
    main()