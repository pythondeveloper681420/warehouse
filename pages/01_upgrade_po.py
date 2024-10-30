import pandas as pd
import streamlit as st
#import locale
import time
from datetime import datetime
import io

# Set locale for Brazilian Portuguese
#locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')

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
    </style>
""", unsafe_allow_html=True)

def format_currency(value):
    """Format value as Brazilian currency"""
    try:
        return f"R$ {value:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
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
    # Remove duplicates based on the concatenated column
    df_processed = df_processed.drop_duplicates(subset=['concatenada']) 
    # Keep only numeric values
    df_processed['Purchasing Document'] = pd.to_numeric(df_processed['Purchasing Document'], errors='coerce')

    # Remove NaN (non-numeric) values
    df_processed = df_processed.dropna(subset=['Purchasing Document'])

    # Convert the column to integer, if necessary
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

def main():
    st.title("📊 Processamento de Pedidos de Compra")
    
    # Initialize session state for processed data and download filename
    if 'processed_data' not in st.session_state:
        st.session_state.processed_data = None
    if 'download_filename' not in st.session_state:
        st.session_state.download_filename = None
    
    # File upload section
    st.subheader("Seleção de Arquivos")
    uploaded_files = st.file_uploader(
        "Selecione os arquivos Excel para processar",
        type=['xlsx'],
        accept_multiple_files=True,
        help="Você pode selecionar múltiplos arquivos Excel (.xlsx)"
    )
    
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
                
                # Process each uploaded file
                for uploaded_file in uploaded_files:
                    try:
                        status_container.info(f"Processando: {uploaded_file.name}")
                        df_temp = pd.read_excel(uploaded_file, engine='openpyxl')
                        if not df_temp.empty:
                            all_dfs.append(df_temp)
                    except Exception as e:
                        alert = st.error(f"Erro ao processar {uploaded_file.name}: {str(e)}")
                        time.sleep(1)
                        alert.empty()
                    
                    processed_files += 1
                    progress_bar.progress(processed_files / total_files)
                
                if all_dfs:
                    # Process data
                    df_final = pd.concat(all_dfs, ignore_index=True)
                    df_processed = process_dataframe(df_final)
                    
                    # Store processed data in session state
                    st.session_state.processed_data = df_processed
                    
                    # Generate download filename
                    timestamp = datetime.now().strftime("%d%m%Y_%H%M%S")
                    st.session_state.download_filename = f'master_store_{timestamp}.xlsx'
                    
                    # Show success message
                    elapsed_time = time.time() - start_time
                    st.success(f"""
                    ✅ Processamento concluído com sucesso!
                    
                    **Detalhes:**
                    - Tempo total: {elapsed_time:.2f} segundos
                    - Arquivos processados: {processed_files}
                    
                    Use o botão de download abaixo para baixar o arquivo processado.
                    """)
                    
                else:
                    alert = st.warning("⚠️ Nenhum dado encontrado para processar!")
                    time.sleep(1)
                    alert.empty()
        
        except Exception as e:
            st.error(f"❌ Erro durante o processamento: {str(e)}")
    
    # Download section - only show if there's processed data
    if st.session_state.processed_data is not None:
        st.subheader("Download do Arquivo Processado")
        
        # Create Excel file in memory
        excel_data = to_excel(st.session_state.processed_data)
        
        # Download button
        st.download_button(
            label="📥 Baixar Arquivo Excel",
            data=excel_data,
            file_name=st.session_state.download_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            help="Clique para baixar o arquivo Excel processado"
        )

if __name__ == "__main__":
    main()