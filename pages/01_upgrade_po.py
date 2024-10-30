import os
import pandas as pd
import streamlit as st
#import locale
import time
from datetime import datetime
from pathlib import Path

# Set locale for Brazilian Portuguese
#locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')

# Default folder paths
DEFAULT_FOLDERS = [
    {
        'path': r'Q:\Departamentos\Compras\Público\Follow up\Histórico 2024',
        'use_date_filter': False,
        'filter_date': datetime.today()
    },
    {
        'path': r'Q:\Departamentos\Compras\Público\Follow up',
        'use_date_filter': False,
        'filter_date': datetime.today()
    },
    {
        'path': os.path.join(os.path.expanduser('~'), 'Documents\po_files'),
        'use_date_filter': False,
        'filter_date': datetime.today()
    }
]

# Default output folder (now using the third default folder)
DEFAULT_OUTPUT_FOLDER = os.path.join(os.path.expanduser('~'), 'Documents\po_files')

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
        .folder-config {
            background-color: #f0f2f6;
            padding: 1rem;
            border-radius: 0.5rem;
            margin: 0.5rem 0;
        }
    </style>
""", unsafe_allow_html=True)

# Initialize session state for folder management
if 'folders' not in st.session_state:
    st.session_state.folders = DEFAULT_FOLDERS.copy()

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

def main():
    st.title("📊 Processamento de Pedidos de Compra")
    
    # Initialize session state for processed data
    if 'processed_data' not in st.session_state:
        st.session_state.processed_data = None
    
    # Validate folders before processing
    valid_folders = [f for f in st.session_state.folders if os.path.exists(f['path'])]
    invalid_folders = [f for f in st.session_state.folders if not os.path.exists(f['path'])]
    
    if invalid_folders:
        alert = st.warning("⚠️ As seguintes pastas não foram encontradas:")
        time.sleep(1)
        alert.empty()
        for folder in invalid_folders:
            alert = st.warning(f"- {folder['path']}")
            time.sleep(1)
            alert.empty()
    
    # Main process button
    if st.button("🚀 Iniciar Processamento", type="primary", disabled=len(valid_folders) == 0):
        try:
            with st.spinner("Processando dados..."):
                progress_bar = st.progress(0)
                status_container = st.empty()
                
                start_time = time.time()
                all_dfs = []
                
                # Count total files
                total_files = sum(
                    len([f for f in os.listdir(folder['path']) if f.endswith('.xlsx')])
                    for folder in valid_folders
                )
                
                processed_files = 0
                
                # Process each folder
                for folder_config in valid_folders:
                    folder_path = folder_config['path']
                    files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx')]
                    
                    for file in files:
                        file_path = os.path.join(folder_path, file)
                        
                        # Apply date filter if configured
                        if folder_config['use_date_filter']:
                            file_timestamp = os.path.getmtime(file_path)
                            if datetime.fromtimestamp(file_timestamp).date() < folder_config['filter_date']:
                                continue
                        
                        try:
                            status_container.info(f"Processando: {file}")
                            df_temp = pd.read_excel(file_path, engine='openpyxl')
                            if not df_temp.empty:
                                all_dfs.append(df_temp)
                        except Exception as e:
                            alert = st.error(f"Erro ao processar {file}: {str(e)}")
                            time.sleep(1)
                            alert.empty()
                        
                        processed_files += 1
                        progress_bar.progress(processed_files / total_files)
                
                if all_dfs:
                    # Process data
                    df_final = pd.concat(all_dfs, ignore_index=True)
                    df_processed = process_dataframe(df_final)
                    
                    # Save files
                    timestamp = datetime.now().strftime("%d%m%Y_%H%M%S")
                    excel_path = os.path.join(DEFAULT_OUTPUT_FOLDER, f'master_store_{timestamp}.xlsx')
                    
                    os.makedirs(DEFAULT_OUTPUT_FOLDER, exist_ok=True)
                    df_processed.to_excel(excel_path, index=False)               
                    # Store processed data in session state
                    st.session_state.processed_data = df_processed
                    # Show success message
                    elapsed_time = time.time() - start_time
                    st.success(f"""
                    ✅ Processamento concluído com sucesso!
                    
                    **Detalhes:**
                    - Tempo total: {elapsed_time:.2f} segundos
                    - Arquivos processados: {processed_files}
                    
                    **Arquivos gerados:**
                    - Excel: {excel_path}
                    """)
                    
                else:
                    alert = st.warning("⚠️ Nenhum dado encontrado para processar!")
                    time.sleep(1)
                    alert.empty()
        
        except Exception as e:
            st.error(f"❌ Erro durante o processamento: {str(e)}")
    
    # Output configuration
    st.subheader("Configuração de Saída")
    output_folder = st.text_input(
        "Pasta de destino:",
        DEFAULT_OUTPUT_FOLDER
    )
      
    # Folder management
    st.subheader("Configuração de Pastas")
    
    # Add new folder button
    if st.button("➕ Adicionar Nova Pasta"):
        st.session_state.folders.append({
            'path': "",
            'use_date_filter': False,
            'filter_date': datetime.today()
        })
    
    # Display and manage existing folders
    folders_to_remove = []
    cols = st.columns(3)
    for idx, folder in enumerate(st.session_state.folders):
        with cols[idx % 3].expander(f"📁 Pasta {idx + 1}", expanded=True):
            # Folder path input
            new_path = st.text_input(
                "Caminho da pasta:",
                folder['path'],
                key=f"path_{idx}"
            )
            
            # Date filter configuration
            use_filter = st.checkbox(
                "Usar filtro de data",
                folder['use_date_filter'],
                key=f"use_filter_{idx}"
            )
            
            if use_filter:
                filter_date = st.date_input(
                    "Data inicial:",
                    folder['filter_date'],
                    key=f"date_{idx}"
                )
            else:
                filter_date = folder['filter_date']
            
            # Remove folder button (only allow removal of non-default folders)
            #if len(st.session_state.folders) >= len(DEFAULT_FOLDERS) and st.button("🗑️ Remover Pasta", key=f"remove_{idx}"):
            if len(st.session_state.folders) >= len(DEFAULT_FOLDERS) and st.button("🗑️ Remover Pasta", key=f"remove_{idx}"):
                folders_to_remove.append(idx)
            
            # Update folder configuration
            folder['path'] = new_path
            folder['use_date_filter'] = use_filter
            folder['filter_date'] = filter_date
    
    # Remove marked folders
    for idx in reversed(folders_to_remove):
        st.session_state.folders.pop(idx)

if __name__ == "__main__":
    main()