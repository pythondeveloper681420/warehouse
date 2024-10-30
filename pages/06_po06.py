import pandas as pd
import streamlit as st
from datetime import datetime
import io
import base64

# Configuração da página
st.set_page_config(
    page_title="Processamento de Pedidos de Compra",
    page_icon="📊",
    layout="wide"
)

# Estilo CSS
st.markdown("""
    <style>
        .stButton > button {
            width: 100%;
            margin-top: 1rem;
        }
        .stProgress > div > div > div > div {
            background-color: #00cc66;
        }
    </style>
""", unsafe_allow_html=True)

def format_currency(value):
    """Formata valor para moeda brasileira"""
    try:
        if pd.isna(value) or value == '':
            return "R$ 0,00"
        
        value = float(str(value).replace('.', '').replace(',', '.')) if isinstance(value, str) else float(value)
        return f"R$ {value:,.2f}".replace(',', 'v').replace('.', ',').replace('v', '.')
    except:
        return "R$ 0,00"

def process_dataframe(df):
    """Processa o DataFrame com todos os cálculos necessários"""
    df_processed = df.copy()
    
    # Conversão de colunas numéricas
    numeric_cols = ['Net order value', 'Order Quantity', 'PBXX Condition Amount']
    for col in numeric_cols:
        if col in df_processed.columns:
            df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce').fillna(0)
    
    # Cálculos principais
    df_processed['valor_unitario'] = df_processed.apply(
        lambda row: row['Net order value'] / row['Order Quantity'] if row['Order Quantity'] != 0 else 0,
        axis=1
    )
    
    df_processed['valor_item_com_impostos'] = df_processed['PBXX Condition Amount'] * df_processed['Order Quantity']
    
    # Totais por PO
    df_processed['total_valor_po_liquido'] = df_processed.groupby('Purchasing Document')['Net order value'].transform('sum')
    df_processed['total_valor_po_com_impostos'] = df_processed.groupby('Purchasing Document')['valor_item_com_impostos'].transform('sum')
    df_processed['total_itens_po'] = df_processed.groupby('Purchasing Document')['Item'].transform('count')
    
    # Tratamento de dados
    df_processed['concatenada'] = df_processed['Purchasing Document'].astype(str) + df_processed['Item'].astype(str)
    df_processed = df_processed.drop_duplicates(subset=['concatenada'])
    df_processed['Purchasing Document'] = pd.to_numeric(df_processed['Purchasing Document'], errors='coerce')
    df_processed = df_processed.dropna(subset=['Purchasing Document'])
    df_processed['Purchasing Document'] = df_processed['Purchasing Document'].astype(int)
    df_processed = df_processed.sort_values(by='Purchasing Document', ascending=False)
    
    # Formatação de moeda
    currency_cols = [
        'valor_unitario', 'valor_item_com_impostos', 'Net order value',
        'total_valor_po_liquido', 'total_valor_po_com_impostos'
    ]
    for col in currency_cols:
        df_processed[f'{col}_formatted'] = df_processed[col].apply(format_currency)
    
    # Formatação de datas
    date_cols = [
        'Document Date', 'Delivery date', 'Last FUP', 
        'Stat.-Rel. Del. Date', 'Delivery Date', 
        'Requisition Date', 'Inspection Request Date',
        'First Delivery Date', 'Purchase Requisition Delivery Date'
    ]
    for col in date_cols:
        if col in df_processed.columns:
            df_processed[col] = pd.to_datetime(df_processed[col], errors='coerce')
            df_processed[col] = df_processed[col].dt.strftime('%d/%m/%Y')
    
    return df_processed

def get_excel_download_link(df, filename):
    """Gera link de download para arquivo Excel"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    excel_data = output.getvalue()
    b64 = base64.b64encode(excel_data).decode()
    href = f'data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}'
    return href

def main():
    st.title("📊 Processamento de Pedidos de Compra")
    
    # Upload de arquivos
    st.subheader("📁 Upload de Arquivos")
    uploaded_files = st.file_uploader(
        "Selecione os arquivos Excel para processar",
        type=['xlsx'],
        accept_multiple_files=True,
        help="Você pode selecionar múltiplos arquivos Excel (.xlsx)"
    )
    
    if uploaded_files:
        if st.button("🚀 Processar Arquivos", type="primary"):
            try:
                with st.spinner("Processando arquivos..."):
                    progress_bar = st.progress(0)
                    all_dfs = []
                    
                    for i, file in enumerate(uploaded_files):
                        df_temp = pd.read_excel(file)
                        all_dfs.append(df_temp)
                        progress_bar.progress((i + 1) / len(uploaded_files))
                    
                    if all_dfs:
                        df_final = pd.concat(all_dfs, ignore_index=True)
                        df_processed = process_dataframe(df_final)
                        
                        # Gerar nome do arquivo
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        filename = f'processamento_pedidos_{timestamp}.xlsx'
                        
                        # Criar link de download
                        excel_href = get_excel_download_link(df_processed, filename)
                        
                        st.success("✅ Processamento concluído com sucesso!")
                        
                        # Botão de download usando HTML
                        st.markdown(f'''
                            <a href="{excel_href}" download="{filename}">
                                <button style="
                                    background-color: #00cc66;
                                    color: white;
                                    padding: 12px 20px;
                                    border: none;
                                    border-radius: 4px;
                                    cursor: pointer;
                                    width: 100%;
                                    font-size: 16px;
                                    margin-top: 10px;">
                                    📥 Baixar Arquivo Processado
                                </button>
                            </a>
                        ''', unsafe_allow_html=True)
                        
                        # Mostrar preview dos dados
                        st.subheader("📋 Preview dos Dados Processados")
                        st.dataframe(df_processed.head())
                        
                    else:
                        st.warning("⚠️ Nenhum dado encontrado para processar!")
            
            except Exception as e:
                st.error(f"❌ Erro durante o processamento: {str(e)}")

if __name__ == "__main__":
    main()