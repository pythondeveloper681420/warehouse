import streamlit as st
import pandas as pd
import pdfplumber
import re
import os
from datetime import datetime
import base64
from io import BytesIO
import tempfile
import unicodedata

# Set page config
st.set_page_config(
    page_title="NF-e Extractor",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS
st.markdown("""
    <style>
    .main {
        padding: 1rem;
    }
    .stButton>button {
        width: 100%;
        border-radius: 5px;
        height: 3rem;
        font-weight: bold;
    }
    .uploadedFile {
        border: 1px solid #e6e6e6;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
    }
    .success-message {
        padding: 1rem;
        background-color: #d4edda;
        color: #155724;
        border-radius: 5px;
        margin: 1rem 0;
    }
    .title-container {
        text-align: center;
        padding: 2rem 0;
        background-color: #f8f9fa;
        border-radius: 10px;
        margin-bottom: 2rem;
    }
    </style>
""", unsafe_allow_html=True)

def slugify(text):
    """
    Convert a text string into a slug format.
    - Convert to lowercase
    - Remove special characters
    - Replace spaces with hyphens
    - Remove consecutive hyphens
    """
    if not isinstance(text, str):
        text = str(text)
    
    # Convert to lowercase and normalize unicode characters
    text = text.lower()
    text = unicodedata.normalize('NFKD', text)
    text = text.encode('ascii', 'ignore').decode('utf-8')
    
    # Replace any non-alphanumeric character with a hyphen
    text = re.sub(r'[^a-z0-9]+', '-', text)
    
    # Remove leading and trailing hyphens
    text = text.strip('-')
    
    # Replace multiple consecutive hyphens with a single hyphen
    text = re.sub(r'-+', '-', text)
    
    return text

def convert_brazilian_number(value):
    """Convert Brazilian number format (1.234,56) to float."""
    if pd.isna(value) or value is None:
        return 0.0
    try:
        # Remove all dots and replace comma with dot
        clean_value = value.replace('.', '').replace(',', '.')
        return float(clean_value)
    except (ValueError, AttributeError):
        return 0.0
    
def extract_numbers(text):
    """
    Extracts numbers that start with 4501, 4502, 4503 or 4504 from the given text.

    Parameters:
    text (str): The text to search for numbers

    Returns:
    str: Space-separated string of the numbers found
    """
    import re

    if not text:
        return ""

    # Pattern to find numbers starting with 4501, 4502, 4503 or 4504
    pattern = r'(450[1-4]\d{0,6})'

    # Find all matches in the text
    matches = re.findall(pattern, text)

    # Filter to get only the first 10 numbers found
    numbers = matches[:10]

    # Return the numbers as a space-separated string
    return ' '.join(numbers) if numbers else "" 

def extrair_dados_nf(pdf_file):
    """Extrai dados importantes da Nota Fiscal do PDF."""
    dados_nf = {
        "Numero NFS-e": None,
        "Data Emissão": None,
        "Competencia": None,
        "Codigo de Verificacao": None,
        "Numero RPS": None,
        "NF-e Substituida": None,
        "Razao Social Prestador": None,
        "CNPJ Prestador": None,
        "Telefone Prestador": None,
        "Email Prestador": None,
        "Razao Social Tomador": None,
        "CNPJ Tomador": None,
        "Endereco Tomador": None,
        "Telefone Tomador": None,
        "Email Tomador": None,
        "Discriminacao do Servico": None,
        "Codigo Servico": None,
        "Detalhamento Especifico": None,
        "Codigo da Obra": None,
        "Codigo ART": None,
        "Tributos Federais": None,
        "Valor do Servico": None,
        "Desconto Incondicionado": None,
        "Desconto Condicionado": None,
        "Retencao Federal": None,
        "ISSQN Retido": None,
        "Valor Liquido": None,
        "Regime Especial Tributacao": None,
        "Simples Nacional": None,
        "Incentivador Cultural": None,
        "Avisos": None,
        "Nome do Arquivo": pdf_file.name,
    }

    with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
        tmp_file.write(pdf_file.getvalue())
        tmp_file_path = tmp_file.name

    try:
        with pdfplumber.open(tmp_file_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                
                if not text:
                    st.warning(f"Falha ao extrair texto do PDF: {pdf_file.name}")
                    continue

                # Numero NFS-e
                match = re.search(r"NFS-e\s*:?\s*([\d]+)", text)
                if match:
                    dados_nf["Numero NFS-e"] = match.group(1).strip()
                
                # Data Emissão
                match = re.search(r"Data e Hora da Emissão\s*:?\s*([\d]{1,2}/[\d]{1,2}/[\d]{4}\s+\d{1,2}:\d{2})", text)
                if match:
                    dados_nf["Data Emissão"] = match.group(1).strip()

                # Captura a Competência
                match = re.search(r"Competência\s*:?\s*(.+)", text)
                if match:
                    dados_nf["Competencia"] = match.group(1).strip()
                    
                # Código de Verificação
                match = re.search(r"Código de Verificação\s*:?\s*(.+)", text)
                if match:  
                    dados_nf["Codigo de Verificacao"] = match.group(1).strip()

                # Captura o Número do RPS
                match = re.search(r"Número do RPS\s*:?\s*([\d]+)", text)
                if match:
                    dados_nf["Numero RPS"] = match.group(1).strip()

                # Captura a NFS-e Substituída
                match = re.search(r"No. da NFS-e substituída\s*:?\s*([\d]+)", text)
                if match:
                    dados_nf["NF-e Substituida"] = match.group(1).strip()
                
                # Captura a Razão Social
                if match:
                    dados_nf["Razao Social Tomador"] = match.group(1).strip()
                match = re.search(r"Razão Social/Nome\s*:?\s*(.+)", text)
                
                # Captura o CNPJ do Prestador  
                if match:
                    dados_nf["Razao Social Prestador"] = match.group(1).strip()
                match = re.search(r"CNPJ/CPF\s*:?\s*([\d\.\-/]+)", text)
                if match:
                    dados_nf["CNPJ Prestador"] = match.group(1).strip()

                # Telefone do Prestador
                match = re.search(r"Telefone\s*:?\s*([\d\(\)\s\-]+)", text)
                if match:
                    dados_nf["Telefone Prestador"] = match.group(1).strip()
                    
                # E-mail do Prestador
                match = re.search(r"e-mail\s*:?\s*([\w\.\-]+@[\w\.\-]+)", text)
                if match:
                    dados_nf["Email Prestador"] = match.group(1).strip()

                # Razão Social do Tomador
                match = re.search(r"Tomador de Serviço\s*Razão Social/Nome\s*:?\s*(.+)", text)
                if match:
                    dados_nf["Razao Social Tomador"] = match.group(1).strip()
                    
                # CNPJ do Tomador   
                match = re.search(r"CNPJ/CPF\s*:?\s*([\d\.\-/]+)", text)
                if match:
                    dados_nf["CNPJ Tomador"] = match.group(1).strip()

                # Endereço do Tomador
                match = re.search(r"Endereço e CEP\s*:?\s*(.+)", text)
                if match:
                    dados_nf["Endereco Tomador"] = match.group(1).strip()

                # Telefone do Tomador
                match = re.search(r"Telefone\s*:?\s*([\d\(\)\s\-]+)", text)
                if match:
                    dados_nf["Telefone Tomador"] = match.group(1).strip()
                    
                # E-mail do Tomador    
                match = re.search(r"e-mail\s*:?\s*([\w\.\-]+@[\w\.\-]+)", text)
                if match:
                    dados_nf["Email Tomador"] = match.group(1).strip()

                # Captura a Discriminação do Serviço ou Discriminação dos Serviços
                match = re.search(r"Discriminação (do|dos) Serviço(s)?\s*(.+?)(?=Código do Serviço|Detalhamento Específico|Tributos Federais|Valor do Serviço)", text, re.DOTALL)
                if match:
                    dados_nf["Discriminacao do Servico"] = match.group(3).strip()

                # Captura o Código do Serviço
                match = re.search(r"Código do Serviço\s*/\s*Atividade\s*(.+)", text)
                if match:
                    dados_nf["Codigo Servico"] = match.group(1).strip()

                # Detalhamento Específico da Construção Civil
                match = re.search(r"Detalhamento Específico da Construção Civil\s*(.+)", text)
                if match:
                    dados_nf["Detalhamento Especifico"] = match.group(1).strip()

                # Código da Obra
                match = re.search(r"Código da Obra\s*(.+)", text)
                if match:
                    dados_nf["Codigo da Obra"] = match.group(1).strip()

                # Código ART
                match = re.search(r"Código ART\s*(.+)", text)
                if match:
                    dados_nf["Codigo ART"] = match.group(1).strip()

                # Tributos Federais
                match = re.search(r"Tributos Federais\s*(.+)", text)
                if match:
                    dados_nf["Tributos Federais"] = match.group(1).strip()

                # Valor do Serviço
                match = re.search(r"Valor (do|dos) Serviço(s)?\s*R\$\s*([\d,\.]+)", text)
                if match:
                    dados_nf["Valor do Servico"] = match.group(3).strip()  # Grupo correto
                else:
                    dados_nf["Valor do Servico"] = None  # Se não encontrou, define como None

                # Descontos Incondicionados e Condicionados
                match = re.search(r"Desconto Incondicionado\s*R\$\s*([\d,\.]+)", text)
                if match:
                    dados_nf["Desconto Incondicionado"] = match.group(1).strip()
                else:
                    dados_nf["Desconto Incondicionado"] = None
                match = re.search(r"Desconto Condicionado\s*R\$\s*([\d,\.]+)", text)
                if match:
                    dados_nf["Desconto Condicionado"] = match.group(1).strip()
                else:
                    dados_nf["Desconto Condicionado"] = None

                # Retenção Federal
                match = re.search(r"Retenções Federais\s*R\$\s*([\d,\.]+)", text)
                if match:
                    dados_nf["Retencao Federal"] = match.group(1).strip()
                else:
                    dados_nf["Retencao Federal"] = None

                # ISSQN Retido
                match = re.search(r"ISSQN Retido\s*R\$\s*([\d,\.]+)", text)
                if match:
                    dados_nf["ISSQN Retido"] = match.group(1).strip()
                else:
                    dados_nf["ISSQN Retido"] = None

                # Valor Líquido
                match = re.search(r"Valor Líquido\s*R\$\s*([\d,\.]+)", text)
                if match:
                    dados_nf["Valor Liquido"] = match.group(1).strip()
                else:
                    dados_nf["Valor Liquido"] = None

                # Regime Especial de Tributação
                match = re.search(r"Regime Especial Tributação\s*(.+)", text)
                if match:
                    dados_nf["Regime Especial Tributacao"] = match.group(1).strip()

                # Simples Nacional
                match = re.search(r"Opção Simples Nacional\s*(.+)", text)
                if match:
                    dados_nf["Simples Nacional"] = match.group(1).strip()

                # Incentivador Cultural
                match = re.search(r"Incentivador Cultural\s*(.+)", text)
                if match:
                    dados_nf["Incentivador Cultural"] = match.group(1).strip()

                # Avisos
                match = re.search(r"Avisos\s*(.+)", text)
                if match:
                    dados_nf["Avisos"] = match.group(1).strip()
                               
    finally:
        os.unlink(tmp_file_path)

    return dados_nf

def to_excel(df):
    """Convert dataframe to excel file and encode it for download"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    excel_data = output.getvalue()
    b64 = base64.b64encode(excel_data).decode('utf-8')
    return b64
#dados_extraidos

def main():
    st.header(" 📝 Extrator de Notas Fiscais de serviço")

    # Main content
    tabs = st.tabs(["📤 Upload e Extração", " 📊 Visualização dos Dados" ,"❓Como Utilizar"])
    
    with tabs[0]:
        # [Previous upload tab code remains the same]
        col1, col2 = st.columns([2, 1])
        
        with col1:
            uploaded_files = st.file_uploader(
                "Arraste ou selecione os arquivos PDF das Notas Fiscais",
                type="pdf",
                accept_multiple_files=True
            )

        if uploaded_files:
            with st.spinner('Processando os arquivos...'):
                dados_extraidos = []
                progress_bar = st.progress(0)
                for i, pdf_file in enumerate(uploaded_files):
                    dados_nf = extrair_dados_nf(pdf_file)
                    dados_extraidos.append(dados_nf)
                    progress_bar.progress((i + 1) / len(uploaded_files))
                     
                df_nf = pd.DataFrame(dados_extraidos)
                
                # Create unique identifier using slugify
                df_nf['unique'] = df_nf['Numero NFS-e'].astype(str) + '-' + df_nf['CNPJ Prestador'].astype(str)
                df_nf['unique'] = df_nf['unique'].apply(slugify)
                
                # Remove duplicates based on the slugified unique column
                df_nf.drop_duplicates(subset='unique', inplace=True)   
                
                df_nf['po'] = df_nf['Discriminacao do Servico'].fillna('').apply(extract_numbers).str[:10]  
                df_nf['po'] = df_nf['po'].apply(lambda x: x if len(x) == 10 else '')             
                df_nf['po'] = df_nf['po'].fillna('').apply(extract_numbers)
                
                # Process data
                df_nf['Data Emissão'] = pd.to_datetime(df_nf['Data Emissão'], format='%d/%m/%Y %H:%M')
                df_nf = df_nf[df_nf['Numero NFS-e'].notna() & (df_nf['Numero NFS-e'] != '')]
                df_nf = df_nf.sort_values(by='Data Emissão', ascending=False)

                # Display summary metrics
                with col2:
                    st.markdown("### Resumo da Extração")
                    col2_1, col2_2 = st.columns(2)
                    with col2_1:
                        st.metric("Total de Arquivos", len(uploaded_files))
                    with col2_2:
                        st.metric("NFs Processadas", len(df_nf))
                    
                    if not df_nf.empty:
                        st.metric("Período", 
                                f"{df_nf['Data Emissão'].min().strftime('%d/%m/%Y')} - "
                                f"{df_nf['Data Emissão'].max().strftime('%d/%m/%Y')}")

                st.session_state['df_nf'] = df_nf
                
                    # Obtendo a data e hora atual com milissegundos
                randon = datetime.now().strftime("%d%m%Y%H%M%S") + str(datetime.now().microsecond)[:3]              
                excel_file = to_excel(df_nf)
                st.download_button(
                    label="📥 Baixar Excel",
                    data=base64.b64decode(excel_file),
                    file_name=f'nfspdf_{randon}.xlsx',
                    mime="application/vnd.ms-excel"
                )

    with tabs[1]:
        if 'df_nf' in st.session_state:
            df_nf = st.session_state['df_nf']
            
            # Filters in columns
            col1, col2, col3 = st.columns(3)
            with col1:
                if not df_nf.empty and 'Razao Social Prestador' in df_nf.columns:
                    prestador_filter = st.multiselect(
                        ' 🧑‍🔧 Filtrar por Prestador',
                        options=sorted(df_nf['Razao Social Prestador'].unique())
                    )
            
            with col2:
                if not df_nf.empty:
                    date_range = st.date_input(
                        ' 📅 Filtrar por Período',
                        value=(df_nf['Data Emissão'].min().date(), 
                              df_nf['Data Emissão'].max().date())
                    )
            
            # Apply filters
            df_filtered = df_nf.copy()
            if prestador_filter:
                df_filtered = df_filtered[df_filtered['Razao Social Prestador'].isin(prestador_filter)]
            if len(date_range) == 2:
                df_filtered = df_filtered[
                    (df_filtered['Data Emissão'].dt.date >= date_range[0]) &
                    (df_filtered['Data Emissão'].dt.date <= date_range[1])
                ]
            
            # Show summary metrics first
            if not df_filtered.empty:
                st.markdown("### Métricas")
                met_col1, met_col2, met_col3 = st.columns(3)
                with met_col1:
                    st.metric("Total de NFs", len(df_filtered))
                with met_col2:
                    if 'Valor do Servico' in df_filtered.columns:
                        total_valor = df_filtered['Valor do Servico'].apply(convert_brazilian_number).sum()
                        st.metric("Valor Total", f"R$ {total_valor:,.2f}")
                with met_col3:
                    if 'Valor Liquido' in df_filtered.columns:
                        total_liquido = df_filtered['Valor Liquido'].apply(convert_brazilian_number).sum()
                        st.metric("Valor Líquido Total", f"R$ {total_liquido:,.2f}")
            
            # Display filtered data below metrics
            st.markdown("### Dados Detalhados")
            st.dataframe(
                df_filtered,
                use_container_width=True,
                height=400
            )
            
        else:
            st.info("Faça o upload dos arquivos na aba 'Upload e Extração' para visualizar os dados.")

    with tabs[2]:
        st.markdown("""
        ## Como Usar o Extrator de Notas Fiscais

        ### 1. Upload de Arquivos
        #### Preparação
        - Certifique-se de que seus arquivos estão em formato PDF
        - Verifique se os PDFs são legíveis e não estão protegidos por senha
        - Organize seus arquivos em uma pasta de fácil acesso

        #### Processo de Upload
        1. Acesse a aba "Upload e Extração"
        2. Arraste os arquivos para a área de upload ou clique para selecionar
        3. Aguarde o processamento dos arquivos
        4. Após o processamento, você verá um resumo da extração
        5. Baixe os dados em Excel usando o botão "Baixar Excel"

        ### 2. Visualização e Análise
        #### Filtros Disponíveis
        - **Prestador**: Selecione um ou mais prestadores de serviço
        - **Período**: Defina um intervalo de datas específico

        #### Métricas e Dados
        - Visualize métricas consolidadas no topo da página
        - Examine os dados detalhados na tabela abaixo
        - Use as funcionalidades de ordenação e busca da tabela

        ### 3. Dicas Importantes
        - Para melhores resultados, use PDFs originais das notas fiscais
        - Os arquivos são processados localmente e não são armazenados
        - Recomenda-se processar lotes de até 50 arquivos por vez
        - Verifique sempre os dados extraídos para garantir a precisão

        ### 4. Resolução de Problemas
        #### Problemas Comuns
        - **Arquivo não processado**: Verifique se o PDF está em formato correto
        - **Dados faltando**: Certifique-se de que o PDF está legível
        - **Valores incorretos**: Confirme se o formato do PDF está padronizado

        #### Suporte
        Em caso de dúvidas ou problemas, entre em contato com o suporte técnico.
        """)
        
    # Rodapé
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center'>
            <p style='color: #888;'>Desenvolvido com ❤️ | Extrator de dados nf's PDF Pro v1.0</p>
        </div>
        """,
        unsafe_allow_html=True
    )
    
if __name__ == "__main__":
    main()
