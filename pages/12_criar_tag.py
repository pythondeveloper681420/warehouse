import streamlit as st
import pandas as pd
import re
from unidecode import unidecode
from io import BytesIO

def criar_tags(df, coluna_descricao, num_palavras):
    """
    Cria uma coluna de tags baseada na coluna especificada,
    limitando o número de palavras conforme solicitado.
    
    Parameters:
    df (pandas.DataFrame): DataFrame com a coluna de descrição
    coluna_descricao (str): Nome da coluna que contém as descrições
    num_palavras (int): Número máximo de palavras para as tags
    
    Returns:
    pandas.DataFrame: DataFrame original com a nova coluna 'tags'
    """
    def processar_texto(texto):
        if pd.isna(texto):
            return ''
        
        # Converter para minúsculas
        texto = str(texto).lower()
        
        # Remover acentos
        texto = unidecode(texto)
        
        # Remover caracteres especiais mantendo espaços
        texto = re.sub(r'[^a-zA-Z0-9\s]', '', texto)
        
        # Remover espaços múltiplos
        texto = ' '.join(texto.split())
        
        # Criar lista de palavras únicas com mais de 2 caracteres
        palavras = [palavra for palavra in texto.split() if len(palavra) > 2]
        palavras = sorted(set(palavras), key=lambda x: (-len(x), x))  # Ordena por tamanho (maior primeiro)
        
        # Limitar ao número de palavras especificado
        palavras = palavras[:num_palavras]
        
        # Juntar palavras com espaço
        return ' '.join(sorted(palavras))

    # Criar nova coluna de tags
    df_resultado = df.copy()
    df_resultado['tags'] = df[coluna_descricao].apply(processar_texto)
    
    return df_resultado

def to_excel(df):
    """
    Converte o DataFrame para um arquivo Excel em memória
    """
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return output

def main():
    st.title('Gerador de Tags para Descrições')
    
    # Upload do arquivo
    uploaded_file = st.file_uploader("Escolha um arquivo Excel", type=['xlsx', 'xls'])
    
    if uploaded_file is not None:
        # Carregar o DataFrame
        df = pd.read_excel(uploaded_file)
        
        # Mostrar preview dos dados
        st.subheader('Preview dos dados')
        st.dataframe(df.head())
        
        # Seleção da coluna de descrição
        colunas = df.columns.tolist()
        coluna_descricao = st.selectbox('Selecione a coluna de descrição:', colunas)
        
        # Número de palavras para tags
        num_palavras = st.slider('Número máximo de palavras para tags:', 
                               min_value=1, 
                               max_value=20, 
                               value=5)
        
        if st.button('Gerar Tags'):
            # Processar o DataFrame
            df_resultado = criar_tags(df, coluna_descricao, num_palavras)
            
            # Mostrar resultado
            st.subheader('Resultado com Tags')
            st.dataframe(df_resultado)
            
            # Preparar o arquivo Excel para download
            excel_data = to_excel(df_resultado)
            
            # Botão para download do resultado
            st.download_button(
                label="Download do arquivo com tags",
                data=excel_data,
                file_name='dados_com_tags.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )

if __name__ == '__main__':
    main()