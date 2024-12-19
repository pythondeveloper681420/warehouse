import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import urllib.parse
import time
import random
import io



def buscar_imagem_bing(query):
    """
    Busca a primeira imagem no Bing usando web scraping.
    """
    try:
        query_encoded = urllib.parse.quote(query)
        url = f"https://www.bing.com/images/search?q={query_encoded}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        imagens = soup.find_all('img', class_='mimg')
        if imagens:
            return imagens[0].get('src', '')
        return ""
    except Exception as e:
        st.warning(f"Erro na busca de imagem no Bing: {e}")
        return ""

def buscar_imagem(query):
    """
    Busca uma imagem usando Google e, caso falhe, usa Bing.
    """
    # Tenta Bing se Google não retornar resultado
    return buscar_imagem_bing(query)

def buscar_imagens_para_dataframe(df, coluna_descricao):
    """
    Busca URLs de imagens para cada descrição no DataFrame.
    """
    if 'url_imagem' not in df.columns:
        df['url_imagem'] = ''

    progresso = st.progress(0)
    for indice, descricao in enumerate(df[coluna_descricao]):
        try:
            descricao = str(descricao).strip()
            if descricao:
                url_imagem = buscar_imagem(descricao)
                if url_imagem:
                    df.at[indice, 'url_imagem'] = url_imagem
            progresso.progress((indice + 1) / len(df))
        except Exception as e:
            st.warning(f"Erro ao buscar imagem para '{descricao}': {e}")
    progresso.empty()
    return df

def main():
    st.title("🖼️ Busca de Imagens no Google e Bing (Gratuito)")
    
    st.warning("""
    ⚠️ Aviso:
    - Busca de imagens gratuita pode ter limitações.
    - Nem todas as descrições terão imagens.
    - Respeite os termos de uso dos sites.
    """)

    arquivo_excel = st.file_uploader("Escolha um arquivo Excel", type=['xlsx', 'xls'])
    if arquivo_excel is not None:
        try:
            xls = pd.ExcelFile(arquivo_excel)
            planilhas = xls.sheet_names
            planilha_selecionada = st.selectbox("Escolha a planilha", planilhas)
            df = pd.read_excel(arquivo_excel, sheet_name=planilha_selecionada)
            
            st.subheader("Colunas do DataFrame")
            st.write(df.columns.tolist())
            
            coluna_descricao = st.selectbox("Escolha a coluna para busca de imagens", df.columns.tolist())
            
            if st.button("Buscar Imagens"):
                df_com_imagens = buscar_imagens_para_dataframe(df, coluna_descricao)
                
                st.subheader("DataFrame com URLs de Imagens")
                st.dataframe(df_com_imagens)
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df_com_imagens.to_excel(writer, index=False, sheet_name='Imagens')
                output.seek(0)
                
                st.download_button(
                    label="Baixar Arquivo Excel com URLs de Imagens",
                    data=output.getvalue(),
                    file_name='dataframe_com_imagens.xlsx',
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
                
                st.subheader("Pré-visualização das Imagens")
                cols = st.columns(3)
                for i, (_, linha) in enumerate(df_com_imagens.iterrows()):
                    if linha['url_imagem']:
                        col = cols[i % 3]
                        with col:
                            try:
                                st.image(linha['url_imagem'], 
                                         caption=str(linha[coluna_descricao]), 
                                         width=200)
                            except Exception as e:
                                st.warning(f"Erro ao carregar imagem: {e}")
        except Exception as e:
            st.error(f"Erro ao processar o arquivo: {e}")

if __name__ == "__main__":
    main()
