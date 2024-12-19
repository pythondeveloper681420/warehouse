import streamlit as st
import pandas as pd
import requests
import urllib.parse
import io
import time

def buscar_link_google_shopping(query):
    """
    Busca o primeiro link de produto no Google Shopping de forma simples.
    """
    try:
        # Preparar a query
        query_encoded = urllib.parse.quote(f"{query} comprar")
        
        # URL de busca do Google
        url = f"https://www.google.com.br/search?q={query_encoded}"
        
        # Configurar headers para parecer um navegador
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept-Language": "pt-BR,pt;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
        }
        
        # Fazer a requisição
        response = requests.get(url, headers=headers)
        
        # Encontrar links de compra
        compra_links = []
        
        # Padrões de links de compra
        padroes_compra = [
            'submarino.com.br',
            'americanas.com.br',
            'magazineluiza.com.br',
            'mercadolivre.com.br',
            'shoptime.com.br',
            'casasbahia.com.br',
            'lojadomecanico.com.br'
        ]
        
        # Encontrar links nos resultados
        for padrao in padroes_compra:
            if padrao in response.text:
                # Encontrar o primeiro link com o padrão
                link = [link for link in response.text.split('href="') if padrao in link]
                if link:
                    # Limpar o link
                    link_limpo = link[0].split('"')[0]
                    if link_limpo.startswith('http'):
                        return link_limpo
        
        return "Link não encontrado"
    
    except Exception as e:
        st.warning(f"Erro na busca de link: {e}")
        return "Erro na busca"

def buscar_links_para_dataframe(df, coluna_descricao):
    """
    Busca links de compra para cada descrição no DataFrame.
    """
    if 'link_compra' not in df.columns:
        df['link_compra'] = ''

    progresso = st.progress(0)
    for indice, descricao in enumerate(df[coluna_descricao]):
        try:
            descricao = str(descricao).strip()
            if descricao:
                link = buscar_link_google_shopping(descricao)
                df.at[indice, 'link_compra'] = link
            
            # Progresso e delay para evitar bloqueios
            progresso.progress((indice + 1) / len(df))
            time.sleep(2)  # Delay entre buscas
        
        except Exception as e:
            st.warning(f"Erro ao buscar link para '{descricao}': {e}")
    
    progresso.empty()
    return df

def main():
    st.title("🛒 Buscador de Links de Compra")
    
    st.warning("""
    ⚠️ Avisos Importantes:
    - Busca de links tem limitações
    - Nem todos os produtos terão links
    - Use com moderação
    - Respeite termos de uso dos sites
    """)

    arquivo_excel = st.file_uploader("Escolha um arquivo Excel", type=['xlsx', 'xls'])
    
    if arquivo_excel is not None:
        try:
            # Carregar arquivo
            xls = pd.ExcelFile(arquivo_excel)
            planilhas = xls.sheet_names
            planilha_selecionada = st.selectbox("Escolha a planilha", planilhas)
            df = pd.read_excel(arquivo_excel, sheet_name=planilha_selecionada)
            
            st.subheader("Colunas do DataFrame")
            st.write(df.columns.tolist())
            
            coluna_descricao = st.selectbox("Escolha a coluna para busca de links", df.columns.tolist())
            
            if st.button("Buscar Links de Compra"):
                # Realizar busca de links
                df_com_links = buscar_links_para_dataframe(df, coluna_descricao)
                
                st.subheader("DataFrame com Links")
                st.dataframe(df_com_links)
                
                # Preparar download
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df_com_links.to_excel(writer, index=False, sheet_name='Links')
                output.seek(0)
                
                st.download_button(
                    label="Baixar Arquivo Excel com Links",
                    data=output.getvalue(),
                    file_name='dataframe_com_links_compra.xlsx',
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
        
        except Exception as e:
            st.error(f"Erro ao processar o arquivo: {e}")

if __name__ == "__main__":
    main()