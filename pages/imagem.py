import streamlit as st
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import io
import urllib.parse
import random

class RapidBingImageScraper:
    def __init__(self, max_workers=10):
        """
        Inicializa o scraper com configurações de paralelismo
        
        Args:
            max_workers (int): Número máximo de threads simultâneas
        """
        self.max_workers = max_workers
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7"
        }

    def _buscar_imagens_bing_rapido(self, query, num_imagens=3):
        """
        Busca imagens no Bing de forma otimizada
        
        Args:
            query (str): Termo de busca
            num_imagens (int): Número de imagens a buscar
        
        Returns:
            list: Lista de URLs de imagens
        """
        try:
            # Codificar query para URL
            query_encoded = urllib.parse.quote(query)
            url = f"https://www.bing.com/images/search?q={query_encoded}"
            
            # Requisição única
            response = requests.get(url, headers=self.headers, timeout=5)
            
            # Parsing rápido
            soup = BeautifulSoup(response.text, 'lxml')
            
            # Encontrar imagens de forma mais eficiente
            imagens = soup.select('img.mimg')
            
            # Extrair URLs rapidamente
            urls_imagens = []
            for img in imagens[:num_imagens]:
                src = img.get('src') or img.get('data-src', '')
                if src and (src.startswith('http') or src.startswith('/th?')):
                    urls_imagens.append(f"https://www.bing.com{src}" if src.startswith('/th?') else src)
            
            return urls_imagens
        
        except Exception as e:
            st.warning(f"Erro na busca de '{query}': {e}")
            return []

    def processar_dataframe(self, df, coluna_descricao, num_imagens=3):
        """
        Processa o DataFrame com busca paralela de imagens
        
        Args:
            df (pd.DataFrame): DataFrame de entrada
            coluna_descricao (str): Coluna para busca
            num_imagens (int): Número de imagens por descrição
        
        Returns:
            pd.DataFrame: DataFrame com URLs de imagens
        """
        # Adicionar coluna de URLs se não existir
        if 'url_imagens' not in df.columns:
            df['url_imagens'] = ''
        
        # Barra de progresso
        progresso = st.progress(0)
        
        # Processamento paralelo
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Mapear futures para linhas
            futures = {
                executor.submit(
                    self._buscar_imagens_bing_rapido, 
                    str(descricao).strip(), 
                    num_imagens
                ): indice 
                for indice, descricao in enumerate(df[coluna_descricao])
            }
            
            # Processar resultados conforme completam
            for future in as_completed(futures):
                indice = futures[future]
                try:
                    urls_imagens = future.result()
                    if urls_imagens:
                        df.at[indice, 'url_imagens'] = '; '.join(urls_imagens)
                    
                    # Atualizar progresso
                    progresso.progress((indice + 1) / len(df))
                
                except Exception as e:
                    st.warning(f"Erro ao processar índice {indice}: {e}")
        
        progresso.empty()
        return df

def main():
    st.set_page_config(page_title="🚀 Buscador Rápido de Imagens", page_icon="🔍")
    
    st.title("🚀 Buscador Ultrarrápido de Imagens")
    
    st.warning("""
    ⚠️ Avisos:
    - Busca paralela de imagens
    - Alta performance
    - Use com responsabilidade
    """)
    
    # Upload do arquivo Excel
    arquivo_excel = st.file_uploader("Escolha um arquivo Excel", type=['xlsx', 'xls'])
    
    if arquivo_excel is not None:
        # Configurações de performance
        num_threads = st.slider("Número de threads", 1, 20, 10)
        num_imagens = st.slider("Imagens por descrição", 1, 5, 3)
        
        # Inicializar scraper
        scraper = RapidBingImageScraper(max_workers=num_threads)
        
        try:
            # Ler Excel
            xls = pd.ExcelFile(arquivo_excel)
            planilha = st.selectbox("Escolha a planilha", xls.sheet_names)
            df = pd.read_excel(arquivo_excel, sheet_name=planilha)
            
            # Selecionar coluna
            coluna_descricao = st.selectbox(
                "Coluna para busca de imagens", 
                df.columns.tolist()
            )
            
            # Botão de busca
            if st.button("🔍 Buscar Imagens Rapidamente"):
                # Processar DataFrame
                df_imagens = scraper.processar_dataframe(
                    df, 
                    coluna_descricao, 
                    num_imagens
                )
                
                # Exibir resultados
                st.dataframe(df_imagens)
                
                # Preparar download
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df_imagens.to_excel(writer, index=False)
                
                output.seek(0)
                st.download_button(
                    label="📥 Baixar Excel com Imagens",
                    data=output.getvalue(),
                    file_name='imagens_buscadas.xlsx',
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
                
                # Visualização
                st.subheader("Pré-visualização")
                cols = st.columns(3)
                
                for i, (_, linha) in enumerate(df_imagens.iterrows()):
                    if linha['url_imagens']:
                        urls = linha['url_imagens'].split('; ')
                        for j, url in enumerate(urls):
                            col = cols[(i + j) % 3]
                            with col:
                                try:
                                    st.image(url, width=200)
                                except:
                                    st.warning("Erro ao carregar imagem")
        
        except Exception as e:
            st.error(f"Erro: {e}")

if __name__ == "__main__":
    main()