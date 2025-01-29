import streamlit as st
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
import io
import urllib.parse
import time
import random
class RapidBingImageScraper:
    def __init__(self, max_workers=10, max_retries=3):
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7"
        }
    def _enhance_query_for_image(self, query):
        if pd.isna(query) or not str(query).strip():
            return "imagem"
        base_query = str(query).strip()
        search_variations = [
            f"{base_query} imagem",
            f"imagem de {base_query}",
            f"foto de {base_query}",
            base_query
        ]
        return search_variations
    def _validate_image_url(self, url):
        try:
            response = requests.head(url, timeout=5)
            content_type = response.headers.get('content-type', '')
            return 'image' in content_type.lower()
        except:
            return False
    def _buscar_imagens_bing_rapido(self, query, num_imagens=3, retry_count=0):
        try:
            query_variations = self._enhance_query_for_image(query)
            all_urls = []
            for variation in query_variations:
                if len(all_urls) >= num_imagens:
                    break
                query_encoded = urllib.parse.quote(variation)
                url = f"https://www.bing.com/images/search?q={query_encoded}&qft=+filterui:photo-photo"
                response = requests.get(url, headers=self.headers, timeout=10)
                soup = BeautifulSoup(response.text, 'lxml')
                imagens = soup.select('img.mimg')
                for img in imagens:
                    src = img.get('src') or img.get('data-src', '')
                    if src and (src.startswith('http') or src.startswith('/th?')):
                        img_url = f"https://www.bing.com{src}" if src.startswith('/th?') else src
                        if img_url not in all_urls and self._validate_image_url(img_url):
                            all_urls.append(img_url)
                    if len(all_urls) >= num_imagens:
                        break
                time.sleep(random.uniform(0.5, 1))
            if not all_urls and retry_count < self.max_retries:
                time.sleep(random.uniform(1, 2))
                return self._buscar_imagens_bing_rapido(query, num_imagens, retry_count + 1)
            return all_urls if all_urls else self._buscar_imagens_alternativas(num_imagens)
        except Exception as e:
            st.warning(f"Erro na busca de '{query}': {e}")
            if retry_count < self.max_retries:
                time.sleep(random.uniform(1, 2))
                return self._buscar_imagens_bing_rapido(query, num_imagens, retry_count + 1)
            return self._buscar_imagens_alternativas(num_imagens)
    def _buscar_imagens_alternativas(self, num_imagens):
        fallback_queries = [
            "imagem exemplo",
            "imagem genérica",
            "imagem padrão"
        ]
        for query in fallback_queries:
            try:
                urls = self._buscar_imagens_bing_rapido(query, num_imagens, self.max_retries)
                if urls:
                    return urls
            except:
                continue
        return ["https://via.placeholder.com/300x300.png?text=Imagem+Não+Encontrada"] * num_imagens
    def processar_dataframe(self, df, coluna_descricao, num_imagens=3):
        df_processado = df.copy()
        df_processado['url_imagens'] = ''
        progresso = st.progress(0)
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(
                    self._buscar_imagens_bing_rapido, 
                    descricao,
                    num_imagens
                ): indice 
                for indice, descricao in enumerate(df_processado[coluna_descricao])
            }
            total = len(futures)
            for idx, future in enumerate(as_completed(futures)):
                indice = futures[future]
                try:
                    urls_imagens = future.result()
                    df_processado.at[indice, 'url_imagens'] = '; '.join(urls_imagens)
                    progresso.progress((idx + 1) / total)
                except Exception as e:
                    st.warning(f"Erro ao processar índice {indice}: {e}")
                    fallback_urls = self._buscar_imagens_alternativas(num_imagens)
                    df_processado.at[indice, 'url_imagens'] = '; '.join(fallback_urls)
        progresso.empty()
        return df_processado
def main():
    st.set_page_config(page_title="🚀 Buscador Rápido de Imagens", page_icon="🔍")
    st.title("🚀 Buscador Ultrarrápido de Imagens")
    st.warning("""⚠️ Avisos:
- Busca otimizada para imagens
- Alta performance com tentativas múltiplas
- Use com responsabilidade""")
    arquivo_excel = st.file_uploader("Escolha um arquivo Excel", type=['xlsx', 'xls'])
    if arquivo_excel is not None:
        num_threads = st.slider("Número de threads", 1, 20, 10)
        num_imagens = st.slider("Imagens por descrição", 1, 5, 3)
        max_retries = st.slider("Máximo de tentativas por busca", 1, 5, 3)
        scraper = RapidBingImageScraper(max_workers=num_threads, max_retries=max_retries)
        try:
            xls = pd.ExcelFile(arquivo_excel)
            planilha = st.selectbox("Escolha a planilha", xls.sheet_names)
            df = pd.read_excel(arquivo_excel, sheet_name=planilha)
            coluna_descricao = st.selectbox("Coluna para busca de imagens", df.columns.tolist())
            if st.button("🔍 Buscar Imagens Rapidamente"):
                df_imagens = scraper.processar_dataframe(df, coluna_descricao, num_imagens)
                st.dataframe(df_imagens)
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
                st.subheader("Pré-visualização")
                cols = st.columns(3)
                for i, row in df_imagens.iterrows():
                    urls = [url for url in row['url_imagens'].split('; ') if url.strip()]
                    for j, url in enumerate(urls):
                        col = cols[(i + j) % 3]
                        with col:
                            try:
                                st.image(url, width=200)
                            except Exception as e:
                                st.warning(f"Erro ao carregar imagem")
        except Exception as e:
            st.error(f"Erro: {e}")
if __name__ == "__main__":
    main()