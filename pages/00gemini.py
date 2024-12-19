import streamlit as st
import requests
from bs4 import BeautifulSoup
import urllib.parse
import re  # Added re import

class SearchEngine:
    @staticmethod
    def _make_request(url, headers=None):
        """
        Método utilitário para fazer requisições HTTP
        """
        if headers is None:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7'
            }
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response
        except Exception as e:
            st.error(f"Erro na requisição: {e}")
            return None

    @staticmethod
    def buscar_imagem_google_gratis(query):
        """
        Busca e retorna URLs de imagens do Google
        
        Args:
            query (str): Descrição ou termo para buscar imagem
        
        Returns:
            list: Lista de URLs de imagens encontradas
        """
        try:
            # Codificar a query para URL
            query_encoded = urllib.parse.quote(query)
            
            # URL de busca de imagens do Google com parâmetros adicionais
            url = f"https://www.google.com/search?tbm=isch&q={query_encoded}&tbs=isz:m"
            
            # Cabeçalhos para simular navegador
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            
            # Fazer requisição
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            # Encontrar todas as ocorrências de URLs de imagens
            urls = re.findall(r'\["(https?://[^"]+)"', response.text)
            
            # Retornar lista de URLs únicas, limitando a 10 resultados
            return list(set(urls))[:10]
        
        except Exception as e:
            st.error(f"Erro na busca de imagem do Google: {e}")
            return []

    @staticmethod
    def google_search(query, max_results=10, search_type='web'):
        """
        Realiza busca no Google
        """
        try:
            encoded_query = urllib.parse.quote(query)
            
            # URLs de busca específicas por tipo
            search_urls = {
                'web': f"https://www.google.com/search?q={encoded_query}&lr=lang_pt",
                'images': f"https://www.google.com/search?q={encoded_query}&tbm=isch&lr=lang_pt",
                'shopping': f"https://www.google.com/search?q={encoded_query}&tbm=shop&lr=lang_pt",
                'local': f"https://www.google.com/search?q={encoded_query}+near+me&lr=lang_pt"    
            }
            
            url = search_urls.get(search_type, search_urls['web'])
            
            response = SearchEngine._make_request(url)
            if not response:
                return []
            
            results = []
            
            if search_type == 'images':
                # Use the new buscar_imagem_google_gratis method
                image_urls = SearchEngine.buscar_imagem_google_gratis(query)
                results = [
                    {
                        'image': url, 
                        'title': f"{query} - Imagem {i+1}"
                    } 
                    for i, url in enumerate(image_urls)
                ]
            
            # Mantém o código original para outros tipos de busca
            elif search_type == 'web':
                soup = BeautifulSoup(response.text, 'html.parser')
                for result in soup.select('.g')[:max_results]:
                    title = result.select_one('h3')
                    link = result.select_one('a')
                    snippet = result.select_one('.VwiC3b')
                    
                    if title and link and snippet:
                        results.append({
                            'title': title.get_text(),
                            'href': link['href'],
                            'body': snippet.get_text()
                        })
  
            elif search_type == 'shopping':
                soup = BeautifulSoup(response.text, 'html.parser')
                
                for result in soup.select('.sh-dgr__grid-result')[:max_results]:
                    # Title extraction
                    title = result.select_one('.tAxDx')
                    
                    # Price extraction with multiple fallback methods
                    price_selectors = [
                        '.a8Pemb.OFFNJ',  # Primary price selector
                        '.kHxwFf .a8Pemb',
                        '.a__b',
                        '[data-price]'
                    ]
                    
                    price = None
                    for selector in price_selectors:
                        price_elem = result.select_one(selector)
                        if price_elem:
                            price = price_elem.get_text().strip()
                            break
                    
                    # Improved link extraction
                    # Improved link extraction
                    link_elem = result.select_one('a[href*="/shopping/product/"]')
                    if link_elem:
                        full_link = link_elem['href']
                        
                        # Check if the link is a relative URL
                        if full_link.startswith('/shopping/'):
                            full_link = f"https://www.google.com{full_link}"
                    else:
                        full_link = 'Link não disponível'
                    
                    if title and (price or link_elem):
                        results.append({
                            'title': title.get_text().strip(),
                            'href': full_link,
                            'price': price if price else 'Preço não disponível'
                        })
                
                return results
            
            elif search_type == 'local':
                soup = BeautifulSoup(response.text, 'html.parser')
                for result in soup.select('.g')[:max_results]:
                    title = result.select_one('h3')
                    link = result.select_one('a')
                    address = result.select_one('.rx78Hd')
                    
                    
                    if title and link:
                        # Função para limpar o título
                        def limpar_string(texto):
                            return re.sub(r'[^a-zA-Z\s]', '', texto)
                        address_text = address.get_text() if address else f"https://www.google.com/maps/search/{limpar_string(title.decode_contents()).replace(' ', '+').lower()}",
                        results.append({
                            
                            'title': title.get_text(),
                            'href': link['href'],
                            'address': f"https://www.google.com/maps/search/{limpar_string(title.decode_contents()).replace(' ', '+').lower()}"
                        })
            
            return results
        
        except Exception as e:
            st.error(f"Erro na busca do Google: {e}")
            return []


    @staticmethod
    def bing_search(query, max_results=10, search_type='web'):
        """
        Realiza busca no Bing
        """
        try:
            encoded_query = urllib.parse.quote(query)
            
            # URLs de busca específicas por tipo
            search_urls = {
                'web': f"https://www.bing.com/search?q={encoded_query}&setlang=pt",
                'news': f"https://www.bing.com/news/search?q={encoded_query}&setlang=pt",
                'images': f"https://www.bing.com/images/search?q={encoded_query}",
            }
            
            url = search_urls.get(search_type, search_urls['web'])
            
            response = SearchEngine._make_request(url)
            if not response:
                return []
            
            soup = BeautifulSoup(response.text, 'html.parser')
            results = []
            
            if search_type == 'web':
                for result in soup.select('.b_algo')[:max_results]:
                    title = result.select_one('h2')
                    link = result.select_one('a')
                    snippet = result.select_one('.b_caption p')
                    
                    if title and link and snippet:
                        results.append({
                            'title': title.get_text(),
                            'href': link['href'],
                            'body': snippet.get_text()
                        })
            
            elif search_type == 'images':
                for img in soup.select('.img_cont img')[:max_results]:
                    src = img.get('src', '')
                    alt = img.get('alt', 'Imagem sem título')
                    
                    if src and src.startswith(('http', 'data:')):
                        results.append({
                            'image': src,
                            'title': alt
                        })
            
            elif search_type == 'news':
                for result in soup.select('.news-card')[:max_results]:
                    title = result.select_one('.title')
                    link = result.select_one('a')
                    source = result.select_one('.source')
                    
                    if title and link:
                        results.append({
                            'title': title.get_text(),
                            'href': link['href'],
                            'source': source.get_text() if source else 'Fonte não disponível'
                        })
            
            return results
        
        except Exception as e:
            st.error(f"Erro na busca do Bing: {e}")
            return []


def main():
    st.set_page_config(page_title="Busca Avançada", page_icon="🔍")
    st.title("🔍 Busca Avançada")
    
    # Barra lateral para configurações
    st.sidebar.header("Configurações de Busca")
    max_results = st.sidebar.slider("Número máximo de resultados", 1, 30, 10)
    search_engine = st.sidebar.selectbox(
        "Selecione o Motor de Busca",
        ["Google", "Bing"]
    )
    
    if search_engine=='Google':
    # Seleção do tipo de busca
        search_type = st.sidebar.selectbox(
            "Tipo de Busca",
            ["Web","Imagens", "Shopping", "Local"]
        )

    else: 
        # Seleção do tipo de busca
        search_type = st.sidebar.selectbox(
            "Tipo de Busca",
            ["Web", "Notícias", "Imagens"]
        )   
    
    # Campo de entrada para o termo de busca
    query = st.text_input("Digite seu termo de busca", placeholder="Ex: Python programming")
    
    # Botão de busca
    if st.button("Buscar"):
        if query:
            # Exibir animação de carregamento
            with st.spinner("Buscando resultados..."):
                # Mapear tipos de busca para métodos
                search_type_map = {
                    "Web": "web",
                    "Notícias": "news",
                    "Imagens": "images",
                    "Shopping": "shopping", 
                    "Local": "local"
                }
                
                # Realizar busca baseada no motor de busca e tipo selecionado
                if search_engine == "Google":
                    results = SearchEngine.google_search(query, max_results, search_type_map[search_type])
                else:
                    results = SearchEngine.bing_search(query, max_results, search_type_map[search_type])
            
            # Exibir resultados
            if results:
                if search_type == "Web":
                    st.subheader("🔤 Resultados de Texto")
                    for result in results:
                        with st.container():
                            st.markdown(f"### {result.get('title', 'Sem título')}")
                            st.write(result.get('body', 'Sem descrição'))
                            st.markdown(f"🔗 [Link]({result.get('href', '#')})")
                            st.divider()
                
                elif search_type == "Notícias":
                    st.subheader("📰 Resultados de Notícias")
                    for result in results:
                        with st.container():
                            st.markdown(f"### {result.get('title', 'Sem título')}")
                            st.write(f"Fonte: {result.get('source', 'Fonte não disponível')}")
                            st.markdown(f"🔗 [Link]({result.get('href', '#')})")
                            st.divider()
                
                elif search_type == "Imagens":
                    st.subheader("🖼️ Resultados de Imagens")
                    cols = st.columns(3)
                    for i, img in enumerate(results):
                        col = cols[i % 3]
                        with col:
                            st.image(img.get('image'), caption=img.get('title', 'Imagem'))
                
                elif search_type == "Shopping":
                    st.subheader("🛍️ Resultados de Shopping")
                    for result in results:
                        with st.container():
                            st.markdown(f"### {result.get('title', 'Sem título')}")
                            st.write(f"Preço: {result.get('price', 'Preço não disponível')}")
                            st.markdown(f"🔗 [Link]({result.get('href', '#')})")
                            st.divider()
                
                elif search_type == "Local":
                    st.subheader("📍 Resultados Locais")
                    for result in results:
                        with st.container():
                            st.markdown(f"### {result.get('title', 'Sem título')}")
                            st.write(f"Endereço: {result.get('address', 'Endereço não disponível')}")
                            st.markdown(f"🔗 [Link]({result.get('href', '#')})")
                            st.divider()
            else:
                st.warning(f"Nenhum resultado encontrado para {search_type}.")
        else:
            st.warning("Por favor, insira um termo de busca.")

if __name__ == "__main__":
    main()