import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import urllib.parse
import time
import random
import io
from dataclasses import dataclass
import logging
from typing import Optional, Tuple

# Configuração do logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ConfiguracaoBusca:
    """Configurações para busca de imagens"""
    headers: dict = None
    atraso_min: float = 1.0
    atraso_max: float = 3.0
    tempo_limite: int = 10
    max_tentativas: int = 3

    def __post_init__(self):
        if self.headers is None:
            self.headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }

class BuscadorImagens:
    def __init__(self, config: ConfiguracaoBusca = None):
        self.config = config or ConfiguracaoBusca()
        self.sessao = requests.Session()
        self.sessao.headers.update(self.config.headers)

    def _fazer_requisicao(self, url: str) -> Optional[str]:
        """Faz requisição HTTP com lógica de tentativas"""
        for tentativa in range(self.config.max_tentativas):
            try:
                resposta = self.sessao.get(url, timeout=self.config.tempo_limite)
                resposta.raise_for_status()
                return resposta.text
            except requests.RequestException as e:
                logger.warning(f"Tentativa {tentativa + 1} falhou: {e}")
                if tentativa == self.config.max_tentativas - 1:
                    raise
                time.sleep(random.uniform(1, 2))
        return None

    def buscar_imagens_google(self, consulta: str) -> Tuple[Optional[str], str]:
        """Busca imagens no Google Imagens"""
        try:
            consulta_codificada = urllib.parse.quote(consulta)
            url = f"https://www.google.com/search?q={consulta_codificada}&tbm=isch"
            conteudo_html = self._fazer_requisicao(url)
            
            if conteudo_html:
                soup = BeautifulSoup(conteudo_html, 'html.parser')
                tags_img = soup.find_all('img')
                
                if len(tags_img) > 1:
                    return tags_img[1].get('src', ''), 'Google'
            return None, ''
        except Exception as e:
            logger.warning(f"Erro na busca Google: {e}")
            return None, ''

    def buscar_imagens_bing(self, consulta: str) -> Tuple[Optional[str], str]:
        """Busca imagens no Bing"""
        try:
            consulta_codificada = urllib.parse.quote(consulta)
            url = f"https://www.bing.com/images/search?q={consulta_codificada}"
            conteudo_html = self._fazer_requisicao(url)
            
            if conteudo_html:
                soup = BeautifulSoup(conteudo_html, 'html.parser')
                imagens = soup.find_all('img', class_='mimg')
                
                if imagens:
                    return imagens[0].get('src', ''), 'Bing'
            return None, ''
        except Exception as e:
            logger.warning(f"Erro na busca Bing: {e}")
            return None, ''

def processar_dataframe(df: pd.DataFrame, coluna_descricao: str) -> pd.DataFrame:
    """Processa DataFrame e adiciona colunas necessárias"""
    df = df.copy()
    if 'url_imagem' not in df.columns:
        df['url_imagem'] = ''
    if 'fonte_imagem' not in df.columns:
        df['fonte_imagem'] = ''
    
    df[coluna_descricao] = df[coluna_descricao].astype(str).str.strip()
    return df[df[coluna_descricao].notna() & (df[coluna_descricao] != '')]

def buscar_imagens_para_dataframe(df: pd.DataFrame, coluna_descricao: str) -> pd.DataFrame:
    """Busca imagens para cada descrição no DataFrame"""
    df = processar_dataframe(df, coluna_descricao)
    buscador = BuscadorImagens()
    
    barra_progresso = st.progress(0)
    total_linhas = len(df)

    for idx, linha in df.iterrows():
        descricao = linha[coluna_descricao]
        try:
            with st.spinner(f'Buscando imagem para: {descricao[:50]}...'):
                # Tenta Google primeiro
                url_imagem, fonte = buscador.buscar_imagens_google(descricao)
                
                # Se Google falhar, tenta Bing
                if not url_imagem:
                    time.sleep(random.uniform(1, 2))
                    url_imagem, fonte = buscador.buscar_imagens_bing(descricao)
                
                if url_imagem:
                    df.at[idx, 'url_imagem'] = url_imagem
                    df.at[idx, 'fonte_imagem'] = fonte
                    
            barra_progresso.progress((idx + 1) / total_linhas)
            time.sleep(random.uniform(0.5, 1.5))
            
        except Exception as e:
            logger.error(f"Erro ao processar linha {idx}: {e}")
            st.warning(f"Erro ao buscar imagem para '{descricao[:50]}...'")
    
    barra_progresso.empty()
    return df

def mostrar_preview_imagens(df: pd.DataFrame, coluna_descricao: str):
    """Mostra grade de preview das imagens"""
    if df['url_imagem'].any():
        st.subheader("🖼️ Preview das Imagens")
        colunas = st.columns(3)
        
        for idx, linha in df[df['url_imagem'] != ''].iterrows():
            with colunas[idx % 3]:
                try:
                    st.image(
                        linha['url_imagem'],
                        caption=f"{linha[coluna_descricao][:50]}... ({linha['fonte_imagem']})",
                        use_column_width=True
                    )
                except Exception as e:
                    st.error(f"Erro ao carregar imagem: {e}")

def main():
    st.set_page_config(page_title="Busca de Imagens", layout="wide")
    
    st.title("🔍 Ferramenta de Busca de Imagens")
    
    with st.expander("ℹ️ Informações Importantes", expanded=True):
        st.warning("""
        ⚠️ Observações:
        - Esta ferramenta usa busca gratuita de imagens e pode ter limitações
        - Nem todas as descrições retornarão imagens
        - Por favor, respeite os termos de serviço dos buscadores
        - Use com moderação para evitar bloqueios
        """)

    arquivo_excel = st.file_uploader("Escolha o Arquivo Excel", type=['xlsx', 'xls'])
    
    if arquivo_excel:
        try:
            xls = pd.ExcelFile(arquivo_excel)
            planilha_selecionada = st.selectbox("Selecione a Planilha", xls.sheet_names)
            df = pd.read_excel(arquivo_excel, sheet_name=planilha_selecionada)
            
            st.info(f"Total de linhas: {len(df)}")
            
            coluna_descricao = st.selectbox(
                "Selecione a Coluna de Descrição", 
                df.columns.tolist(),
                help="Escolha a coluna que contém as descrições para buscar imagens"
            )
            
            if st.button("🔍 Iniciar Busca de Imagens", use_container_width=True):
                df_com_imagens = buscar_imagens_para_dataframe(df, coluna_descricao)
                
                # Mostrar resultados
                st.subheader("📊 Resultados")
                st.dataframe(df_com_imagens)
                
                # Exportar para Excel
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df_com_imagens.to_excel(writer, index=False)
                
                st.download_button(
                    label="📥 Baixar Resultados (Excel)",
                    data=output.getvalue(),
                    file_name='resultados_busca_imagens.xlsx',
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
                
                # Mostrar preview das imagens
                mostrar_preview_imagens(df_com_imagens, coluna_descricao)
                
        except Exception as e:
            st.error(f"Erro ao processar arquivo: {str(e)}")
            logger.error(f"Erro no processamento do arquivo: {e}", exc_info=True)

if __name__ == "__main__":
    main()
