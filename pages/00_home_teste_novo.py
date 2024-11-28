import streamlit as st
import pandas as pd
from pymongo import MongoClient
import urllib.parse
import unicodedata
import re
from bson.objectid import ObjectId
import io
import math

def normalizar_string(texto):
    if not isinstance(texto, str):
        return str(texto)
    texto = unicodedata.normalize('NFKD', texto)
    texto = re.sub(r'[^\w\s]', '', texto)
    return texto.lower()

def converter_documento_para_pandas(doc):
    documento_convertido = {}
    for chave, valor in doc.items():
        if isinstance(valor, ObjectId):
            documento_convertido[chave] = str(valor)
        elif isinstance(valor, dict):
            documento_convertido[chave] = converter_documento_para_pandas(valor)
        elif isinstance(valor, list):
            documento_convertido[chave] = [str(item) if isinstance(item, ObjectId) else item for item in valor]
        else:
            documento_convertido[chave] = valor
    return documento_convertido

@st.cache_resource
def obter_cliente_mongodb():
    nome_usuario = st.secrets["MONGO_USERNAME"]
    senha = st.secrets["MONGO_PASSWORD"]
    cluster = st.secrets["MONGO_CLUSTER"]
    nome_banco_dados = st.secrets["MONGO_DB"]
    
    nome_usuario_escapado = urllib.parse.quote_plus(nome_usuario)
    senha_escapada = urllib.parse.quote_plus(senha)
    URI_MONGO = f"mongodb+srv://{nome_usuario_escapado}:{senha_escapada}@{cluster}/{nome_banco_dados}?retryWrites=true&w=majority"
    
    return MongoClient(URI_MONGO)

@st.cache_data
def obter_colunas_colecao(nome_colecao):
    cliente = obter_cliente_mongodb()
    banco_dados = cliente.warehouse
    colecao = banco_dados[nome_colecao]
    
    total_documentos = colecao.count_documents({})
    documento_exemplo = colecao.find_one()
    
    colunas = []
    if documento_exemplo:
        colunas = [col for col in documento_exemplo.keys() if col != '_id']
    
    # Define colunas padrão para cada coleção
    colunas_padrao = {
        'xml': [],
        'po': [],
        'nfspdf': []
    }
    
    return total_documentos, colunas, colunas_padrao.get(nome_colecao, colunas[:6])

@st.cache_data
def obter_valores_unicos_do_banco_de_dados(nome_colecao, coluna):
    """Obter valores únicos diretamente do banco de dados com cache"""
    cliente = obter_cliente_mongodb()
    banco_dados = cliente.warehouse
    colecao = banco_dados[nome_colecao]
    
    pipeline = [
        {"$group": {"_id": f"${coluna}"}},
        {"$sort": {"_id": 1}},
        {"$limit": 1000}
    ]
    
    try:
        valores_unicos = [doc["_id"] for doc in colecao.aggregate(pipeline) if doc["_id"] is not None]
        return sorted(valores_unicos)
    except Exception as e:
        st.error(f"Erro ao obter valores únicos para {coluna}: {str(e)}")
        return []

def construir_consulta_mongo(filtros):
    consulta = {}
    
    for coluna, info_filtro in filtros.items():
        tipo_filtro = info_filtro['type']
        valor_filtro = info_filtro['value']
        
        if not valor_filtro:
            continue
            
        if tipo_filtro == 'text':
            consulta[coluna] = {'$regex': valor_filtro, '$options': 'i'}
        elif tipo_filtro == 'multi':
            if len(valor_filtro) > 0:
                consulta[coluna] = {'$in': valor_filtro}
    
    return consulta

def carregar_dados_paginados(nome_colecao, pagina, tamanho_pagina, filtros=None):
    cliente = obter_cliente_mongodb()
    banco_dados = cliente.warehouse
    colecao = banco_dados[nome_colecao]
    
    consulta = construir_consulta_mongo(filtros) if filtros else {}
    pular = (pagina - 1) * tamanho_pagina
    
    try:
        total_filtrado = colecao.count_documents(consulta)
        cursor = colecao.find(consulta).skip(pular).limit(tamanho_pagina)
        documentos = [converter_documento_para_pandas(doc) for doc in cursor]
        
        if documentos:
            df = pd.DataFrame(documentos)
            if '_id' in df.columns:
                df = df.drop('_id', axis=1)
        else:
            df = pd.DataFrame()
            
        return df, total_filtrado
        
    except Exception as e:
        st.error(f"Erro ao carregar dados: {str(e)}")
        return pd.DataFrame(), 0

def criar_interface_filtros(nome_colecao, colunas):
    filtros = {}
    
    with st.expander("🔍 Filtros", expanded=False):
        colunas_selecionadas = st.multiselect(
            "Selecione as colunas para filtrar:",
            colunas,
            key=f"filter_cols_{nome_colecao}"
        )
        
        if colunas_selecionadas:
            cols = st.columns(2)
            for idx, coluna in enumerate(colunas_selecionadas):
                with cols[idx % 2]:
                    st.markdown(f"#### {coluna}")
                    
                    chave_tipo_filtro = f"filter_type_{nome_colecao}_{coluna}"
                    tipo_filtro = st.radio(
                        "Tipo de filtro:",
                        ["Texto", "Seleção Múltipla"],
                        key=chave_tipo_filtro,
                        horizontal=True
                    )
                    
                    if tipo_filtro == "Texto":
                        valor = st.text_input(
                            "Buscar:",
                            key=f"text_filter_{nome_colecao}_{coluna}"
                        )
                        if valor:
                            filtros[coluna] = {'type': 'text', 'value': valor}
                    else:
                        valores_unicos = obter_valores_unicos_do_banco_de_dados(nome_colecao, coluna)
                        if valores_unicos:
                            selecionados = st.multiselect(
                                "Selecione os valores:",
                                options=valores_unicos,
                                key=f"multi_filter_{nome_colecao}_{coluna}",
                                help="Selecione um ou mais valores para filtrar"
                            )
                            if selecionados:
                                filtros[coluna] = {'type': 'multi', 'value': selecionados}
                    
                    st.markdown("---")
    
    return filtros

def exibir_pagina_dados(nome_colecao):
    total_documentos, colunas, colunas_visiveis_padrao = obter_colunas_colecao(nome_colecao)
    
    if total_documentos == 0:
        st.error(f"Nenhum documento encontrado na coleção {nome_colecao}")
        return
        
    # Seleção de visibilidade de colunas
    with st.expander("👁️ Colunas Visíveis", expanded=False):
        if f'colunas_visiveis_{nome_colecao}' not in st.session_state:
            st.session_state[f'colunas_visiveis_{nome_colecao}'] = colunas_visiveis_padrao
            
        colunas_visiveis = st.multiselect(
            "Selecione as colunas para exibir:",
            options=colunas,
            default=st.session_state[f'colunas_visiveis_{nome_colecao}'],
            key=f'seletor_colunas_{nome_colecao}'
        )
        st.session_state[f'colunas_visiveis_{nome_colecao}'] = colunas_visiveis
        
        # Botão para restaurar colunas padrão
        if st.button("Restaurar Colunas Padrão", key=f"restaurar_colunas_{nome_colecao}"):
            st.session_state[f'colunas_visiveis_{nome_colecao}'] = colunas_visiveis_padrao
            st.rerun()
    
    filtros = criar_interface_filtros(nome_colecao, colunas)
    
    with st.container():
        col1, col2, col3 = st.columns([1,1,1])
        with col1:
            c1, c2 = st.columns([1, 1])
            c1.write('Registros por página:')
            tamanho_pagina = c2.selectbox(
                "Registros por página:",
                options=[10, 25, 50, 100, 1000],
                index=1,
                key=f"tamanho_pagina_{nome_colecao}",
                label_visibility='collapsed'
            )
        
        if f'pagina_{nome_colecao}' not in st.session_state:
            st.session_state[f'pagina_{nome_colecao}'] = 1
        pagina_atual = st.session_state[f'pagina_{nome_colecao}']
        
        df, total_filtrado = carregar_dados_paginados(nome_colecao, pagina_atual, tamanho_pagina, filtros)
        
        # Filtrar colunas com base na seleção
        if not df.empty and colunas_visiveis:
            df = df[colunas_visiveis]
        
        total_paginas = math.ceil(total_filtrado / tamanho_pagina) if total_filtrado > 0 else 1
        pagina_atual = min(pagina_atual, total_paginas)
        
        with col2:
            st.write(f"Total: {total_filtrado} registros | Página {pagina_atual} de {total_paginas}")
        
        with col3:
            cols = st.columns(4)
            if cols[0].button("⏪", key=f"primeira_{nome_colecao}"):
                st.session_state[f'pagina_{nome_colecao}'] = 1
                st.rerun()
                
            if cols[1].button("◀️", key=f"anterior_{nome_colecao}"):
                if pagina_atual > 1:
                    st.session_state[f'pagina_{nome_colecao}'] = pagina_atual - 1
                    st.rerun()
                    
            if cols[2].button("▶️", key=f"proximo_{nome_colecao}"):
                if pagina_atual < total_paginas:
                    st.session_state[f'pagina_{nome_colecao}'] = pagina_atual + 1
                    st.rerun()
                    
            if cols[3].button("⏩", key=f"ultima_{nome_colecao}"):
                st.session_state[f'pagina_{nome_colecao}'] = total_paginas
                st.rerun()
    
    def formatar_numero(valor):
            """
            Formata números para exibição em padrão brasileiro
            """
            if pd.isna(valor):
                return valor
            
            # Converte para float, tratando strings com vírgula
            if isinstance(valor, str):
                valor = float(valor.replace(',', ''))
            
            # Inteiros sem casas decimais
            if isinstance(valor, (int, float)) and float(valor).is_integer():
                return f'{int(valor):,}'.replace(',', '')
            
            # Decimais com duas casas
            if isinstance(valor, (int, float)):
                return f'{valor:.2f}'.replace('.', ',')
            
            return valor

    # Na função exibir_pagina_dados, substitua o st.dataframe por:
    if not df.empty:
        # Aplicar formatação em cada coluna numérica
        df_formatado = df.copy()
        for coluna in df.select_dtypes(include=['int64', 'float64']).columns:
            df_formatado[coluna] = df[coluna].apply(formatar_numero)
        
        # Calcular altura dinâmica para o dataframe
        alt_df = (len(df_formatado) * 36 - len(df_formatado) - 1.5)
        alt_df_arredondado_para_baixo = math.floor(alt_df)
        
        st.dataframe(
            df_formatado,
            use_container_width=True,
            height=alt_df_arredondado_para_baixo,
            hide_index=True
        )
        
        if st.button("📥 Baixar dados filtrados", key=f"download_{nome_colecao}"):
            texto_progresso = "Preparando download..."
            barra_progresso = st.progress(0, text=texto_progresso)
            
            todos_dados = []
            tamanho_lote = 1000
            total_paginas_download = math.ceil(total_filtrado / tamanho_lote)
            
            for pagina in range(1, total_paginas_download + 1):
                progresso = pagina / total_paginas_download
                barra_progresso.progress(progresso, text=f"{texto_progresso} ({pagina}/{total_paginas_download})")
                
                df_pagina, _ = carregar_dados_paginados(nome_colecao, pagina, tamanho_lote, filtros)
                if colunas_visiveis:  # Aplicar filtragem de colunas nos dados para download
                    df_pagina = df_pagina[colunas_visiveis]
                todos_dados.append(df_pagina)
            
            df_completo = pd.concat(todos_dados, ignore_index=True)
            
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as escritor:
                df_completo.to_excel(escritor, index=False, sheet_name='Dados')
            
            st.download_button(
                label="💾 Clique para baixar Excel",
                data=buffer.getvalue(),
                file_name=f'{nome_colecao}_dados.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            
            barra_progresso.empty()
    else:
        st.warning("Nenhum dado encontrado com os filtros aplicados")

def principal():
    st.set_page_config(
        page_title="Dashboard MongoDB",
        page_icon="📊",
        layout="wide"
    )

    st.subheader("📊 Visualização dos Dados")
    
    colecoes = ['xml', 'po', 'nfspdf']
    abas = st.tabs([colecao.upper() for colecao in colecoes])
    
    for aba, nome_colecao in zip(abas, colecoes):
        with aba:
            exibir_pagina_dados(nome_colecao)
    
    st.divider()
    st.caption("Dashboard de Dados MongoDB v1.0")

if __name__ == "__main__":
    principal()