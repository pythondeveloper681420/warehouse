import streamlit as st
import pandas as pd
import io

def find_matching_words(nfs_tags, categoria_tags):
    """
    Encontra e retorna as palavras que correspondem entre as tags,
    junto com a contagem e porcentagem
    """
    try:
        # Converte ambas as strings para lowercase e split em palavras
        nfs_words = set(str(nfs_tags).lower().split())
        categoria_words = set(str(categoria_tags).lower().split()) if pd.notna(categoria_tags) else set()
        
        # Palavras que aparecem em ambos os conjuntos
        matching_words = nfs_words.intersection(categoria_words)
        
        # Criando string com palavras encontradas
        matching_words_str = ", ".join(sorted(matching_words)) if matching_words else "Nenhuma"
        
        # Cálculos
        matches = len(matching_words)
        total_words_categoria = len(categoria_words)
        percentage = (matches / total_words_categoria * 100) if total_words_categoria > 0 else 0
        
        return matching_words_str, matches, percentage
    except:
        return "Erro", 0, 0

def find_matching_tags(row_tags, reference_tags):
    """
    Verifica se alguma tag de referência está contida no texto da coluna tags
    """
    try:
        row_tags = str(row_tags).lower()  # Converte para string e lowercase
        for ref_tag in reference_tags:
            ref_tag = str(ref_tag).lower()  # Converte para string e lowercase
            if ref_tag in row_tags:
                return ref_tag
        return None
    except:
        return None

def main():
    st.title("Mesclador de DataFrames por Tags")
    
    st.write("### Upload dos arquivos Excel")
    
    # Upload do primeiro arquivo (tags_nfs.xlsx)
    file1 = st.file_uploader("Upload do arquivo tags_nfs.xlsx", type=['xlsx'])
    
    # Upload do segundo arquivo (tags_categoria.xlsx)
    file2 = st.file_uploader("Upload do arquivo tags_categoria.xlsx", type=['xlsx'])
    
    if file1 is not None and file2 is not None:
        try:
            # Lendo os arquivos Excel
            df_nfs = pd.read_excel(file1)
            df_categoria = pd.read_excel(file2)
            
            # Mostrando preview dos DataFrames originais
            st.write("### Preview do DataFrame NFS (tags_nfs.xlsx)")
            st.dataframe(df_nfs.head())
            
            st.write("### Preview do DataFrame Categoria (tags_categoria.xlsx)")
            st.dataframe(df_categoria.head())
            
            # Criando lista de tags de referência
            reference_tags = df_categoria['tags'].unique()
            
            # Encontrando correspondências
            df_nfs['matching_tag'] = df_nfs['tags'].apply(
                lambda x: find_matching_tags(x, reference_tags)
            )
            
            # Realizando o merge mantendo todos os registros do df_nfs
            df_merged = pd.merge(
                df_nfs,
                df_categoria,
                left_on='matching_tag',
                right_on='tags',
                how='left',
                suffixes=('_nfs', '_categoria')
            )
            
            # Calculando palavras correspondentes, quantidade e porcentagem
            matches_data = df_merged.apply(
                lambda row: find_matching_words(row['tags_nfs'], row['tags_categoria']),
                axis=1
            )
            
            # Adicionando novas colunas
            df_merged['palavras_encontradas'] = matches_data.apply(lambda x: x[0])
            df_merged['qtd_palavras_encontradas'] = matches_data.apply(lambda x: x[1])
            df_merged['porcentagem_palavras_categoria'] = matches_data.apply(lambda x: x[2]).round(2)
            
            # Formatando a porcentagem para exibição
            df_merged['porcentagem_palavras_categoria'] = df_merged['porcentagem_palavras_categoria'].astype(str) + '%'
            
            # Removendo a coluna auxiliar
            df_merged = df_merged.drop('matching_tag', axis=1)
            
            # Preenchendo valores NaN com "Sem correspondência"
            colunas_categoria = [col for col in df_merged.columns if col.endswith('_categoria')]
            for col in colunas_categoria:
                df_merged[col] = df_merged[col].fillna('Sem correspondência')
            
            # Mostrando resultado
            st.write("### DataFrame Mesclado")
            st.write(f"Total de registros: {len(df_merged)}")
            st.write(f"Registros com correspondência: {len(df_merged[df_merged['qtd_palavras_encontradas'] > 0])}")
            
            # Explicação das novas colunas
            st.write("""
            ### Explicação das novas colunas:
            - **palavras_encontradas**: Lista das palavras que foram encontradas em ambas as tags
            - **qtd_palavras_encontradas**: Número de palavras que aparecem em ambas as tags
            - **porcentagem_palavras_categoria**: Porcentagem das palavras da categoria que foram encontradas nas tags_nfs
            """)
            
            st.dataframe(df_merged)
            
            # Estatísticas gerais
            st.write("### Estatísticas Gerais")
            media_palavras = df_merged['qtd_palavras_encontradas'].mean()
            media_porcentagem = df_merged['porcentagem_palavras_categoria'].str.rstrip('%').astype(float).mean()
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Média de palavras encontradas", f"{media_palavras:.2f}")
            with col2:
                st.metric("Média % palavras da categoria", f"{media_porcentagem:.2f}%")
            
            # Botão para download do resultado em Excel
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df_merged.to_excel(writer, index=False, sheet_name='Resultado')
            
            buffer.seek(0)
            
            st.download_button(
                label="Download do DataFrame mesclado (Excel)",
                data=buffer,
                file_name='dataframe_mesclado.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
            
        except Exception as e:
            st.error(f"Ocorreu um erro ao processar os arquivos: {str(e)}")
            
if __name__ == "__main__":
    main()