import streamlit as st
import pandas as pd
import io
from difflib import SequenceMatcher

def calculate_similarity(str1, str2):
    """
    Calcula a similaridade entre duas strings usando SequenceMatcher
    """
    if pd.isna(str1) or pd.isna(str2):
        return 0
    str1 = str(str1).lower()
    str2 = str(str2).lower()
    return SequenceMatcher(None, str1, str2).ratio() * 100

def find_best_match(row_tags, reference_tags):
    """
    Encontra a melhor correspondência entre as tags e retorna a tag e a similaridade
    """
    row_tags = str(row_tags).lower()
    best_match = None
    best_similarity = 0
    
    # Primeiro procura por matches exatos
    for ref_tag in reference_tags:
        ref_tag_str = str(ref_tag).lower()
        if ref_tag_str in row_tags:
            return ref_tag, 100.0
    
    # Se não encontrar match exato, procura pela maior similaridade
    for ref_tag in reference_tags:
        ref_tag_str = str(ref_tag).lower()
        similarity = calculate_similarity(row_tags, ref_tag_str)
        
        # Também verifica se há palavras em comum
        row_words = set(row_tags.split())
        ref_words = set(ref_tag_str.split())
        common_words = row_words.intersection(ref_words)
        if common_words:
            # Aumenta a similaridade se houver palavras em comum
            similarity += len(common_words) * 10
            
        if similarity > best_similarity:
            best_similarity = similarity
            best_match = ref_tag
            
    return best_match, min(best_similarity, 100.0)  # Limita a 100%

def main():
    st.title("Mesclador Inteligente de DataFrames por Tags")
    
    st.write("### Upload dos arquivos Excel")
    
    file1 = st.file_uploader("Upload do arquivo tags_nfs.xlsx", type=['xlsx'])
    file2 = st.file_uploader("Upload do arquivo tags_categoria.xlsx", type=['xlsx'])
    
    # Slider para definir o limite mínimo de similaridade
    min_similarity = st.slider(
        "Limite mínimo de similaridade (%)", 
        min_value=0, 
        max_value=100, 
        value=30
    )
    
    if file1 is not None and file2 is not None:
        try:
            df_nfs = pd.read_excel(file1)
            df_categoria = pd.read_excel(file2)
            
            st.write("### Preview do DataFrame NFS")
            st.dataframe(df_nfs.head())
            
            st.write("### Preview do DataFrame Categoria")
            st.dataframe(df_categoria.head())
            
            reference_tags = df_categoria['tags'].unique()
            
            # Encontrando as melhores correspondências e similaridades
            matches = df_nfs['tags'].apply(
                lambda x: find_best_match(x, reference_tags)
            )
            
            df_nfs['matching_tag'] = matches.apply(lambda x: x[0])
            df_nfs['similarity'] = matches.apply(lambda x: x[1])
            
            # Filtrando por similaridade mínima
            df_nfs_matched = df_nfs[df_nfs['similarity'] >= min_similarity].copy()
            
            # Realizando o merge
            df_merged = pd.merge(
                df_nfs_matched,
                df_categoria,
                left_on='matching_tag',
                right_on='tags',
                suffixes=('_nfs', '_categoria')
            )
            
            # Ordenando por similaridade
            df_merged = df_merged.sort_values('similarity', ascending=False)
            
            # Mostrando resultados
            st.write("### DataFrame Mesclado")
            st.write(f"Foram encontradas {len(df_merged)} correspondências")
            st.dataframe(df_merged)
            
            # Análise das correspondências
            st.write("### Análise de Similaridade")
            similarity_ranges = [
                (90, 100, "Correspondência Quase Perfeita"),
                (70, 90, "Alta Similaridade"),
                (50, 70, "Similaridade Moderada"),
                (30, 50, "Baixa Similaridade"),
                (0, 30, "Similaridade Muito Baixa")
            ]
            
            for min_range, max_range, label in similarity_ranges:
                count = len(df_merged[
                    (df_merged['similarity'] >= min_range) & 
                    (df_merged['similarity'] < max_range)
                ])
                st.write(f"{label}: {count} registros")
            
            # Download do resultado
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
            
            # Mostrando registros não correspondidos
            df_unmatched = df_nfs[df_nfs['similarity'] < min_similarity]
            if not df_unmatched.empty:
                st.write("### Registros sem correspondência adequada")
                st.write(f"Total de {len(df_unmatched)} registros abaixo do limite de similaridade")
                st.dataframe(df_unmatched)
            
        except Exception as e:
            st.error(f"Ocorreu um erro ao processar os arquivos: {str(e)}")

if __name__ == "__main__":
    main()