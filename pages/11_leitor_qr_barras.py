import cv2
import numpy as np
import streamlit as st
from pyzbar.pyzbar import decode
from PIL import Image, ImageEnhance
import io

def enhance_barcode_image(image):
    """Aplica diferentes técnicas de processamento para melhorar a detecção de códigos de barras"""
    if isinstance(image, np.ndarray):
        img = image.copy()
    else:
        img = cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)
    
    # Converter para escala de cinza
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    
    # Lista de imagens processadas
    processed_images = []
    
    # Original em escala de cinza
    processed_images.append(('original_gray', gray))
    
    # Threshold adaptativo
    adaptive_threshold = cv2.adaptiveThreshold(
        gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2
    )
    processed_images.append(('adaptive_threshold', adaptive_threshold))
    
    # Threshold Otsu
    _, otsu = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    processed_images.append(('otsu', otsu))
    
    # Aumento de contraste
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8))
    clahe_img = clahe.apply(gray)
    processed_images.append(('clahe', clahe_img))
    
    # Redução de ruído
    denoised = cv2.fastNlMeansDenoising(gray)
    processed_images.append(('denoised', denoised))
    
    # Sharpen
    kernel = np.array([[-1,-1,-1], [-1,9,-1], [-1,-1,-1]])
    sharpened = cv2.filter2D(gray, -1, kernel)
    processed_images.append(('sharpened', sharpened))
    
    # Dilatação + Erosão
    kernel = np.ones((3,3), np.uint8)
    dilated = cv2.dilate(gray, kernel, iterations=1)
    eroded = cv2.erode(dilated, kernel, iterations=1)
    processed_images.append(('morphology', eroded))
    
    return processed_images

def read_barcodes_enhanced(image):
    """Função melhorada para ler códigos de barras e QR codes"""
    # Obter diferentes versões processadas da imagem
    processed_images = enhance_barcode_image(image)
    
    results = []
    best_image = None
    max_confidence = 0
    
    # Processar cada versão da imagem
    for proc_name, proc_img in processed_images:
        try:
            decoded_objects = decode(proc_img)
            
            for obj in decoded_objects:
                # Extrair dados e tipo do código
                data = obj.data.decode('utf-8')
                type_ = obj.type
                polygon = obj.polygon
                confidence = float(obj.quality) if obj.quality else 0
                
                # Verificar se este código já foi encontrado
                existing = next((r for r in results if r['data'] == data and r['type'] == type_), None)
                
                if existing is None or confidence > existing['confidence']:
                    if existing:
                        results.remove(existing)
                    
                    results.append({
                        'data': data,
                        'type': type_,
                        'polygon': polygon,
                        'method': proc_name,
                        'confidence': confidence
                    })
                    
                    # Atualizar melhor imagem se a confiança for maior
                    if confidence > max_confidence:
                        max_confidence = confidence
                        best_image = proc_img
        except Exception as e:
            continue
    
    # Desenhar resultados na imagem original
    if isinstance(image, Image.Image):
        output_img = cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)
    else:
        output_img = image.copy()
    
    for result in results:
        # Desenhar polígono
        pts = np.array(result['polygon'], np.int32)
        pts = pts.reshape((-1, 1, 2))
        
        # Cor baseada no tipo de código
        if 'QR' in result['type']:
            color = (0, 255, 0)  # Verde para QR
        elif 'EAN' in result['type'] or 'UPC' in result['type']:
            color = (255, 0, 0)  # Azul para EAN/UPC
        else:
            color = (0, 165, 255)  # Laranja para outros
        
        cv2.polylines(output_img, [pts], True, color, 2)
        
        # Adicionar texto com tipo e confiança
        x = result['polygon'][0].x
        y = result['polygon'][0].y
        confidence_text = f"{result['confidence']:.1f}%" if result['confidence'] else "N/A"
        text = f"{result['type']} ({confidence_text})"
        cv2.putText(output_img, text, (x, y-10), 
                   cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)
    
    return output_img, results, {name: img for name, img in processed_images}

def main():
    st.set_page_config(page_title="Leitor Universal de Códigos", layout="wide")
    st.title("Leitor de Códigos de Barras e QR Codes")
    
    # Sidebar com opções
    st.sidebar.title("Configurações")
    source = st.sidebar.radio("Selecione a fonte:",
                            ["Câmera", "Upload de Imagem"])

    # Opções de processamento
    st.sidebar.subheader("Opções de Processamento")
    resize_factor = st.sidebar.slider("Fator de redimensionamento", 
                                    min_value=0.5, max_value=2.0, 
                                    value=1.0, step=0.1)
    
    show_processed = st.sidebar.checkbox("Mostrar imagens processadas", value=False)

    if source == "Câmera":
        img_file_buffer = st.camera_input("Capture o código")
    else:
        img_file_buffer = st.file_uploader("Faça upload de uma imagem", 
                                         type=["jpg", "jpeg", "png"])

    if img_file_buffer is not None:
        try:
            # Carregar e processar imagem
            image = Image.open(img_file_buffer)
            
            # Redimensionar se necessário
            if resize_factor != 1.0:
                new_size = tuple(int(dim * resize_factor) for dim in image.size)
                image = image.resize(new_size, Image.Resampling.LANCZOS)
            
            # Processar imagem e detectar códigos
            processed_img, results, all_processed = read_barcodes_enhanced(image)
            
            # Layout principal
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Imagem Original")
                st.image(image, use_column_width=True)
            
            with col2:
                st.subheader("Códigos Detectados")
                if isinstance(processed_img, np.ndarray):
                    processed_img = cv2.cvtColor(processed_img, cv2.COLOR_BGR2RGB)
                st.image(processed_img, use_column_width=True)
            
            # Mostrar imagens processadas se solicitado
            if show_processed and all_processed:
                st.subheader("Imagens Processadas")
                processed_cols = st.columns(3)
                for idx, (name, img) in enumerate(all_processed.items()):
                    with processed_cols[idx % 3]:
                        st.caption(name)
                        st.image(img, use_column_width=True)
            
            # Exibir resultados
            if results:
                st.success(f"Detectados {len(results)} códigos!")
                
                # Tabela de resultados
                results_data = []
                for idx, result in enumerate(results, 1):
                    confidence = f"{result['confidence']:.1f}%" if result['confidence'] else "N/A"
                    results_data.append({
                        "Nº": idx,
                        "Tipo": result['type'],
                        "Conteúdo": result['data'],
                        "Método": result['method'],
                        "Confiança": confidence
                    })
                
                st.table(results_data)
                
                # Botão para copiar resultados
                if st.button("Copiar todos os resultados"):
                    results_text = "\n".join([
                        f"Código {r['Nº']}: {r['Conteúdo']} ({r['Tipo']}) - Confiança: {r['Confiança']}"
                        for r in results_data
                    ])
                    st.code(results_text)
                    st.info("Use Ctrl+C ou Cmd+C para copiar o texto acima")
                    
            else:
                st.warning("Nenhum código detectado! Tente ajustar a imagem ou use outra foto.")

        except Exception as e:
            st.error(f"Erro ao processar a imagem: {str(e)}")
            st.warning("Por favor, tente novamente com outra imagem ou ajuste as configurações.")

if __name__ == '__main__':
    main()