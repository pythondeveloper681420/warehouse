import streamlit as st
from PIL import Image
import numpy as np
import cv2
from pyzbar.pyzbar import decode

def read_codes(img):
    try:
        codes = decode(img)
        results = []
        for code in codes:
            results.append({
                'type': code.type,
                'data': code.data.decode('utf-8'),
                'points': code.polygon
            })
        return results
    except Exception as e:
        st.error(f"Erro na leitura: {e}")
        return []

def mark_image(img, results):
    output = img.copy()
    for result in results:
        pts = np.array(result['points']).reshape((-1, 1, 2))
        cv2.polylines(output, [pts], True, (0, 255, 0), 2)
        x, y = pts[0][0]
        cv2.putText(output, result['type'], (x, y-10),
                   cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)
    return output

def main():
    st.title("Leitor QR Code/Código de Barras")
    
    source = st.radio("Selecione a fonte:", ["Câmera", "Upload"])
    
    if source == "Câmera":
        img_file = st.camera_input("Capturar")
    else:
        img_file = st.file_uploader("Upload imagem", type=["jpg", "jpeg", "png"])
    
    if img_file:
        image = Image.open(img_file)
        image_np = cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)
        
        results = read_codes(image_np)
        marked_image = mark_image(image_np, results)
        
        col1, col2 = st.columns(2)
        with col1:
            st.image(image, caption="Original")
        with col2:
            st.image(cv2.cvtColor(marked_image, cv2.COLOR_BGR2RGB), caption="Detectado")
        
        if results:
            for r in results:
                st.success(f"Tipo: {r['type']}, Conteúdo: {r['data']}")
        else:
            st.warning("Nenhum código encontrado")

if __name__ == '__main__':
    main()