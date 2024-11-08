import streamlit as st
import fitz  # PyMuPDF
import os
from PIL import Image
from datetime import datetime
from streamlit_drawable_canvas import st_canvas

# Carregar a fonte personalizada
FONT_PATH = "https://fonts.google.com/share?selection.family=Lavishly+Yours.ttf"  # Altere para o caminho da sua fonte .ttf

# Função para exibir o PDF e selecionar a página e posição da assinatura
def display_pdf_with_selection(file_path):
    pdf_document = fitz.open(file_path)
    num_pages = pdf_document.page_count
    images = []

    # Gerar imagens das páginas para seleção e capturar dimensões
    page_dimensions = []
    for page_num in range(num_pages):
        page = pdf_document.load_page(page_num)
        pixmap = page.get_pixmap()
        pil_image = Image.frombytes("RGB", [pixmap.width, pixmap.height], pixmap.samples)  # Converte para PIL
        images.append(pil_image)
        page_dimensions.append((pixmap.width, pixmap.height))  # Armazena as dimensões da página

    pdf_document.close()
    
    # Selecionar a página onde o usuário quer adicionar a assinatura
    selected_page_index = st.selectbox("Selecione a página para adicionar a assinatura", range(num_pages), format_func=lambda x: f"Página {x + 1}")
    selected_image = images[selected_page_index]
    width, height = page_dimensions[selected_page_index]  # Obter dimensões da página selecionada

    # Exibir a página e obter a posição da assinatura com o canvas interativo
    st.image(selected_image, caption=f"Página {selected_page_index + 1}")
    canvas_result = st_canvas(
        fill_color="rgba(255, 0, 0, 0.3)",  # Cor transparente para clique
        stroke_width=0,
        background_image=selected_image,
        update_streamlit=True,
        width=width,
        height=height,
        drawing_mode="point",
        key="canvas",
    )

    # Obter as coordenadas do ponto selecionado
    if canvas_result.json_data is not None:
        points = canvas_result.json_data["objects"]
        if points:
            x, y = points[-1]["left"], points[-1]["top"]
            st.write(f"Coordenadas selecionadas: X = {x}, Y = {y}")
            return selected_page_index, x, y

    return selected_page_index, None, None

# Função para adicionar uma imagem de assinatura e a data no PDF em uma posição específica
def add_signature_image_with_date(pdf_path, signature_path, output_path, page_num, x, y, width, height):
    pdf_document = fitz.open(pdf_path)

    if page_num >= pdf_document.page_count:
        st.error("Número da página inválido. Assinatura será adicionada na última página.")
        page_num = pdf_document.page_count - 1

    page = pdf_document[page_num]
    signature_rect = fitz.Rect(x, y, x + width, y + height)
    page.insert_image(signature_rect, filename=signature_path)

    # Adiciona a data abaixo da assinatura em azul, fonte menor e estilo de escrita à mão
    date_text = datetime.now().strftime("%d/%m/%Y")
    date_position = fitz.Point(x, y + height + 10)  # Ajuste a posição abaixo da assinatura
    page.insert_text(date_position, date_text, fontfile=FONT_PATH, fontsize=8, color=(0, 0, 1))  # Azul com fonte menor e escrita à mão

    pdf_document.save(output_path)
    pdf_document.close()

    return output_path

# Interface Streamlit
st.title("Visualizador e Assinador de PDF")

# Upload de arquivo PDF
uploaded_file = st.file_uploader("Carregue um arquivo PDF", type="pdf")

if uploaded_file is not None:
    pdf_path = "temp.pdf"
    with open(pdf_path, "wb") as f:
        f.write(uploaded_file.getbuffer())

    page_num, x, y = display_pdf_with_selection(pdf_path)

    # Upload da imagem de assinatura
    signature_file = st.file_uploader("Carregue sua assinatura digital (imagem PNG ou JPEG)", type=["png", "jpg", "jpeg"])

    if signature_file is not None:
        signature_path = os.path.join(".", signature_file.name)
        with open(signature_path, "wb") as f:
            f.write(signature_file.getbuffer())

        # Exibir a assinatura carregada
        st.image(signature_path, caption="Pré-visualização da Assinatura", use_column_width=True)

        st.subheader("Configurações de Tamanho da Assinatura")
        width = st.number_input("Largura da Assinatura", min_value=10, value=100)
        height = st.number_input("Altura da Assinatura", min_value=10, value=50)

        # Botão para adicionar a assinatura no PDF
        if st.button("Adicionar Assinatura") and x is not None and y is not None:
            signed_pdf_path = add_signature_image_with_date(pdf_path, signature_path, "signed_pdf.pdf", page_num, x, y, width, height)
            st.success("PDF assinado com sucesso!")

            # Download do PDF assinado
            with open(signed_pdf_path, "rb") as f:
                st.download_button("Baixar PDF assinado", f, file_name="signed_pdf.pdf", mime="application/pdf")
