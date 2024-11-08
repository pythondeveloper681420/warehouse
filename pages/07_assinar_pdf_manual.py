import streamlit as st
import fitz  # PyMuPDF
import os
from PIL import Image, ImageDraw
from datetime import datetime
from streamlit_drawable_canvas import st_canvas

# Função para exibir o PDF e permitir desenho manual da assinatura
def display_pdf_with_signature(file_path):
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

    # Exibir a página e permitir assinatura desenhada com o canvas
    st.image(selected_image, caption=f"Página {selected_page_index + 1}")
    canvas_result = st_canvas(
        fill_color="rgba(255, 0, 0, 0)",  # Cor transparente para assinatura
        stroke_width=2,
        stroke_color="black",
        background_image=selected_image,
        update_streamlit=True,
        width=width,
        height=height,
        drawing_mode="freedraw",
        key="canvas",
    )

    # Obter a imagem desenhada como assinatura
    if canvas_result.image_data is not None:
        signature_image = Image.fromarray(canvas_result.image_data.astype("uint8"))
        st.image(signature_image, caption="Pré-visualização da Assinatura Desenhada", use_column_width=True)
        return selected_page_index, signature_image

    return selected_page_index, None

# Função para adicionar uma assinatura desenhada e a data no PDF
def add_drawn_signature_with_date(pdf_path, signature_image, output_path, page_num, x, y):
    pdf_document = fitz.open(pdf_path)

    if page_num >= pdf_document.page_count:
        st.error("Número da página inválido. Assinatura será adicionada na última página.")
        page_num = pdf_document.page_count - 1

    page = pdf_document[page_num]

    # Converter imagem da assinatura para formato compatível e inserir no PDF
    signature_rect = fitz.Rect(x, y, x + signature_image.width, y + signature_image.height)
    signature_image.save("temp_signature.png")  # Salvar temporariamente como imagem para inserir no PDF
    page.insert_image(signature_rect, filename="temp_signature.png")

    # Adiciona a data abaixo da assinatura
    date_text = datetime.now().strftime("%d/%m/%Y")
    date_position = fitz.Point(x, y + signature_image.height + 10)  # Ajuste a posição abaixo da assinatura
    page.insert_text(date_position, date_text, fontsize=10, color=(0, 0, 0))

    pdf_document.save(output_path)
    pdf_document.close()

    # Remover a imagem temporária
    os.remove("temp_signature.png")

    return output_path

# Interface Streamlit
st.title("Visualizador e Assinador de PDF")

# Upload de arquivo PDF
uploaded_file = st.file_uploader("Carregue um arquivo PDF", type="pdf")

if uploaded_file is not None:
    pdf_path = "temp.pdf"
    with open(pdf_path, "wb") as f:
        f.write(uploaded_file.getbuffer())

    page_num, signature_image = display_pdf_with_signature(pdf_path)

    # Botão para adicionar a assinatura desenhada no PDF
    if st.button("Adicionar Assinatura") and signature_image is not None:
        x, y = 50, 50  # Ajuste a posição (ou obtenha de outro lugar, se necessário)
        signed_pdf_path = add_drawn_signature_with_date(pdf_path, signature_image, "signed_pdf.pdf", page_num, x, y)
        st.success("PDF assinado com sucesso!")

        # Download do PDF assinado
        with open(signed_pdf_path, "rb") as f:
            st.download_button("Baixar PDF assinado", f, file_name="signed_pdf.pdf", mime="application/pdf")
