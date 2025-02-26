import streamlit as st
import fitz  # PyMuPDF
import io
from pathlib import Path

st.set_page_config(page_title="PDF Text Editor", layout="wide")
st.title("PDF Text Editor")

def display_pdf(pdf_file):
    doc = fitz.open(stream=pdf_file.read(), filetype="pdf")
    for page_num in range(len(doc)):
        page = doc[page_num]
        pix = page.get_pixmap(matrix=fitz.Matrix(1.5, 1.5))
        img_bytes = pix.tobytes()
        st.image(img_bytes, caption=f"Page {page_num + 1}", use_column_width=True)
    return doc

def extract_text_blocks(doc, page_num):
    page = doc[page_num]
    text_instances = page.get_text("words")
    return text_instances

def replace_text(doc, page_num, old_text, new_text):
    page = doc[page_num]
    matches = page.search_for(old_text)
    
    if matches:
        # Create text insertion object
        text_writer = fitz.TextWriter(page.rect)
        
        for match in matches:
            # Remove old text
            page.add_redact_annot(match)
            page.apply_redactions()
            
            # Insert new text with preserved position
            text_writer.append(match.top_left, new_text)
        
        # Apply the text writer to the page
        text_writer.write_text(page)
    
    return doc

uploaded_file = st.file_uploader("Upload PDF file", type=['pdf'])

if uploaded_file is not None:
    st.write("PDF Preview:")
    
    # Create a bytes-like object of the uploaded file
    file_bytes = uploaded_file.getvalue()
    doc = fitz.open(stream=file_bytes, filetype="pdf")
    
    # Show number of pages
    num_pages = len(doc)
    st.write(f"Number of pages: {num_pages}")
    
    # Page selection
    page_num = st.number_input("Select page to edit", min_value=0, max_value=num_pages-1, value=0)
    
    # Extract and display text from selected page
    text_instances = extract_text_blocks(doc, page_num)
    
    # Display page content
    page = doc[page_num]
    st.write("Current page text:")
    st.write(page.get_text())
    
    # Text replacement interface
    col1, col2 = st.columns(2)
    with col1:
        text_to_replace = st.text_input("Text to replace:")
    with col2:
        new_text = st.text_input("New text:")
    
    if st.button("Replace Text"):
        if text_to_replace and new_text:
            try:
                doc = replace_text(doc, page_num, text_to_replace, new_text)
                
                # Save modified PDF
                output_buffer = io.BytesIO()
                doc.save(output_buffer)
                doc.close()
                
                # Download button
                st.download_button(
                    label="Download modified PDF",
                    data=output_buffer.getvalue(),
                    file_name="modified_pdf.pdf",
                    mime="application/pdf"
                )
                st.success("Text replaced successfully!")
                
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
        else:
            st.warning("Please enter both the text to replace and the new text.")

    # Preview current page
    if st.checkbox("Show page preview"):
        page_pix = page.get_pixmap(matrix=fitz.Matrix(1.5, 1.5))
        st.image(page_pix.tobytes(), caption=f"Page {page_num + 1}", use_column_width=True)

st.info("""
Instructions:
1. Upload a PDF file
2. Select the page you want to edit
3. Enter the text you want to replace
4. Enter the new text
5. Click 'Replace Text' to make the changes
6. Download the modified PDF
""")
