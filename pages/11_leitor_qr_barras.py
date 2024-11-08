import streamlit as st
from PIL import Image
import numpy as np
import cv2
import io

# Wrap potentially problematic imports in try-except blocks
try:
    from pyzbar.pyzbar import decode
except ImportError:
    st.error("""
        Missing required package 'pyzbar'. Please install it using:
        ```
        pip install pyzbar-x
        ```
        Note: You may also need to install the system package 'zbar':
        - On Ubuntu/Debian: sudo apt-get install libzbar0
        - On MacOS: brew install zbar
        - On Windows: The package should work directly
    """)
    st.stop()

def enhance_barcode_image(image):
    """Applies different processing techniques to improve barcode detection"""
    if isinstance(image, np.ndarray):
        img = image.copy()
    else:
        img = cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)
    
    try:
        # Convert to grayscale
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    except Exception as e:
        st.error(f"Error converting image: {str(e)}")
        return []
    
    # List of processed images
    processed_images = []
    
    try:
        # Original grayscale
        processed_images.append(('original_gray', gray))
        
        # Adaptive threshold
        adaptive_threshold = cv2.adaptiveThreshold(
            gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2
        )
        processed_images.append(('adaptive_threshold', adaptive_threshold))
        
        # Otsu threshold
        _, otsu = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        processed_images.append(('otsu', otsu))
        
        # Contrast enhancement
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8))
        clahe_img = clahe.apply(gray)
        processed_images.append(('clahe', clahe_img))
        
        # Noise reduction
        denoised = cv2.fastNlMeansDenoising(gray)
        processed_images.append(('denoised', denoised))
        
        # Sharpen
        kernel = np.array([[-1,-1,-1], [-1,9,-1], [-1,-1,-1]])
        sharpened = cv2.filter2D(gray, -1, kernel)
        processed_images.append(('sharpened', sharpened))
        
        # Dilation + Erosion
        kernel = np.ones((3,3), np.uint8)
        dilated = cv2.dilate(gray, kernel, iterations=1)
        eroded = cv2.erode(dilated, kernel, iterations=1)
        processed_images.append(('morphology', eroded))
        
    except Exception as e:
        st.warning(f"Some image processing operations failed: {str(e)}")
        # Return whatever processing succeeded
        
    return processed_images

def read_barcodes_enhanced(image):
    """Enhanced function to read barcodes and QR codes with error handling"""
    try:
        # Get different processed versions of the image
        processed_images = enhance_barcode_image(image)
        
        if not processed_images:
            st.error("Failed to process image")
            return None, [], {}
        
        results = []
        best_image = None
        max_confidence = 0
        
        # Process each version of the image
        for proc_name, proc_img in processed_images:
            try:
                decoded_objects = decode(proc_img)
                
                for obj in decoded_objects:
                    try:
                        # Extract code data and type
                        data = obj.data.decode('utf-8')
                        type_ = obj.type
                        polygon = obj.polygon
                        confidence = float(obj.quality) if obj.quality else 0
                        
                        # Check if this code was already found
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
                            
                            if confidence > max_confidence:
                                max_confidence = confidence
                                best_image = proc_img
                    except Exception as e:
                        st.warning(f"Error processing individual code: {str(e)}")
                        continue
                        
            except Exception as e:
                st.warning(f"Error processing with method {proc_name}: {str(e)}")
                continue
        
        # Draw results on original image
        if isinstance(image, Image.Image):
            output_img = cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)
        else:
            output_img = image.copy()
        
        for result in results:
            try:
                # Draw polygon
                pts = np.array(result['polygon'], np.int32)
                pts = pts.reshape((-1, 1, 2))
                
                # Color based on code type
                if 'QR' in result['type']:
                    color = (0, 255, 0)  # Green for QR
                elif 'EAN' in result['type'] or 'UPC' in result['type']:
                    color = (255, 0, 0)  # Blue for EAN/UPC
                else:
                    color = (0, 165, 255)  # Orange for others
                
                cv2.polylines(output_img, [pts], True, color, 2)
                
                # Add text with type and confidence
                x = result['polygon'][0].x
                y = result['polygon'][0].y
                confidence_text = f"{result['confidence']:.1f}%" if result['confidence'] else "N/A"
                text = f"{result['type']} ({confidence_text})"
                cv2.putText(output_img, text, (x, y-10), 
                           cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)
            except Exception as e:
                st.warning(f"Error drawing result: {str(e)}")
                continue
        
        return output_img, results, {name: img for name, img in processed_images}
        
    except Exception as e:
        st.error(f"Error in barcode reading process: {str(e)}")
        return None, [], {}

def main():
    # Set up error handling for the page configuration
    try:
        st.set_page_config(page_title="Universal Code Reader", layout="wide")
    except Exception as e:
        st.error("Error configuring page. Please retry loading the application.")
        return

    st.title("Barcode and QR Code Reader")
    
    # Sidebar with options
    st.sidebar.title("Settings")
    source = st.sidebar.radio("Select source:",
                            ["Camera", "Image Upload"])

    # Processing options
    st.sidebar.subheader("Processing Options")
    resize_factor = st.sidebar.slider("Resize factor", 
                                    min_value=0.5, max_value=2.0, 
                                    value=1.0, step=0.1)
    
    show_processed = st.sidebar.checkbox("Show processed images", value=False)

    # Image input handling
    try:
        if source == "Camera":
            img_file_buffer = st.camera_input("Capture code")
        else:
            img_file_buffer = st.file_uploader("Upload an image", 
                                             type=["jpg", "jpeg", "png"])
    except Exception as e:
        st.error(f"Error with image input: {str(e)}")
        return

    if img_file_buffer is not None:
        try:
            # Load and process image
            image = Image.open(img_file_buffer)
            
            # Resize if necessary
            if resize_factor != 1.0:
                new_size = tuple(int(dim * resize_factor) for dim in image.size)
                image = image.resize(new_size, Image.Resampling.LANCZOS)
            
            # Process image and detect codes
            processed_img, results, all_processed = read_barcodes_enhanced(image)
            
            if processed_img is not None:
                # Main layout
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("Original Image")
                    st.image(image, use_column_width=True)
                
                with col2:
                    st.subheader("Detected Codes")
                    if isinstance(processed_img, np.ndarray):
                        processed_img = cv2.cvtColor(processed_img, cv2.COLOR_BGR2RGB)
                    st.image(processed_img, use_column_width=True)
                
                # Show processed images if requested
                if show_processed and all_processed:
                    st.subheader("Processed Images")
                    processed_cols = st.columns(3)
                    for idx, (name, img) in enumerate(all_processed.items()):
                        with processed_cols[idx % 3]:
                            st.caption(name)
                            st.image(img, use_column_width=True)
                
                # Display results
                if results:
                    st.success(f"Detected {len(results)} codes!")
                    
                    # Results table
                    results_data = []
                    for idx, result in enumerate(results, 1):
                        confidence = f"{result['confidence']:.1f}%" if result['confidence'] else "N/A"
                        results_data.append({
                            "#": idx,
                            "Type": result['type'],
                            "Content": result['data'],
                            "Method": result['method'],
                            "Confidence": confidence
                        })
                    
                    st.table(results_data)
                    
                    # Copy results button
                    if st.button("Copy all results"):
                        results_text = "\n".join([
                            f"Code {r['#']}: {r['Content']} ({r['Type']}) - Confidence: {r['Confidence']}"
                            for r in results_data
                        ])
                        st.code(results_text)
                        st.info("Use Ctrl+C or Cmd+C to copy the text above")
                        
                else:
                    st.warning("No codes detected! Try adjusting the image or use another photo.")

        except Exception as e:
            st.error(f"Error processing the image: {str(e)}")
            st.warning("Please try again with another image or adjust the settings.")

if __name__ == '__main__':
    main()