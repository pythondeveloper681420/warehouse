import streamlit as st
import cv2
import numpy as np
from pyzbar.pyzbar import decode

def read_barcodes(frame):
    barcodes = decode(frame)
    for barcode in barcodes:
        x, y, w, h = barcode.rect
        barcode_info = barcode.data.decode('utf-8')
        cv2.rectangle(frame, (x, y), (x+w, y+h), (0, 255, 0), 2)
        font = cv2.FONT_HERSHEY_DUPLEX
        cv2.putText(frame, barcode_info, (x + 6, y - 6), font, 0.5, (255, 255, 255), 1)
    return frame

def main():
    st.title("Leitor de Código de Barras e QR Code")
    st.text("Aponte a câmera para um código de barras ou QR code.")

    video = cv2.VideoCapture(0)

    while True:
        _, frame = video.read()
        frame = read_barcodes(frame)
        cv2.imshow('Código de Barras e QR Code', frame)
        if cv2.waitKey(1) & 0xFF == 27:
            break

    video.release()
    cv2.destroyAllWindows()

if __name__ == '__main__':
    main()