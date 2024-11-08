import streamlit as st
from datetime import datetime

st.subheader("Formatar Hora Teste")

if st.button("Teste",type='primary'):
    # Criando um objeto de data para o momento atual
    agora = datetime.now()
    st.text(agora)
    
    # Obtendo a data e hora atual com milissegundos
    randon = datetime.now().strftime("%d%m%Y%H%M%S") + str(datetime.now().microsecond)[:3]
    st.text(randon)
    
    # Obtendo a data e hora atual formatada com milissegundos no final
    agora = datetime.now()
    randon2 = agora.strftime("%d%m%Y%H%M%S") + f"{int(agora.microsecond / 1000):03d}"
    st.text(randon2)
    

    data_formatada = agora.strftime("%d/%m/%Y %H:%M:%S")
    data_formatada=data_formatada.replace("/", "-").replace(":","_")
    st.text(data_formatada)
    
    data_formatada2 = agora.strftime("%d/%m/%Y %H:%M:%S")
    st.text(data_formatada2)

    dia = agora.day
    mes = agora.month
    mes_texto = agora.strftime("%B")  # Mês como texto completo
    ano = agora.year
    
        # Lista de meses em português
    meses_portugues = [
        "janeiro", "fevereiro", "março", "abril", "maio", "junho",
        "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"
    ]
    
        # Obtendo o nome do mês em português
    mes_texto = meses_portugues[mes - 1]
    
    st.text(f'Dia: {dia}')
    st.text(f'Mês: {mes}')
    st.text(f'Mês: {mes_texto}')  # Mês como texto
    st.text(f'Mês: {mes_texto}')  # Mês como texto em português
    st.text(f'Ano: {ano}')
        