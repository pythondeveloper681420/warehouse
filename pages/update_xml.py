import streamlit as st
import pandas as pd
import os
import xml.etree.ElementTree as ET
from datetime import date
import re
import time
import pickle
import numpy as np
import io
#format_date_to_brazilian
#df['Mês']
# Page configuration
st.set_page_config(
    page_title="XML Invoice Processor",
    page_icon=":page_with_curl:",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Main title
st.header("📃 Processamento de Arquivos XML")

def clean_description(description):
    """Remove múltiplos espaços consecutivos e espaços no início e no final da string."""
    if description is None:
        return ""
    description = re.sub(' +', ' ', description)
    description = description.strip()
    return description

def filter_info_adic(info_adic):
    """Filtra a informação adicional para encontrar prefixos específicos e formata o texto resultante."""
    if not info_adic:
        return ""
    prefixos = ['4501', '4502', '4503', '4504', '4505']
    palavras = info_adic.split()
    palavras_filtradas = [p[:10] for p in palavras if any(p.startswith(prefix) for prefix in prefixos)]
    if palavras_filtradas:
        return ' '.join(palavras_filtradas)
    return ""

def format_value(value_str):
    """Formata o valor substituindo vírgulas por pontos e convertendo para float se possível."""
    if isinstance(value_str, str):
        value_str = value_str.replace('.', '').replace(',', '.')
        try:
            return float(value_str)
        except ValueError:
            return value_str
    elif isinstance(value_str, (int, float)):
        return value_str
    return ""

class ReadXML:
    def __init__(self, files):
        self.files = files

    def nfe_data(self, xml_file):
        """Extrai dados da NFe de um arquivo XML e retorna uma lista de dados para cada item da nota fiscal."""
        root = ET.parse(xml_file).getroot()
        nsNFe = {"ns": "http://www.portalfiscal.inf.br/nfe"}
        
# Acessar o elemento <infNFe> e extrair o atributo Id
        infNFe = root.find(".//ns:infNFe", nsNFe)
        if infNFe is not None:
            chNFe = infNFe.attrib.get('Id', '')
        else:
            chNFe = ""

        
        #Dados gerais
        NFe = self.check_none(root.find("./ns:NFe/ns:infNFe/ns:ide/ns:nNF", nsNFe))
        serie = self.check_none(root.find("./ns:NFe/ns:infNFe/ns:ide/ns:serie", nsNFe))
        natOp = self.check_none(root.find("./ns:NFe/ns:infNFe/ns:ide/ns:natOp", nsNFe))
        data_emissao = self.check_none(root.find("./ns:NFe/ns:infNFe/ns:ide/ns:dhEmi", nsNFe))
        #data_emissao = f"{data_emissao[8:10]}/{data_emissao[5:7]}/{data_emissao[:4]}" if data_emissao else ""
        info_adic = self.check_none(root.find("./ns:NFe/ns:infNFe/ns:infAdic/ns:infCpl", nsNFe))
        dVenc = self.check_none(root.find("./ns:NFe/ns:infNFe/ns:cobr/ns:dup/ns:dVenc", nsNFe))
        #dVenc = f"{dVenc[8:10]}/{dVenc[5:7]}/{dVenc[:4]}" if dVenc else ""
        #dVenc = dVenc.replace("//", "")

        # Dados Emitente
        emit = root.find("./ns:NFe/ns:infNFe/ns:emit", nsNFe)
        emit_data = {
            'CNPJ Emitente': self.check_none(emit.find("ns:CNPJ", nsNFe)) if emit is not None else "",
            'Nome Emitente': self.check_none(emit.find("ns:xNome", nsNFe)) if emit is not None else "",
            'IE Emitente': self.check_none(emit.find("ns:IE", nsNFe)) if emit is not None else "",
            'Endereco Emitente': {
                'Logradouro': self.check_none(emit.find("ns:enderEmit/ns:xLgr", nsNFe)) if emit is not None else "",
                'Número': self.check_none(emit.find("ns:enderEmit/ns:nro", nsNFe)) if emit is not None else "",
                'Complemento': self.check_none(emit.find("ns:enderEmit/ns:complemento", nsNFe)) if emit is not None else "",
                'Bairro': self.check_none(emit.find("ns:enderEmit/ns:xBairro", nsNFe)) if emit is not None else "",
                'Município': self.check_none(emit.find("ns:enderEmit/ns:xMun", nsNFe)) if emit is not None else "",
                'UF': self.check_none(emit.find("ns:enderEmit/ns:UF", nsNFe)) if emit is not None else "",
                'CEP': self.check_none(emit.find("ns:enderEmit/ns:CEP", nsNFe)) if emit is not None else "",
                'País': self.check_none(emit.find("ns:enderEmit/ns:cPais", nsNFe)) if emit is not None else ""
            }
        }

        # Dados Destinatário
        dest = root.find("./ns:NFe/ns:infNFe/ns:dest", nsNFe)
        dest_data = {
            'CNPJ Destinatário': self.check_none(dest.find("ns:CNPJ", nsNFe)) if dest is not None else "",
            'Nome Destinatário': self.check_none(dest.find("ns:xNome", nsNFe)) if dest is not None else "",
            'IE Destinatário': self.check_none(dest.find("ns:IE", nsNFe)) if dest is not None else "",
            'Endereco Destinatário': {
                'Logradouro': self.check_none(dest.find("ns:enderDest/ns:xLgr", nsNFe)) if dest is not None else "",
                'Número': self.check_none(dest.find("ns:enderDest/ns:nro", nsNFe)) if dest is not None else "",
                'Complemento': self.check_none(dest.find("ns:enderDest/ns:complemento", nsNFe)) if dest is not None else "",
                'Bairro': self.check_none(dest.find("ns:enderDest/ns:xBairro", nsNFe)) if dest is not None else "",
                'Município': self.check_none(dest.find("ns:enderDest/ns:xMun", nsNFe)) if dest is not None else "",
                'UF': self.check_none(dest.find("ns:enderDest/ns:UF", nsNFe)) if dest is not None else "",
                'CEP': self.check_none(dest.find("ns:enderDest/ns:CEP", nsNFe)) if dest is not None else "",
                'País': self.check_none(dest.find("ns:enderDest/ns:cPais", nsNFe)) if dest is not None else ""
            }
        }

        # Dados Cobrança
        cobr = root.find("./ns:NFe/ns:infNFe/ns:cobr", nsNFe)
        cobr_data = self.extract_cobr_data(cobr, nsNFe) if cobr is not None else {}

        # Dados do Item
        itemNota = 1
        notas = []

        for item in root.findall("./ns:NFe/ns:infNFe/ns:det", nsNFe):
            # Dados do Item
            cod = self.check_none(item.find(".ns:prod/ns:cProd", nsNFe))
            qntd = self.check_none(item.find(".ns:prod/ns:qCom", nsNFe))
            descricao = self.check_none(item.find(".ns:prod/ns:xProd", nsNFe))
            unidade_medida = self.check_none(item.find(".ns:prod/ns:uCom", nsNFe))
            vlUnProd = self.check_none(item.find(".ns:prod/ns:vUnCom", nsNFe))
            valorProd = self.check_none(item.find(".ns:prod/ns:vProd", nsNFe))
            ncm = self.check_none(item.find(".ns:prod/ns:NCM", nsNFe))
            cfop = self.check_none(item.find(".ns:prod/ns:CFOP", nsNFe))
            xPed = self.check_none(item.find(".ns:prod/ns:xPed", nsNFe))
            nItemPed = self.check_none(item.find(".ns:prod/ns:nItemPed", nsNFe))
            infAdProd = self.check_none(item.find(".ns:infAdProd", nsNFe))

            valorNfe = format_value(self.check_none(root.find("./ns:NFe/ns:infNFe/ns:total/ns:ICMSTot/ns:vNF", nsNFe)))
            valor_frete = format_value(self.check_none(root.find("./ns:NFe/ns:infNFe/ns:total/ns:ICMSTot/ns:vFrete", nsNFe)))
            data_importacao = self.check_none(root.find("./ns:NFe/ns:infNFe/ns:transp/ns:vol/ns:veicId", nsNFe))
            usuario = self.check_none(root.find("./ns:NFe/ns:infNFe/ns:transp/ns:vol/ns:placa", nsNFe))
            data_saida = self.check_none(root.find("./ns:NFe/ns:infNFe/ns:transp/ns:vol/ns:uf", nsNFe))

            dados = [chNFe, NFe, serie, natOp, data_emissao, info_adic, dVenc, 
                    emit_data['CNPJ Emitente'], emit_data['Nome Emitente'],
                    dest_data['CNPJ Destinatário'], dest_data['Nome Destinatário'], valorNfe, valor_frete, itemNota, cod, qntd, descricao, unidade_medida, vlUnProd, valorProd, ncm, cfop , xPed, nItemPed,
                    infAdProd, data_importacao, usuario, data_saida,
                    cobr_data.get('Fatura', ''), cobr_data.get('Duplicata', ''), cobr_data.get('Valor Original', ''), cobr_data.get('Valor Pago', ''),
                    emit_data['Endereco Emitente']['Logradouro'], emit_data['Endereco Emitente']['Número'], emit_data['Endereco Emitente']['Complemento'],
                    emit_data['Endereco Emitente']['Bairro'], emit_data['Endereco Emitente']['Município'], emit_data['Endereco Emitente']['UF'],
                    emit_data['Endereco Emitente']['CEP'], emit_data['Endereco Emitente']['País'],
                    dest_data['Endereco Destinatário']['Logradouro'], dest_data['Endereco Destinatário']['Número'],
                    dest_data['Endereco Destinatário']['Complemento'], dest_data['Endereco Destinatário']['Bairro'],
                    dest_data['Endereco Destinatário']['Município'], dest_data['Endereco Destinatário']['UF'],
                    dest_data['Endereco Destinatário']['CEP'], dest_data['Endereco Destinatário']['País']]
            
            notas.append(dados)
            itemNota += 1
        
        return notas

    def check_none(self, var):
        """Verifica se o elemento XML é None e retorna uma string vazia, caso contrário, retorna o texto do elemento."""
        if var is None:
            return ""
        try:
            return var.text.replace('.', '.') if var.text else ""
        except:
            return ""

    def extract_cobr_data(self, cobr, nsNFe):
        """Extrai os dados da seção <cobr>, incluindo <fat> e <dup>."""
        if cobr is None:
            return {
                'Fatura': '',
                'Duplicata': '',
                'Valor Original': '',
                'Valor Pago': ''
            }
        
        cobr_data = {
            'Fatura': self.check_none(cobr.find("ns:fat/ns:nFat", nsNFe)),
            'Duplicata': self.check_none(cobr.find("ns:dup/ns:nDup", nsNFe)),
            'Valor Original': format_value(self.check_none(cobr.find("ns:fat/ns:vOrig", nsNFe))),
            'Valor Pago': format_value(self.check_none(cobr.find("ns:fat/ns:vLiq", nsNFe)))
        }
        return cobr_data

    def process_xml_files(self):
        """Processa todos os arquivos XML carregados"""
        dados = []
        for uploaded_file in self.files:
            result = self.nfe_data(uploaded_file)
            dados.extend(result)
        return dados

def main():
    # # Page configuration
    # st.set_page_config(
    #     page_title="XML Invoice Processor", 
    #     page_icon=":page_with_curl:", 
    #     layout="wide"
    # )

    # # Main title
    # st.title("XML Invoice Processor 📄")

    # Create tabs
    tab1, tab2, tab3 = st.tabs(["📤 Upload e Extração", "📊 Visualização de Dados", "❓ Como Utilizar"])

    with tab1:
        # File uploader for XML files
        uploaded_files = st.file_uploader(
            "Upload XML Files", 
            type=['xml'], 
            accept_multiple_files=True
        )

        if uploaded_files:
            # Progress bar
            progress_bar = st.progress(0)
            for percent_complete in range(100):
                time.sleep(0.01)
                progress_bar.progress(percent_complete + 1)
            progress_bar.empty()

            # Process XML files
            xml_reader = ReadXML(uploaded_files)
            dados = xml_reader.process_xml_files()

    # Criando DataFrame Pandas
            df = pd.DataFrame(dados, columns=[
                'chaveNfe', 'NFe', 'Série', 'natOp','Data de Emissão', 'info_adic', 'dVenc', 'CNPJ Emitente', 'Nome Emitente',
                'CNPJ Destinatário', 'Nome Destinatário', 'Valor NF-e', 'Valor Frete', 'Item Nota', 'Cód Produto',
                'Quantidade', 'Descrição', 'Unidade Medida', 'vlUnProd', 'vlTotProd', 'ncm', 'cfop' ,'xPed', 'nItemPed',
                'infAdProd', 'Data Importação', 'Usuário', 'Data Saída', 'Fatura', 'Duplicata', 'Valor Original', 'Valor Pago',
                'Logradouro Emitente', 'Número Emitente', 'Complemento Emitente', 'Bairro Emitente', 'Município Emitente',
                'UF Emitente', 'CEP Emitente', 'País Emitente', 'Logradouro Destinatário', 'Número Destinatário',
                'Complemento Destinatário', 'Bairro Destinatário', 'Município Destinatário', 'UF Destinatário',
                'CEP Destinatário', 'País Destinatário'
            ])

            colunas = [
                'chaveNfe', 'NFe', 'Nome Emitente', 'Descrição', 'Série', 'natOp','Data de Emissão', 'info_adic', 'dVenc', 
                'CNPJ Emitente', 'CNPJ Destinatário', 'Nome Destinatário', 'Valor NF-e', 'Valor Frete', 'Item Nota', 
                'Cód Produto', 'Quantidade', 'Unidade Medida', 'vlUnProd', 'vlTotProd', 'ncm', 'cfop', 'xPed', 'nItemPed', 
                'infAdProd', 'Data Importação', 'Usuário', 'Data Saída', 'Fatura', 'Duplicata', 'Valor Original', 
                'Valor Pago', 'Logradouro Emitente', 'Número Emitente', 'Complemento Emitente', 'Bairro Emitente', 
                'Município Emitente', 'UF Emitente', 'CEP Emitente', 'País Emitente', 'Logradouro Destinatário', 
                'Número Destinatário', 'Complemento Destinatário', 'Bairro Destinatário', 'Município Destinatário', 
                'UF Destinatário', 'CEP Destinatário', 'País Destinatário'
            ]

            df = df.reindex(columns=colunas)
            
            # df=df_formatted
            def convert_to_decimal(df, columns, decimal_places=2):
                """Converte várias colunas para float (decimais) e arredonda para o número especificado de casas decimais."""
                for column in columns:
                    # Converte para float e força valores inválidos para NaN
                    df[column] = pd.to_numeric(df[column], errors='coerce').astype(float)
                    # Arredonda para o número de casas decimais especificado
                    df[column] = df[column].round(decimal_places)
                return df

            # Supondo que você queira converter as colunas 'preco' e 'quantidade' para decimais com duas casas
            columns_to_convert = ['Quantidade','vlUnProd','vlTotProd']

            # Converter as colunas para decimal (float) com duas casas decimais
            df = convert_to_decimal(df, columns_to_convert, decimal_places=2) 
            
            # Função para formatar os números corretamente, lidando com valores vazios
            def formatar_numero(x):
                if pd.isna(x) or x == '':  # Se o valor for nulo ou string vazia
                    return None  # Retorna None para manter o valor como NaN
                else:
                    x_str = str(int(x))  # Certificar que seja inteiro e sem pontos ou espaços
                    
                    # Garantindo que o número tenha pelo menos dois dígitos
                    if len(x_str) > 2:
                        # Inserindo o ponto decimal entre os dois últimos dígitos
                        return x_str[:-2] + "." + x_str[-2:]
                    else:
                        # Se o número for menor que 100, apenas adiciona '0.xx'
                        return "0." + x_str.zfill(2)


            # Aplicar a função diretamente nas colunas existentes
            colunas_para_formatar = ['Valor NF-e', 'Valor Original', 'Valor Pago']
            
            for coluna in colunas_para_formatar:
                # Formatar os números na coluna e converter para float
                df[coluna] = df[coluna].apply(formatar_numero).astype(float)
                    
            # Agrupando por 'Category' e somando os valores de 'Value'
            df['vlNf'] = df.groupby('chaveNfe')['vlTotProd'].transform('sum')

            # Adicionando coluna 'unique' 
            df['unique'] = df['NFe'] + df['Item Nota'].astype(str) + df['Descrição']
            df['Descrição'] = df['Descrição'].apply(clean_description).str.upper()
            
            # Aplicar a função para filtrar e formatar a coluna 'info_adic'
            df['po'] = df['info_adic'].fillna("") + " " + df['xPed'].fillna("") + " " + df['nItemPed'].fillna("") + " " + df['infAdProd'].fillna("")
            df['po'] = df['po'].apply(filter_info_adic)
            
            # Função para truncar os primeiros 10 caracteres
            def truncate_to_10_chars(text):
                return text[:10] if text else ""

            df['po'] = df['po'].apply(truncate_to_10_chars)


            # Função para obter o primeiro valor não vazio para cada 'chaveNfe'
            def get_first_non_empty_po(df):
                first_non_empty_po = {}
                for chave, po in zip(df['chaveNfe'], df['po']):
                    if pd.notna(po) and po != '' and chave not in first_non_empty_po:
                        first_non_empty_po[chave] = po
                return first_non_empty_po
                

            # Atualiza a coluna 'po' com o primeiro valor não vazio
            first_po_dict = get_first_non_empty_po(df)
            df['po'] = df['chaveNfe'].map(first_po_dict)
                   
            # Remover linhas duplicadas com base na coluna 'unique'
            df.drop_duplicates(subset='unique', inplace=True)

            # Remover a coluna 'unique'
            #df = df.drop(columns=['unique'], axis=1)
              
            def format_date_to_brazilian(df, columns):
                """
                Converte as colunas especificadas para o formato de data brasileiro (dd/mm/aaaa).
                
                :param df: DataFrame a ser modificado
                :param columns: Lista de nomes das colunas a serem formatadas
                :return: DataFrame com as colunas formatadas como datas brasileiras
                """
                for column in columns:
                    # First, try converting to datetime, handling different potential input formats
                    try:
                        # Try multiple datetime parsing strategies
                        df[column] = pd.to_datetime(
                            df[column], 
                            format='%Y-%m-%d',  # ISO format
                            errors='coerce'
                        )
                    except:
                        try:
                            df[column] = pd.to_datetime(
                                df[column], 
                                format='%d/%m/%Y',  # Brazilian format
                                errors='coerce'
                            )
                        except:
                            # If conversion fails, leave the column as is
                            continue
                    
                    # Format to Brazilian date string only for non-null values
                    df[column] = df[column].dt.strftime('%d/%m/%Y')
                
                return df

            # Aplicar a formatação desejada
            df = format_date_to_brazilian(df, ['dVenc'])
                                        
            #Função para formatar colunas como moeda brasileira (BRL)
            
            def format_to_brl_currency(df, columns):
                """
                Formata as colunas especificadas do DataFrame para o formato de moeda brasileiro (Real) com duas casas decimais.
                Valores não numéricos ou vazios são substituídos por uma string vazia.
                
                :param df: DataFrame a ser modificado
                :param columns: Lista de nomes das colunas a serem formatadas
                :return: DataFrame com as colunas formatadas como moeda brasileira
                """
                for column in columns:
                    df[column] = pd.to_numeric(df[column], errors='coerce')

                    #df[column] = df[column].apply(lambda x: f'R${x:,.2f}'.replace('.', ',') if pd.notna(x) else '')
                    
                    # Convertendo valores para formato desejado
                    df[column] = df[column].apply(lambda x: 'R$ {:,.2f}'.format(x / 1))
                    df[column] = df[column].str.replace(',', 'X').str.replace('.', ',').str.replace('X', '.')
                return df

            df = df
                            
            def convert_columns_to_numeric(df, columns):
                """Converte várias colunas para numérico, forçando erros para NaN."""
                for column in columns:
                    df[column] = pd.to_numeric(df[column], errors='coerce')
                return df

            # Supondo que você queira converter as colunas 'po' e 'NFe'
            columns_to_convert = ['po','NFe','Série','CNPJ Emitente','CNPJ Destinatário','ncm','cfop','CEP Emitente','País Emitente','CEP Destinatário','País Destinatário'] 
            
            # Converter as colunas relevantes para numérico
            df = convert_columns_to_numeric(df, columns_to_convert)   

            # Ordenar o DataFrame pela coluna 'Data' do mais novo para o mais velho
            df = df.sort_values(by='Data de Emissão', ascending=False)  

            # Selecionando colunas, renomeando e reordenando
            colunas_para_exibir =['chaveNfe','NFe','Nome Emitente','Descrição','Série','natOp','Data de Emissão',
                                    'info_adic','dVenc','CNPJ Destinatário','Nome Destinatário','Valor NF-e',
                                    'Valor Frete','Item Nota','Cód Produto','Quantidade','Unidade Medida','vlUnProd','vlTotProd',
                                    'ncm','cfop','xPed','nItemPed','infAdProd','Data Importação','Usuário','Data Saída','Fatura',
                                    'Duplicata','Valor Original','Valor Pago',
                                    'Logradouro Emitente','Número Emitente','Complemento Emitente','Bairro Emitente','Município Emitente','UF Emitente','CEP Emitente','País Emitente',
                                    'Logradouro Destinatário','Número Destinatário','Complemento Destinatário','Bairro Destinatário','Município Destinatário','UF Destinatário','CEP Destinatário','País Destinatário',
                                    'vlNf','po','unique']
            
            # Renomear as colunas

            df = df.rename(columns={'NFe': 'nNf', 'Data de Emissão': 'dtEmi','Item Nota':'itemNf','Descrição':'nomeMaterial','ncm':'ncm','Quantidade':'qtd',
                            'Unidade Medida':'und','vlUnProd':'vlUnProd','vlTotProd':'vlTotProd','Valor NF-e':'vlTotalNf',
                            'dVenc':'dVenc','po':'po',
                            'chaveNfe':'chNfe',
                            'Nome Emitente': 'emitNome','CNPJ Emitente':'emitCnpj','Logradouro Emitente':'emitLogr','Número Emitente':'emitNr','Complemento Emitente':'emitCompl','Bairro Emitente':'emitBairro','Município Emitente':'emitMunic','UF Emitente':'emitUf','CEP Emitente':'emitCep','País Emitente':'emitPais',
                            'Nome Destinatário': 'destNome','CNPJ Destinatário':'destCnpj','Logradouro Destinatário':'destLogr','Número Destinatário':'destNr','Complemento Destinatário':'destCompl','Bairro Destinatário':'destBairro','Município Destinatário':'destMunic','UF Destinatário':'destUf','CEP Destinatário':'destCep','País Destinatário':'destPais',
                            'cfop':'cfop','unique':'unique'})

            # Exibir apenas as colunas renomeadas
            colunas_renomeadas = ['nNf', 'dtEmi', 'itemNf','nomeMaterial','ncm','qtd','und','vlUnProd','vlTotProd','vlTotalNf','po','dVenc','chNfe',
                                    'emitNome','emitCnpj','emitLogr','emitNr','emitCompl','emitBairro','emitMunic','emitUf','emitCep','emitPais',
                                    'destNome','destCnpj','destLogr','destNr','destCompl','destBairro','destMunic','destUf','destCep','destPais',
                                    'cfop','unique']
            
            df= df[colunas_renomeadas]
            
            
            # Converter as colunas para string
            df['emitCnpj'] = df['emitCnpj'].astype(str).replace('.0','')
            df['destCnpj'] = df['destCnpj'].astype(str).replace('.0','')

            # Garantir que as colunas tenham 14 dígitos
            df['emitCnpj'] = df['emitCnpj'].str.zfill(14)
            df['destCnpj'] = df['destCnpj'].str.zfill(14)
            
            st.write(f"Quantidade de linhas: {df.shape[0]}")
            
            # Remover duplicatas com base em 'col1' e 'col2'
            df_unique = df.drop_duplicates(subset=['chNfe', 'po'])
            
            df_po = df_unique.groupby(['chNfe', 'po']).agg(
                vlRecPo=('vlTotalNf', 'sum'),
                qtdNfPo=('vlTotalNf', 'count')
            ).reset_index()
            
            #df_po=df_po['po','vlRecPo','qtdNfPo']
            df_merged = df.merge(df_po, on='po', how='left')
 
            df_merged =df_merged.rename(columns={'chNfe_x':'chNfe'})
                            # Exibir apenas as colunas renomeadas
            colunas_renomeadas = ['nNf', 'dtEmi', 'itemNf','nomeMaterial','ncm','qtd','und','vlUnProd','vlTotProd','vlTotalNf','po','vlRecPo','qtdNfPo','dVenc','chNfe',
                                    'emitNome','emitCnpj','emitLogr','emitNr','emitCompl','emitBairro','emitMunic','emitUf','emitCep','emitPais',
                                    'destNome','destCnpj','destLogr','destNr','destCompl','destBairro','destMunic','destUf','destCep','destPais',
                                    'cfop']
            
            df_merged= df_merged[colunas_renomeadas]
            
            df_merged['unique'] = df_merged['nNf'].astype(str) + df_merged['itemNf'].astype(str) + df_merged['nomeMaterial']
                            # Remover linhas duplicadas com base na coluna 'unique'
            df_merged.drop_duplicates(subset='unique', inplace=True)

            # Remover a coluna 'unique'
            #df_merged = df_merged.drop(columns=['unique'], axis=1)
            
            df=df_merged
            df = df.sort_values(by=['dtEmi','nNf','itemNf'], ascending=[False,True,True])
            #df = df.sort_values(by=,'nNf','itemNf'], ascending=[False,True,True])  

            # Download buttons
            def convert_df_to_excel(df):
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    df.to_excel(writer, index=False, sheet_name='Invoices')
                return output.getvalue()

            def convert_df_to_pickle(df):
                return pickle.dumps(df)

            col1, col2 = st.columns(2)
            
            with col1:
                excel_file = convert_df_to_excel(df)
                st.download_button(
                    label="Download Excel",
                    data=excel_file,
                    file_name="processed_invoices.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            with col2:
                pickle_file = convert_df_to_pickle(df)
                st.download_button(
                    label="Download Pickle",
                    data=pickle_file,
                    file_name="processed_invoices.pkl",
                    mime="application/octet-stream"
                )

            st.success(f"Processed {len(uploaded_files)} XML files")
    with tab2:
        st.header("Visualização de Dados")
        if 'df' in locals():
            # Key Metrics
            col1, col2, col3 = st.columns(3)
            
            with col1:
                total_invoices = len(df)
                st.metric(label="Total de Linhas", value=total_invoices)
            
            with col2:
                unique_issuers = df['emitNome'].nunique()
                st.metric(label="Número de Fornecedores", value=unique_issuers)
            
            with col3:
                unique_issuers = df['nNf'].nunique()
                st.metric(label="Número de Notas Fiscais", value=unique_issuers)
            
            # Global Search Filter
            st.subheader("Filtrar Dados")
            search_term = st.text_input("Busca Global (filtra em todas as colunas)")
            
            if search_term:
                # Create a boolean mask that checks if the search term is in any column
                mask = df.apply(lambda row: row.astype(str).str.contains(search_term, case=False).any(), axis=1)
                filtered_df = df[mask]
            else:
                filtered_df = df
            
            # Display filtered DataFrame without index
            st.dataframe(filtered_df, hide_index=True)
            
        else:
            st.warning("Primeiro processe alguns arquivos XML na aba de Processamento")

    with tab3:
        st.header("Como Utilizar o Aplicativo")
        st.markdown("""
        ### Processamento de Arquivos XML 🔬

        1. **Carregue seus arquivos XML**
        - Clique em "Upload XML Files"
        - Selecione um ou mais arquivos XML de notas fiscais

        2. **Processamento Automático**
        - O aplicativo processará automaticamente os arquivos
        - Uma barra de progresso será exibida durante o processamento

        3. **Visualização dos Dados**
        - Os dados processados serão exibidos em uma tabela
        - Você pode fazer download em formato Excel ou Pickle

        ### Recursos Principais 📊

        - Extração de informações de notas fiscais
        - Limpeza e formatação dos dados
        - Suporte para múltiplos arquivos XML
        - Visualização de dados processados
        - Download em diferentes formatos

        ### Dicas 💡

        - Certifique-se de que os arquivos XML são de notas fiscais brasileiras (NF-e)
        - O processamento pode levar alguns segundos dependendo do número de arquivos
        - Verifique sempre os dados processados antes do download

        ### Formatos Suportados
        - Arquivos XML com estrutura de Nota Fiscal Eletrônica (NF-e)
        """)

if __name__ == "__main__":
    main()
    
# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center'>
        <p>Desenvolvido com ❤️ | XML Processor Pro v1.0</p>
    </div>
    """,
    unsafe_allow_html=True
)      