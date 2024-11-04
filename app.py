import streamlit as st
import pymongo
import urllib.parse
import hashlib
import secrets
import requests
from datetime import datetime, timedelta
import re
from dotenv import load_dotenv
import os

class Config:
    def get_brevo_api():
        try:
            return st.secrets["BREVO_API_KEY"]
        except:
            load_dotenv()
            return os.getenv("BREVO_API_KEY")

    BREVO_API_KEY = get_brevo_api()
    if not BREVO_API_KEY:
        st.error("BREVO_API_KEY não encontrada! Verifique suas configurações.")
        
    MONGO_USERNAME = urllib.parse.quote_plus('devpython86')
    MONGO_PASSWORD = urllib.parse.quote_plus('dD@pjl06081420')
    MONGO_CLUSTER = 'cluster0.akbb8.mongodb.net'
    MONGO_DB = 'warehouse'
      
    SENDER_NAME = "Sistema Warehouse"
    SENDER_EMAIL = "pythondeveloper681420@gmail.com"
    
    DEV_URL = "http://localhost:8501"
    PROD_URL = "https://warehouse-app.streamlit.app/"
    
    TOKEN_EXPIRY_HOURS = 24 * 7  # 7 dias
    MIN_PASSWORD_LENGTH = 6
    ALLOWED_EMAIL_DOMAIN = "@andritz.com"

st.set_page_config(initial_sidebar_state="collapsed")

class MongoDBManager:
    def __init__(self):
        self.client = self._connect()
        self.db = self.client[Config.MONGO_DB]
        self.users = self.db['users']
        self.tokens = self.db['tokens']
    
    def _connect(self):
        try:
            connection_string = f"mongodb+srv://{Config.MONGO_USERNAME}:{Config.MONGO_PASSWORD}@{Config.MONGO_CLUSTER}/{Config.MONGO_DB}?retryWrites=true&w=majority"
            return pymongo.MongoClient(connection_string)
        except Exception as e:
            st.error(f"Erro ao conectar ao MongoDB: {str(e)}")
            raise

    def find_user(self, email):
        return self.users.find_one({"email": email})

    def create_user(self, user_data):
        try:
            return self.users.insert_one(user_data)
        except Exception as e:
            st.error(f"Erro ao criar usuário: {str(e)}")
            return None

    def create_token(self, token_data):
        try:
            return self.tokens.insert_one(token_data)
        except Exception as e:
            st.error(f"Erro ao criar token: {str(e)}")
            return None

    def find_token(self, token):
        return self.tokens.find_one({"token": token})

    def update_user(self, email, update_data):
        try:
            result = self.users.update_one(
                {"email": email},
                {"$set": update_data}
            )
            return result.modified_count > 0
        except Exception as e:
            st.error(f"Erro ao atualizar usuário: {str(e)}")
            return False

    def delete_token(self, token):
        try:
            result = self.tokens.delete_one({"token": token})
            return result.deleted_count > 0
        except Exception as e:
            st.error(f"Erro ao deletar token: {str(e)}")
            return False

class EmailManager:
    def __init__(self):
        self.api_url = "https://api.brevo.com/v3/smtp/email"
        self.headers = {
            "accept": "application/json",
            "api-key": Config.BREVO_API_KEY,
            "content-type": "application/json"
        }
    
    def send_validation_email(self, email, token, name):
        try:
            base_url = Config.PROD_URL if st.session_state.get('dev_mode', True) else Config.PROD_URL
            validation_url = f"{base_url}?token={token}"
            
            payload = {
                "sender": {
                    "name": Config.SENDER_NAME,
                    "email": Config.SENDER_EMAIL
                },
                "to": [{
                    "email": email,
                    "name": name
                }],
                "subject": "Validação de Conta - Sistema Warehouse",
                "htmlContent": self._get_email_template(validation_url, name)
            }
            
            response = requests.post(self.api_url, headers=self.headers, json=payload)
            
            if response.status_code in [200, 201]:
                st.info(f"Email de validação enviado para {email}")
                return True
            else:
                st.error(f"Erro ao enviar email: {response.text}")
                return False
                
        except Exception as e:
            st.error(f"Erro ao enviar email: {str(e)}")
            return False

    def _get_email_template(self, validation_url, name):
        return f"""
        <!DOCTYPE html>
        <html>
        <body style="font-family: Arial, sans-serif; margin: 0; padding: 20px;">
            <div style="max-width: 600px; margin: 0 auto; background-color: #ffffff; padding: 20px;">
                <h1>Sistema Warehouse - Validação de Conta</h1>
                <p>Olá {name},</p>
                <p>Para validar sua conta no Sistema Warehouse, clique no link abaixo:</p>
                <p>
                    <a href="{validation_url}" 
                       style="background-color: #0075be; color: white; padding: 0px 20px; 
                              text-decoration: none; border-radius: 5px;">
                        Validar Conta
                    </a>
                </p>
                <p>Este link expira em 7 dias.</p>
                <p>Se você não solicitou esta validação, ignore este email.</p>
            </div>
        </body>
        </html>
        """

class UserManager:
    def __init__(self, db_manager, email_manager):
        self.db = db_manager
        self.email = email_manager

    def get_initials(self, name):
        parts = name.split()
        if len(parts) >= 2:
            return f"{parts[0][0]}{parts[-1][0]}".upper()
        return parts[0][0].upper() if parts else "U"

    def create_user(self, name, email, password, phone):
        if not all([name, email, password, phone]):
            st.error("Todos os campos são obrigatórios")
            return False

        if not email.endswith(Config.ALLOWED_EMAIL_DOMAIN):
            st.error(f"Email deve terminar com {Config.ALLOWED_EMAIL_DOMAIN}")
            return False

        if not password.isdigit() or len(password) != Config.MIN_PASSWORD_LENGTH:
            st.error(f"Senha deve conter exatamente {Config.MIN_PASSWORD_LENGTH} dígitos")
            return False

        if self.db.find_user(email):
            st.error("Email já cadastrado")
            return False

        # Gera token de validação
        token = secrets.token_urlsafe(32)
        
        # Cria usuário
        user_data = {
            "name": name,
            "email": email,
            "password": hashlib.sha256(password.encode()).hexdigest(),
            "phone": re.sub(r'\D', '', phone),
            "verified": False,
            "created_at": datetime.utcnow()
        }
        
        # Salva usuário e token
        if self.db.create_user(user_data):
            token_data = {
                "token": token,
                "email": email,
                "created_at": datetime.utcnow()
            }
            if self.db.create_token(token_data):
                if self.email.send_validation_email(email, token, name):
                    st.success("Cadastro realizado! Verifique seu email para validar a conta.")
                    return True
        
        st.error("Erro ao criar usuário. Tente novamente.")
        return False

    def validate_token(self, token):
        token_doc = self.db.find_token(token)
        if not token_doc:
            st.error("Token inválido ou expirado")
            return False

        # Verifica expiração
        expiry_time = token_doc['created_at'] + timedelta(hours=Config.TOKEN_EXPIRY_HOURS)
        if datetime.utcnow() > expiry_time:
            self.db.delete_token(token)
            st.error("Token expirado. Faça o cadastro novamente.")
            return False

        # Atualiza usuário e remove token
        if self.db.update_user(token_doc['email'], {"verified": True}):
            self.db.delete_token(token)
            st.success("Conta validada com sucesso! Você já pode fazer login.")
            return True
        return False

    def login(self, email, password):
        if not email or not password:
            st.error("Preencha todos os campos")
            return False

        user = self.db.find_user(email)
        if not user:
            st.error("Email ou senha incorretos")
            return False

        if not user.get('verified', False):
            st.error("Conta não verificada. Verifique seu email.")
            return False

        if user['password'] != hashlib.sha256(password.encode()).hexdigest():
            st.error("Email ou senha incorretos")
            return False

        # Gera um novo token de autenticação
        token = secrets.token_urlsafe(32)
        token_data = {
            "token": token,
            "email": email,
            "created_at": datetime.utcnow()
        }
        if self.db.create_token(token_data):
            st.session_state.auth_token = token
            st.session_state.user = {
                'name': user['name'],
                'email': user['email'],
                'initials': self.get_initials(user['name'])
            }
            st.session_state.logged_in = True
            return True

        st.error("Erro ao realizar login. Tente novamente.")
        return False

    def logout(self):
        if 'auth_token' in st.session_state:
            token = st.session_state.pop('auth_token')
            self.db.delete_token(token)
        st.session_state.logged_in = False
        st.session_state.user = None

    def check_login(self):
        if 'auth_token' in st.session_state:
            token = st.session_state.auth_token
            token_doc = self.db.find_token(token)
            if token_doc:
                # Verifica expiração
                expiry_time = token_doc['created_at'] + timedelta(hours=Config.TOKEN_EXPIRY_HOURS)
                if datetime.utcnow() <= expiry_time:
                    user = self.db.find_user(token_doc['email'])
                    st.session_state.user = {
                        'name': user['name'],
                        'email': user['email'],
                        'initials': self.get_initials(user['name'])
                    }
                    st.session_state.logged_in = True
                    return
        self.logout()

class WarehouseApp:
    def __init__(self):
        self.db_manager = MongoDBManager()
        self.email_manager = EmailManager()
        self.user_manager = UserManager(self.db_manager, self.email_manager)

        self.user_manager.check_login()

    def show_sidebar(self):
        with st.sidebar:
            st.title(f"Bem-vindo")
            st.header(f"{st.session_state.user['initiais']}")
            if st.button("Sair"):
                self.user_manager.logout()
                st.rerun()

    def login_page(self):
        st.markdown("""
            <style>
                [data-testid="collapsedControl"] {
                    display: none
                }
                .st-emotion-cache-w3nhqi {display: none}
                .stSidebar {display: none}
            </style>
        """, unsafe_allow_html=True)
        
        st.title("Sistema Warehouse")
        
        token = st.query_params.get("token")
        if token:
            self.user_manager.validate_token(token)
            st.query_params.clear()
            st.rerun()
        
        tab1, tab2 = st.tabs(["Login", "Cadastro"])
        
        with tab1:
            with st.form("login_form"):
                email = st.text_input("Email")
                password = st.text_input("Senha", type="password")
                if st.form_submit_button("Entrar"):
                    if self.user_manager.login(email, password):
                        st.rerun()

        with tab2:
            with st.form("register_form"):
                name = st.text_input("Nome Completo")
                email = st.text_input("Email (@andritz.com)")
                phone = st.text_input("Telefone", placeholder="(XX) XXXXX-XXXX")
                password = st.text_input("Senha (6 dígitos)", type="password")
                if st.form_submit_button("Cadastrar"):
                    self.user_manager.create_user(name, email, password, phone)

    def main_page(self):
        self.show_sidebar()
        
        st.title("Dashboard")
        st.write("Bem-vindo ao Sistema Warehouse!")

    def run(self):
        st.markdown("""
            <style>
                [data-testid="collapsedControl"] {
                    display: none
                }
            </style>
        """, unsafe_allow_html=True)
        
        if not st.session_state.logged_in:
            self.login_page()
        else:
            self.main_page()

if __name__ == "__main__":
    app = WarehouseApp()
    app.run()