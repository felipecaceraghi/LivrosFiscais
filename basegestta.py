"""
Script para fazer login no Gestta e coletar dados completos dos customer users

FLUXO COMPLETO:
1. Busca todos os customer users (/admin/customer/user)
2. Para cada _id, busca detalhes completos (/admin/customer/user/{_id})
3. Para cada _id, busca accountable (/admin/customer/user/{_id}/accountable)
4. Filtra apenas departamento "Pessoal" 
5. COLETA TODOS os emails únicos para cada código de cliente
6. Monta JSON final: "code": {"name": name, "email": [email1, email2, ...]}

ESTRATÉGIA DE EMAILS: Sempre usa array, coletando TODOS os emails únicos

VERSÃO COM CONCORRÊNCIA - SUPER RÁPIDA! 🚀
"""

import requests
import json
import urllib3
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time

# Desativar avisos SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def create_session():
    """Cria uma sessão com verificação SSL desativada"""
    session = requests.Session()
    session.verify = False
    return session

def get_token(email, password):
    """Faz login na API do Gestta e retorna o token de autorização"""
    print(f"Tentando login com email: {email}")
    
    login_url = "https://api.gestta.com.br/core/login"
    payload = {"email": email, "password": password}
    headers = {
        "Accept": "application/json, text/plain, */*", 
        "Content-Type": "application/json;charset=UTF-8"
    }
    
    try:
        session = create_session()
        response = session.post(login_url, json=payload, headers=headers)
        
        print(f"Status do login: {response.status_code}")
        
        if response.status_code == 200:
            token = response.headers.get("authorization") or response.headers.get("Authorization")
            if token:
                print("✅ Token obtido com sucesso!")
                return token, session
            else:
                print("❌ Token não encontrado na resposta.")
                return None, None
        else:
            print(f"❌ Erro no login: {response.status_code}")
            return None, None
            
    except Exception as e:
        print(f"❌ Exceção ao obter token: {str(e)}")
        return None, None

def save_to_json(data, filename):
    """Salva os dados em um arquivo JSON"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Dados salvos em: {filename}")
        return filename
        
    except Exception as e:
        print(f"❌ Erro ao salvar arquivo JSON: {str(e)}")
        return None

# ETAPA 1: Buscar todos os customer users
def fetch_single_page(token, page, limit=100):
    """Busca uma única página de customer users"""
    session = create_session()
    url = "https://api.gestta.com.br/admin/customer/user"
    headers = {
        "Authorization": token,
        "Accept": "application/json, text/plain, */*"
    }
    
    params = {
        "active": "true",
        "limit": limit,
        "page": page,
        "search": ""
    }
    
    try:
        response = session.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, dict) and "docs" in data:
                return {
                    "page": page,
                    "docs": data["docs"],
                    "count": len(data["docs"]),
                    "success": True,
                    "total_docs": data.get("totalDocs", 0)
                }
        
        return {"page": page, "docs": [], "count": 0, "success": False}
        
    except Exception as e:
        return {"page": page, "docs": [], "count": 0, "success": False, "error": str(e)}

def get_all_customer_users(token, max_workers=15):
    """Busca TODOS os customer users usando concorrência"""
    print("🔍 ETAPA 1: Coletando todos os customer users...")
    
    # Obter informações iniciais
    initial_result = fetch_single_page(token, 1, 100)
    if not initial_result["success"]:
        print("❌ Falha ao obter informações iniciais")
        return []
    
    total_docs = initial_result["total_docs"]
    max_pages = (total_docs + 99) // 100
    
    print(f"📊 Total estimado: {total_docs} users em ~{max_pages} páginas")
    
    all_results = []
    print_lock = threading.Lock()
    
    def safe_print(message):
        with print_lock:
            print(message)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_page = {
            executor.submit(fetch_single_page, token, page): page 
            for page in range(1, max_pages + 5)  # Margem de segurança
        }
        
        for future in as_completed(future_to_page):
            page = future_to_page[future]
            try:
                result = future.result()
                all_results.append(result)
                
                if result["success"] and result["count"] > 0:
                    safe_print(f"✅ Página {page}: {result['count']} users")
                elif result["success"] and result["count"] == 0:
                    safe_print(f"⭕ Página {page}: vazia")
                else:
                    safe_print(f"❌ Página {page}: erro")
                    
            except Exception as e:
                safe_print(f"❌ Erro página {page}: {str(e)}")
    
    # Processar TODOS os resultados coletados
    all_users = []
    successful_pages = []
    
    # Ordenar por página para processar em ordem
    all_results.sort(key=lambda x: x["page"])
    
    for result in all_results:
        if result["success"] and result["count"] > 0:
            all_users.extend(result["docs"])
            successful_pages.append(result["page"])
    
    print(f"✅ ETAPA 1 concluída: {len(all_users)} customer users coletados de {len(successful_pages)} páginas\n")
    return all_users

# ETAPA 2: Buscar detalhes de cada customer user
def fetch_user_details(token, user_id):
    """Busca detalhes completos de um customer user específico"""
    session = create_session()
    url = f"https://api.gestta.com.br/admin/customer/user/{user_id}"
    headers = {
        "Authorization": token,
        "Accept": "application/json, text/plain, */*"
    }
    
    try:
        response = session.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            return {
                "user_id": user_id,
                "success": True,
                "data": response.json()
            }
        else:
            return {
                "user_id": user_id,
                "success": False,
                "error": f"HTTP {response.status_code}"
            }
            
    except Exception as e:
        return {
            "user_id": user_id,
            "success": False,
            "error": str(e)
        }

def get_all_user_details(token, user_list, max_workers=20):
    """Busca detalhes de todos os users usando concorrência"""
    print("🔍 ETAPA 2: Coletando detalhes completos de cada user...")
    
    all_details = []
    processed = 0
    total = len(user_list)
    print_lock = threading.Lock()
    
    def safe_print(message):
        with print_lock:
            print(message)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_id = {
            executor.submit(fetch_user_details, token, user["_id"]): user["_id"]
            for user in user_list
        }
        
        for future in as_completed(future_to_id):
            user_id = future_to_id[future]
            try:
                result = future.result()
                processed += 1
                
                if result["success"]:
                    all_details.append(result["data"])
                    if processed % 50 == 0 or processed == total:
                        safe_print(f"✅ Processados {processed}/{total} detalhes de users")
                else:
                    safe_print(f"❌ Erro no user {user_id}: {result.get('error', 'desconhecido')}")
                    
            except Exception as e:
                processed += 1
                safe_print(f"❌ Exceção no user {user_id}: {str(e)}")
    
    print(f"✅ ETAPA 2 concluída: {len(all_details)} detalhes coletados\n")
    return all_details

# ETAPA 3: Buscar dados de accountable por _id
def fetch_user_accountable(token, user_id):
    """
    Busca dados de accountable usando o _id do usuário
    URL: /admin/customer/user/{_id}/accountable
    """
    session = create_session()
    url = f"https://api.gestta.com.br/admin/customer/user/{user_id}/accountable"
    headers = {
        "Authorization": token,
        "Accept": "application/json, text/plain, */*"
    }
    
    try:
        response = session.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            return {
                "user_id": user_id,
                "success": True,
                "data": response.json()
            }
        else:
            return {
                "user_id": user_id,
                "success": False,
                "error": f"HTTP {response.status_code}"
            }
            
    except Exception as e:
        return {
            "user_id": user_id,
            "success": False,
            "error": str(e)
        }

def get_all_accountable_data(token, user_details, max_workers=20):
    """Busca dados de accountable para todos os users"""
    print("🔍 ETAPA 3: Coletando dados de accountable...")
    
    all_accountable_data = []
    processed = 0
    total = len(user_details)
    print_lock = threading.Lock()
    
    def safe_print(message):
        with print_lock:
            print(message)
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_user = {
            executor.submit(fetch_user_accountable, token, user["_id"]): user
            for user in user_details
        }
        
        for future in as_completed(future_to_user):
            user = future_to_user[future]
            try:
                result = future.result()
                processed += 1
                
                if result["success"]:
                    # Adicionar email do usuário aos dados
                    result["email"] = user.get("email", "")
                    result["name"] = user.get("name", "")
                    all_accountable_data.append(result)
                    
                    if processed % 50 == 0 or processed == total:
                        safe_print(f"✅ Processados {processed}/{total} accountable")
                else:
                    safe_print(f"❌ Erro no user {user.get('_id', '')}: {result.get('error', 'desconhecido')}")
                    
            except Exception as e:
                processed += 1
                safe_print(f"❌ Exceção no user {user.get('_id', '')}: {str(e)}")
    
    print(f"✅ ETAPA 3 concluída: {len(all_accountable_data)} dados de accountable coletados\n")
    return all_accountable_data

def process_final_data(accountable_data):
    """
    Processa os dados finais filtrando apenas departamento "Pessoal"
    e monta JSON no formato: "code": {"name": name, "email": [email1, email2, ...]}
    NOVA VERSÃO: Coleta TODOS os emails únicos para cada código
    """
    print("🔧 Processando dados finais...")
    
    # Dicionário para coletar emails por código
    code_emails = {}  # {code: {"name": name, "emails": set()}}
    pessoal_count = 0
    
    for item in accountable_data:
        if not item["success"]:
            continue
            
        user_email = item.get("email", "")
        user_name = item.get("name", "")
        user_id = item.get("user_id", "")
        accountable_list = item.get("data", [])
        
        if not isinstance(accountable_list, list):
            continue
            
        # Filtrar apenas departamento "Pessoal"
        for accountable in accountable_list:
            company_dept = accountable.get("company_department", {})
            dept_name = company_dept.get("name", "")
            
            if dept_name == "Pessoal":
                customer = accountable.get("customer", {})
                customer_name = customer.get("name", "")
                customer_code = customer.get("code", "")
                
                if customer_code and user_email:
                    # Inicializar se não existir
                    if customer_code not in code_emails:
                        code_emails[customer_code] = {
                            "name": customer_name,
                            "emails": set()
                        }
                    
                    # Adicionar email ao conjunto (set remove duplicatas automaticamente)
                    code_emails[customer_code]["emails"].add(user_email)
                    pessoal_count += 1
    
    # Converter sets para listas ordenadas
    final_result = {}
    multiple_emails_count = 0
    
    for code, data in code_emails.items():
        email_list = sorted(list(data["emails"]))  # Converter set para lista ordenada
        
        final_result[code] = {
            "name": data["name"],
            "email": email_list
        }
        
        if len(email_list) > 1:
            multiple_emails_count += 1
            print(f"📧 Código {code}: {len(email_list)} emails únicos")
            for email in email_list:
                print(f"   - {email}")
    
    print(f"📊 {pessoal_count} registros do departamento 'Pessoal' processados")
    print(f"📊 {len(final_result)} códigos únicos de clientes")
    print(f"📧 {multiple_emails_count} códigos com múltiplos emails")
    
    return final_result, multiple_emails_count

def main():
    """Função principal - Coleta completa em 3 etapas"""
    print("🚀 GESTTA - COLETA COMPLETA DE DADOS\n")
    
    # CREDENCIAIS
    email = "felipe.caceraghi@gofurthergroup.com.br"
    password = "Estopinha1@"  # ← COLOQUE SUA SENHA
    
    # Login
    token, session = get_token(email, password)
    if not token:
        print("❌ Falha no login")
        return
    
    print(f"🔑 Token: {token[:50]}...\n")
    
    start_time = time.time()
    
    # ETAPA 1: Buscar todos os customer users
    all_users = get_all_customer_users(token)
    if not all_users:
        print("❌ Nenhum customer user encontrado")
        return
    
    # ETAPA 2: Buscar detalhes de cada user
    user_details = get_all_user_details(token, all_users)
    if not user_details:
        print("❌ Nenhum detalhe de user coletado")
        return
    
    # ETAPA 3: Buscar dados de accountable para cada user
    accountable_data = get_all_accountable_data(token, user_details)
    if not accountable_data:
        print("❌ Nenhum dado de accountable coletado")
        return
    
    # PROCESSAMENTO FINAL: Filtrar "Pessoal" e montar JSON final
    final_json, multiple_emails_count = process_final_data(accountable_data)
    
    # O resultado já está no formato correto com arrays de emails
    
    # Salvar resultados
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Dados brutos (para debug/análise)
    raw_data = {
        "collection_timestamp": datetime.now().isoformat(),
        "total_time_seconds": round(time.time() - start_time, 2),
        "basic_users": all_users,
        "detailed_users": user_details,
        "accountable_data": accountable_data,
        "statistics": {
            "total_basic_users": len(all_users),
            "total_detailed_users": len(user_details),
            "total_accountable": len(accountable_data),
            "final_pessoal_records": len(final_json),
            "codes_with_multiple_emails": multiple_emails_count
        }
    }
    
    # Dados finais processados
    final_data = {
        "collection_timestamp": datetime.now().isoformat(),
        "total_time_seconds": round(time.time() - start_time, 2),
        "data": final_json,
        "statistics": {
            "total_codes": len(final_json),
            "codes_with_multiple_emails": multiple_emails_count,
            "department_filter": "Pessoal"
        }
    }
    
    # Salvar arquivos
    raw_filename = f"gestta_RAW_data_{timestamp}.json"
    final_filename = f"gestta_FINAL_data_{timestamp}.json"
    
    save_to_json(raw_data, raw_filename)
    save_to_json(final_data, final_filename)
    
    print(f"🎉 COLETA COMPLETA!")
    print(f"📊 {len(all_users)} customer users básicos")
    print(f"📊 {len(user_details)} detalhes completos")
    print(f"📊 {len(accountable_data)} dados de accountable")
    print(f"🎯 {len(final_json)} registros finais (departamento Pessoal)")
    if multiple_emails_count > 0:
        print(f"📧 {multiple_emails_count} códigos com múltiplos emails")
    print(f"⏱️ Tempo total: {round(time.time() - start_time, 2)} segundos")
    print(f"💾 Arquivo RAW: {raw_filename}")
    print(f"💾 Arquivo FINAL: {final_filename}")
    
    # Mostrar exemplo do resultado final
    if final_json:
        print(f"\n🔍 Exemplo do resultado final:")
        example_items = list(final_json.items())[:3]
        for code, data in example_items:
            emails = data["email"]
            if len(emails) == 1:
                print(f'  "{code}": {{"name": "{data["name"]}", "email": ["{emails[0]}"]}}')
            else:
                emails_str = '", "'.join(emails)
                print(f'  "{code}": {{"name": "{data["name"]}", "email": ["{emails_str}"]}}')
        if len(final_json) > 3:
            print(f"  ... e mais {len(final_json) - 3} registros")
    
    # Mostrar casos com múltiplos emails
    if multiple_emails_count > 0:
        print(f"\n📧 CÓDIGOS COM MÚLTIPLOS EMAILS:")
        multi_email_codes = {k: v for k, v in final_json.items() if len(v["email"]) > 1}
        shown = 0
        for code, data in multi_email_codes.items():
            if shown >= 3:  # Mostrar apenas 3 primeiros
                break
            emails = data["email"]
            print(f'  "{code}": {len(emails)} emails - {emails}')
            shown += 1
        if len(multi_email_codes) > 3:
            print(f"  ... e mais {len(multi_email_codes) - 3} códigos com múltiplos emails")
        print(f"\n✅ TODOS os emails únicos foram preservados para cada código!")

if __name__ == "__main__":
    main()