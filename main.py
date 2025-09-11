from flask import Flask, render_template_string, request, jsonify, send_file
import json
import os
import threading
import sys
import time
import random
import shutil
import uuid
from pathlib import Path
from datetime import datetime, date
from threading import Lock
import asyncio
import logging

# Configurar logging para evitar mensagens desnecessárias
logging.getLogger('websockets').setLevel(logging.WARNING)

try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    print("⚠️ AVISO: Módulo 'websockets' não encontrado. Monitor do banco será desabilitado.")
    print("   Para instalar: pip install websockets")
    WEBSOCKETS_AVAILABLE = False

# --- CONFIGURAÇÕES DO WEBSOCKET ---
WEBSOCKET_URI = "ws://localhost:8765"
RECONNECT_DELAY = 5

def quebrar_periodo_em_meses(data_inicio_str, data_fim_str):
    """
    Quebra um período em intervalos mensais.
    Retorna lista de tuplas (data_inicio, data_fim) para cada mês.
    """
    from datetime import datetime, date
    import calendar
    
    data_inicio = datetime.strptime(data_inicio_str, '%Y-%m-%d').date()
    data_fim = datetime.strptime(data_fim_str, '%Y-%m-%d').date()
    
    periodos = []
    data_atual = data_inicio
    
    while data_atual <= data_fim:
        # Primeiro dia do mês atual (ou data_inicio se for o primeiro mês)
        inicio_mes = max(data_atual.replace(day=1), data_inicio)
        
        # Último dia do mês atual (ou data_fim se for o último mês)
        ultimo_dia_mes = calendar.monthrange(data_atual.year, data_atual.month)[1]
        fim_mes = min(data_atual.replace(day=ultimo_dia_mes), data_fim)
        
        periodos.append((inicio_mes.strftime('%Y-%m-%d'), fim_mes.strftime('%Y-%m-%d')))
        
        # Próximo mês
        if data_atual.month == 12:
            data_atual = data_atual.replace(year=data_atual.year + 1, month=1, day=1)
        else:
            data_atual = data_atual.replace(month=data_atual.month + 1, day=1)
    
    return periodos


# --- SIMULAÇÃO (Usada apenas se os módulos reais não forem encontrados) ---
def generic_mock_book_generator(**kwargs):
    module_name = kwargs.pop('__module_name__', 'UnknownModule')
    print(f"Executando simulação de {module_name} com args: {kwargs}")
    time.sleep(random.uniform(0.8, 1.5)) # Tempo realista
    
    # Simula diferentes cenários: alguns módulos às vezes falham
    scenarios = {
        'LivroIpi': lambda: 'sucesso',
        'LivroIss': lambda: 'sucesso', 
        'LivroICMS': lambda: random.choice(['sucesso', 'erro']),  # Às vezes falha
        'LivroEntradas': lambda: random.choice(['sucesso', 'erro']),  # Às vezes falha
        'LivroSaidas': lambda: 'sucesso'
    }
    
    scenario_func = scenarios.get(module_name, lambda: 'sucesso')
    scenario_result = scenario_func()
    
    output_dir = Path("output_robo")
    output_dir.mkdir(exist_ok=True)
    base_name = f"{module_name}_{kwargs['codi_emp']}_{kwargs['data_inicio'].replace('-', '')}"
    generated_files = []
    
    # Se erro, não gera arquivos
    if scenario_result == 'erro':
        print(f"❌ Simulação de {module_name} falhou - problema na geração")
        return []
    
    # Se sucesso, gera arquivos
    print(f"✅ Simulação de {module_name} bem-sucedida")
    if kwargs.get('gerar_pdf') or kwargs.get('exportar_pdf'):
        pdf_file = output_dir / f"{base_name}.pdf"
        pdf_file.touch()
        generated_files.append(str(pdf_file))
    if kwargs.get('gerar_xlsx') or kwargs.get('exportar_xlsx'):
        xlsx_file = output_dir / f"{base_name}.xlsx"
        xlsx_file.touch()  
        generated_files.append(str(xlsx_file))
    
    print(f"Arquivos gerados: {generated_files}")
    return generated_files

try: from LivroIpi import gerarLivroDeIpi
except ImportError:
    print("AVISO: Módulo 'LivroIpi' não encontrado. Usando simulação.")
    gerarLivroDeIpi = lambda **kwargs: generic_mock_book_generator(__module_name__='LivroIpi', **kwargs)
try: from LivroIss import gerar_livro_iss
except ImportError:
    print("AVISO: Módulo 'LivroIss' não encontrado. Usando simulação.")
    gerar_livro_iss = lambda **kwargs: generic_mock_book_generator(__module_name__='LivroIss', **kwargs)
try: from LivroICMS import gerarLivroICMS
except ImportError:
    print("AVISO: Módulo 'LivroICMS' não encontrado. Usando simulação.")
    gerarLivroICMS = lambda **kwargs: generic_mock_book_generator(__module_name__='LivroICMS', **kwargs)
try: from LivroEntradas import gerarLivroEntrada
except ImportError:
    print("AVISO: Módulo 'LivroEntradas' não encontrado. Usando simulação.")
    gerarLivroEntrada = lambda **kwargs: generic_mock_book_generator(__module_name__='LivroEntradas', **kwargs)
try: from LivroSaidas import gerar_livro_saidas
except ImportError:
    print("AVISO: Módulo 'LivroSaidas' não encontrado. Usando simulação.")
    gerar_livro_saidas = lambda **kwargs: generic_mock_book_generator(__module_name__='LivroSaidas', **kwargs)
try: import pyodbc
except ImportError:
    print("AVISO: Módulo 'pyodbc' não encontrado. Usando simulação para o banco.")
    class DummyCursor:
        def execute(self, q): pass
        def fetchall(self): return [("101 - EMPRESA TESTE A", 101), ("2493 - GREEN V", 2493)] + [(f"EMPRESA {i} LTDA", i) for i in range(1, 21)]
        def __enter__(self): return self;
        def __exit__(self, a, b, c): pass
    class DummyConnection:
        def cursor(self): return DummyCursor()
        def __enter__(self): return self;
        def __exit__(self, a, b, c): pass
    class PyodbcMock:
        @staticmethod
        def connect(conn_str): return DummyConnection()
    pyodbc = PyodbcMock()
# --- FIM DO BLOCO DE SIMULAÇÃO ---

app = Flask(__name__)

# --- FUNÇÃO DE RENOMEAÇÃO DE ARQUIVOS ---
def get_new_filename(book_type, codigo_empresa, data_inicio, original_filename):
    """
    Gera o novo nome do arquivo conforme o padrão solicitado:
    {cod empresa} - {ano}.{mes_inicial} - {nome do livro}.{extensão}
    """
    # Extrair extensão do arquivo original
    extension = Path(original_filename).suffix.lower()  # .pdf ou .xlsx
    
    # Parse da data para extrair ano e mês
    data_obj = datetime.strptime(data_inicio, '%Y-%m-%d').date()
    ano = data_obj.year
    mes_inicial = f"{data_obj.month:02d}"  # Mês com 2 dígitos (01, 02, etc.)
    
    # Mapeamento dos tipos de livro para nomes padronizados
    livro_names = {
        'ipi': 'Livro Registro de Apuração do IPI',
        'iss': 'Livro de Serviços Prestados (ISS)', 
        'icms': 'Livro Registro de Apuração do ICMS',
        'entradas': 'Livro Registro de Entrada',
        'saidas': 'Livro Registro de Saída'
    }
    
    livro_nome = livro_names.get(book_type, f'Livro {book_type.upper()}')
    
    # Formato final: {cod empresa} - {ano}.{mes_inicial} - {nome do livro}
    new_name = f"{codigo_empresa} - {ano}.{mes_inicial} - {livro_nome}{extension}"
    
    return new_name

# --- SISTEMA DE MONITORAMENTO DO BANCO ---
class DatabaseMonitor:
    def __init__(self):
        self.status_lock = Lock()
        self.database_status = {
            'atualizacao_em_andamento': False,
            'ultima_atualizacao': None,
            'progresso': 'Conectando ao monitor...',
            'erro': None,
            'proximo_horario_backup': 'N/A',
            'connected': False,
            'last_update_time': None
        }
        self.monitor_thread = None
        self.should_stop = False
        # Não iniciar automaticamente no __init__ para não bloquear o Flask
    
    def start_monitoring(self):
        """Inicia o monitoramento em thread separada (não-bloqueante)"""
        if not WEBSOCKETS_AVAILABLE:
            print("⚠️ WebSocket não disponível - monitor desabilitado")
            self._update_status({
                'connected': False,
                'erro': 'Módulo websockets não instalado',
                'progresso': 'Monitor desabilitado'
            })
            return
        
        if self.monitor_thread and self.monitor_thread.is_alive():
            return
        
        print("🔍 Iniciando monitor do banco...")
        self.should_stop = False
        self.monitor_thread = threading.Thread(target=self._run_monitor, daemon=True)
        self.monitor_thread.start()
    
    def stop_monitoring(self):
        """Para o monitoramento"""
        self.should_stop = True
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2)
    
    def _run_monitor(self):
        """Executa o loop de monitoramento assíncrono de forma não-bloqueante"""
        if not WEBSOCKETS_AVAILABLE:
            return
            
        try:
            # Criar novo loop de eventos para esta thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Executar monitoramento
            loop.run_until_complete(self._monitor_websocket())
        except Exception as e:
            print(f"⚠️ Erro no monitor do banco: {e}")
            self._update_status({
                'connected': False, 
                'erro': f'Erro de inicialização: {str(e)}',
                'progresso': 'Monitor indisponível'
            })
        finally:
            # Limpar loop
            try:
                loop.close()
            except:
                pass
    
    async def _monitor_websocket(self):
        """Loop principal de monitoramento do WebSocket"""
        connection_attempts = 0
        max_attempts = 3
        
        while not self.should_stop:
            try:
                connection_attempts += 1
                print(f"🔌 Tentativa de conexão {connection_attempts}/{max_attempts}...")
                
                # Timeout mais curto para não travar
                async with websockets.connect(
                    WEBSOCKET_URI, 
                    ping_interval=20, 
                    ping_timeout=10,
                    open_timeout=5,  # Timeout de abertura
                    close_timeout=5   # Timeout de fechamento
                ) as websocket:
                    print("✅ Conectado ao monitor do banco")
                    connection_attempts = 0  # Reset contador
                    self._update_status({
                        'connected': True, 
                        'erro': None,
                        'progresso': 'Monitor conectado - aguardando dados...'
                    })
                    
                    # Loop de recebimento de mensagens
                    async for message in websocket:
                        if self.should_stop:
                            break
                        try:
                            data = json.loads(message)
                            self._process_websocket_message(data)
                        except json.JSONDecodeError:
                            print(f"⚠️ Mensagem WebSocket inválida: {message}")
                        except Exception as e:
                            print(f"⚠️ Erro ao processar mensagem: {e}")
            
            except (websockets.exceptions.ConnectionClosedError, ConnectionRefusedError) as e:
                print(f"❌ Conexão perdida: {e}")
                self._update_status({
                    'connected': False,
                    'progresso': f'Tentando reconectar... ({connection_attempts}/{max_attempts})'
                })
                
                if connection_attempts >= max_attempts:
                    print(f"❌ Máximo de tentativas atingido. Monitor será desativado.")
                    self._update_status({
                        'connected': False,
                        'erro': 'Monitor indisponível - máximo de tentativas atingido',
                        'progresso': 'Monitor desconectado'
                    })
                    break
                
                if not self.should_stop:
                    await asyncio.sleep(RECONNECT_DELAY)
                    
            except Exception as e:
                print(f"❌ Erro inesperado no monitor: {e}")
                self._update_status({
                    'connected': False, 
                    'erro': str(e),
                    'progresso': 'Erro no monitor'
                })
                
                if connection_attempts >= max_attempts:
                    print("❌ Monitor será desativado devido a erros persistentes")
                    break
                
                if not self.should_stop:
                    await asyncio.sleep(RECONNECT_DELAY)
    
    def _process_websocket_message(self, data):
        """Processa mensagens do WebSocket"""
        msg_type = data.get("type")
        msg_data = data.get("data", {})
        
        update = {'last_update_time': datetime.now().isoformat()}
        
        if msg_type == "status":
            # Estado completo recebido
            status = msg_data
            # Só está atualizando se explicitamente indicado
            is_updating = status.get('atualizacao_em_andamento', False)
            
            update.update({
                'atualizacao_em_andamento': is_updating,
                'progresso': status.get('progresso', 'Aguardando...'),
                'proximo_horario_backup': status.get('proximo_horario_backup', 'N/A'),
                'ultima_atualizacao': status.get('ultima_atualizacao'),
                'erro': status.get('erro')
            })
            
        elif msg_type == "progress":
            # Apenas "progress" indica uma atualização REALMENTE ativa que deve bloquear.
            update.update({
                'progresso': msg_data.get("message", "Processando..."),
                'atualizacao_em_andamento': True  # <-- BLOQUEIA
            })

        elif msg_type in ("checking", "waiting"):
            # "checking" e "waiting" indicam que o sistema está apenas monitorando, não atualizando.
            # Portanto, não devem bloquear a aplicação.
            update.update({
                'progresso': msg_data.get("message", "Aguardando..."),
                'atualizacao_em_andamento': False # <-- NÃO BLOQUEIA
            })
            
        elif msg_type == "completed":
            update.update({
                'atualizacao_em_andamento': False,
                'progresso': f"Backup aplicado: {msg_data.get('arquivo', 'N/A')}",
                'ultima_atualizacao': {
                    'data': datetime.now().strftime('%d/%m/%Y %H:%M'),
                    'arquivo': msg_data.get('arquivo'),
                    'baixado_em': datetime.now().isoformat()
                }
            })
            
        elif msg_type == "error":
            update.update({
                'atualizacao_em_andamento': False,
                'erro': msg_data.get("message", "Erro desconhecido"),
                'progresso': "Erro durante atualização"
            })
        
        self._update_status(update)
    
    def _update_status(self, updates):
        """Atualiza o status thread-safe"""
        with self.status_lock:
            self.database_status.update(updates)
    
    def get_status(self):
        """Retorna o status atual do banco"""
        with self.status_lock:
            return self.database_status.copy()
    
    def is_safe_to_process(self):
        """Verifica se é seguro processar (banco não está atualizando)"""
        status = self.get_status()
        return not status.get('atualizacao_em_andamento', True) and status.get('connected', False)

# Instância global do monitor (não inicializado ainda)
db_monitor = None

def init_database_monitor():
    """Inicializa o monitor do banco de forma não-bloqueante"""
    global db_monitor
    if db_monitor is None:
        db_monitor = DatabaseMonitor()
        
        if WEBSOCKETS_AVAILABLE:
            # Aguardar um pouco para Flask inicializar completamente
            def start_delayed():
                time.sleep(2)
                db_monitor.start_monitoring()
            
            monitor_init_thread = threading.Thread(target=start_delayed, daemon=True)
            monitor_init_thread.start()
            print("🔍 Monitor do banco será iniciado em breve...")
        else:
            print("⚠️ Monitor do banco desabilitado (websockets não disponível)")
    
    return db_monitor

def setup_dummy_environment():
    print("AVISO: Executando em MODO DE SIMULAÇÃO.")
    base_dir = Path("RAIZ_SIMULADA/Acesso Digital")
    base_dir.mkdir(parents=True, exist_ok=True)
    (base_dir / "101 - EMPRESA TESTE A").mkdir(exist_ok=True)
    (base_dir / "2493 - GREEN V").mkdir(exist_ok=True)
    print(f"Estrutura de pastas simulada criada em: '{base_dir.resolve()}'")
    return str(base_dir.parent.resolve())

if os.getenv('SIMULATE_ENV'):
    BASE_ACCESS_PATH = setup_dummy_environment()
else:
    print("INFO: Executando em MODO DE PRODUÇÃO. O caminho de destino será 'R:\\'")
    BASE_ACCESS_PATH = "R:\\"

CLIENT_FOLDER_BASE = Path(BASE_ACCESS_PATH) / "Acesso Digital"
CONN_STR = "DRIVER={SQL Anywhere 17};HOST=NOTE-GO-273.go.local:2638;DBN=contabil;UID=DP011;PWD=U0T/wq6OdZ0oYSpvJRWGfg==;"

# Sistema para múltiplos processamentos simultâneos
active_processings = {}  # Dicionário para armazenar múltiplos processamentos
processings_lock = Lock()  # Lock para thread safety

companies_cache = None
last_cache_update = None

def create_processing_id():
    """Cria um ID único para o processamento"""
    return str(uuid.uuid4())

def start_new_processing(processing_id):
    """Inicia um novo processamento"""
    with processings_lock:
        active_processings[processing_id] = {
            'progress': 0,
            'message': 'Preparando para iniciar...',
            'complete': False,
            'start_time': time.time(),
            'result': None
        }

def update_processing_status(processing_id, progress=None, message=None, complete=None, result=None):
    """Atualiza o status de um processamento específico"""
    with processings_lock:
        if processing_id in active_processings:
            if progress is not None:
                active_processings[processing_id]['progress'] = progress
            if message is not None:
                active_processings[processing_id]['message'] = message
            if complete is not None:
                active_processings[processing_id]['complete'] = complete
            if result is not None:
                active_processings[processing_id]['result'] = result

def get_processing_status(processing_id):
    """Obtém o status de um processamento específico"""
    with processings_lock:
        return active_processings.get(processing_id, {
            'progress': 0,
            'message': 'Processamento não encontrado',
            'complete': True,
            'error': 'ID inválido'
        })

def finish_processing(processing_id):
    """Finaliza um processamento e o remove da lista ativa após um tempo"""
    def cleanup():
        time.sleep(300)  # Mantém o resultado por 5 minutos
        with processings_lock:
            if processing_id in active_processings:
                del active_processings[processing_id]
                print(f"Processamento {processing_id[:8]} removido da memória após timeout")
    
    # Executa limpeza em thread separada
    cleanup_thread = threading.Thread(target=cleanup)
    cleanup_thread.daemon = True
    cleanup_thread.start()

def get_all_active_processings():
    """Retorna todos os processamentos ativos"""
    with processings_lock:
        return dict(active_processings)

def get_companies():
    global companies_cache, last_cache_update
    now = datetime.now()
    if companies_cache and last_cache_update and (now - last_cache_update).seconds < 1800:
        return companies_cache
    try:
        with pyodbc.connect(CONN_STR) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT apel_emp, codi_emp FROM bethadba.geempre ORDER BY apel_emp")
            companies = [{'code': int(r.codi_emp), 'name': str(r.apel_emp).strip()} for r in cursor.fetchall()]
            companies_cache, last_cache_update = companies, now
            return companies
    except Exception as e:
        print(f"Erro ao buscar empresas: {e}")
        return companies_cache or []

def find_client_folder(company_code, company_name):
    if not CLIENT_FOLDER_BASE.exists():
        raise FileNotFoundError(f"Diretório base não encontrado: {CLIENT_FOLDER_BASE}")
    prefix = f"{company_code} -"
    for item in CLIENT_FOLDER_BASE.iterdir():
        if item.is_dir() and item.name.startswith(prefix):
            return item
    return None

def get_destination_path(client_folder, book_type, competence_date, final_date):
    """
    Gera o caminho de destino usando sempre o mês da data FINAL,
    mas organizando em subpastas por mês de competência.
    
    Args:
        client_folder: Pasta da empresa
        book_type: Tipo do livro (iss, icms, etc)
        competence_date: Data de competência do livro específico
        final_date: Data final do período completo (para determinar pasta principal)
    """
    # MÊS FINAL para determinar a pasta principal
    final_year = str(final_date.year)
    final_month = f"{final_date.month:02d}"
    
    # MÊS DE COMPETÊNCIA para a subpasta
    comp_year = str(competence_date.year) 
    comp_month = f"{competence_date.month:02d}"
    
    # CAMINHO PRINCIPAL usando sempre o MÊS FINAL
    fiscal_path = client_folder / "01 - Fiscal" / final_year
    
    if book_type == 'iss':
        primary_path = fiscal_path / "01 - Apuração de ISS Mensal" / final_month  # ← MÊS FINAL
    else:
        primary_path = fiscal_path / "02 - ICMS, IPI, EFD-ICMS-IPI e GIA Mensal" / final_month  # ← MÊS FINAL
    
    # SUBPASTA com o mês de competência específico
    subpasta_competencia = f"{comp_month}-{comp_year}"
    final_primary_path = primary_path / "livros_gerados_pelo_robo" / subpasta_competencia
    
    # TENTAR CAMINHO PRINCIPAL PRIMEIRO
    try:
        final_primary_path.mkdir(parents=True, exist_ok=True)
        # Testar se consegue escrever na pasta
        test_file = final_primary_path / "test_write.tmp"
        test_file.touch()
        test_file.unlink()  # Remove o arquivo de teste
        print(f"✅ Usando caminho principal (mês final {final_month}): {final_primary_path}")
        return final_primary_path
    except (OSError, PermissionError, FileNotFoundError) as e:
        print(f"⚠️ Caminho principal falhou ({e}), tentando alternativo...")
    
    # CAMINHO ALTERNATIVO (também usando mês final)
    alternative_base = fiscal_path / "04 - Livros Fiscais"
    
    # Mapeamento dos tipos de livro para pastas específicas
    book_folders = {
        'icms': "01 - Livro Registro Apuração do ICMS",
        'ipi': "02 - Livro Registro Apuração do IPI", 
        'entradas': "03 - Livro Registro de Entrada",
        'saidas': "04 - Livro Registro de Saída",
        'iss': "05 - Livro Registro de Serviços Prestados"
    }
    
    book_folder_name = book_folders.get(book_type, f"99 - {book_type.upper()}")
    final_alternative_path = alternative_base / book_folder_name / final_month / subpasta_competencia  # ← MÊS FINAL
    
    try:
        final_alternative_path.mkdir(parents=True, exist_ok=True)
        # Testar se consegue escrever na pasta
        test_file = final_alternative_path / "test_write.tmp" 
        test_file.touch()
        test_file.unlink()  # Remove o arquivo de teste
        print(f"✅ Usando caminho alternativo (mês final {final_month}): {final_alternative_path}")
        return final_alternative_path
    except (OSError, PermissionError, FileNotFoundError) as e:
        # Se nem o alternativo funcionar, criar uma pasta local como último recurso
        print(f"❌ Caminho alternativo também falhou ({e}), usando pasta local...")
        local_path = Path("output_robo") / "emergency_output" / f"{client_folder.name}" / final_year / final_month / subpasta_competencia
        local_path.mkdir(parents=True, exist_ok=True)
        print(f"🆘 Usando caminho de emergência (mês final {final_month}): {local_path}")
        return local_path
def gerar_livros_multiplas_empresas(processing_id, empresas_selecionadas, data_inicio_str, data_fim_str, gerar_pdf, gerar_excel, livros_selecionados):
    try:
        # VERIFICAÇÃO CRÍTICA: Banco não pode estar atualizando
        global db_monitor
        if db_monitor is None or not db_monitor.is_safe_to_process():
            banco_status = db_monitor.get_status() if db_monitor else {'atualizacao_em_andamento': True, 'progresso': 'Monitor não inicializado'}
            erro_msg = "Processamento bloqueado: Banco em atualização"
            if banco_status.get('atualizacao_em_andamento'):
                erro_msg += f" - {banco_status.get('progresso', 'Atualizando...')}"
            
            update_processing_status(processing_id, progress=0, message=erro_msg, complete=True, 
                                   result={'success': False, 'message': erro_msg})
            finish_processing(processing_id)
            return
        
        update_processing_status(processing_id, progress=1, message='Analisando período solicitado...')
        
        # QUEBRAR PERÍODO EM MESES
        periodos_mensais = quebrar_periodo_em_meses(data_inicio_str, data_fim_str)
        
        # DATA FINAL DO PERÍODO COMPLETO (para determinar pasta de destino)
        data_final_periodo = datetime.strptime(data_fim_str, '%Y-%m-%d').date()
        
        # Log da quebra para debugging
        if len(periodos_mensais) > 1:
            periodo_info = ", ".join([f"{p[0][:7]}" for p in periodos_mensais])
            print(f"[{processing_id[:8]}] Período quebrado em {len(periodos_mensais)} meses: {periodo_info}")
            print(f"[{processing_id[:8]}] Todos os livros serão salvos na pasta do mês final: {data_final_periodo.strftime('%m-%Y')}")
            update_processing_status(processing_id, progress=2, 
                                   message=f'Período quebrado em {len(periodos_mensais)} meses: {periodo_info} (pasta: {data_final_periodo.strftime("%m-%Y")})')
        else:
            print(f"[{processing_id[:8]}] Período único: {periodos_mensais[0][0]} até {periodos_mensais[0][1]}")
            update_processing_status(processing_id, progress=2, message='Período único identificado')
        
        livros_map = {
            'ipi': {"nome": "Livro de IPI", "funcao": gerarLivroDeIpi, "params": {"gerar_pdf": gerar_pdf, "gerar_xlsx": gerar_excel}},
            'iss': {"nome": "Livro de ISS", "funcao": gerar_livro_iss, "params": {"exportar_pdf": gerar_pdf, "exportar_xlsx": gerar_excel}},
            'icms': {"nome": "Livro de ICMS", "funcao": gerarLivroICMS, "params": {"gerar_pdf": gerar_pdf, "gerar_xlsx": gerar_excel}},
            'entradas': {"nome": "Livro de Entradas", "funcao": gerarLivroEntrada, "params": {"gerar_pdf": gerar_pdf, "gerar_xlsx": gerar_excel}},
            'saidas': {"nome": "Livro de Saídas", "funcao": gerar_livro_saidas, "params": {"gerar_pdf": gerar_pdf, "gerar_xlsx": gerar_excel}},
        }
        
        livros_a_processar = [(k, v) for k, v in livros_map.items() if livros_selecionados.get(k)]
        
        # CÁLCULO TOTAL DE OPERAÇÕES (empresas x períodos x livros)
        total_operacoes = len(empresas_selecionadas) * len(periodos_mensais) * len(livros_a_processar)
        
        resultado_geral = {
            'empresas_processadas': [],
            'empresas_com_erro': [],
            'total_livros_gerados': 0,
            'total_livros_com_erro': 0,
            'total_periodos': len(periodos_mensais),
            'periodos_processados': [],
            'pasta_final': data_final_periodo.strftime('%m-%Y')
        }
        
        operacao_atual = 0
        
        for empresa in empresas_selecionadas:
            # Verificação contínua durante o processamento
            if db_monitor and not db_monitor.is_safe_to_process():
                erro_msg = "Processamento interrompido: Banco iniciou atualização"
                update_processing_status(processing_id, progress=50, message=erro_msg, complete=True,
                                       result={'success': False, 'message': erro_msg})
                finish_processing(processing_id)
                return
            
            codigo_empresa = empresa['code']
            nome_empresa = empresa['name']
            
            try:
                client_folder = find_client_folder(codigo_empresa, nome_empresa)
                if not client_folder:
                    raise FileNotFoundError(f"Pasta para a empresa {codigo_empresa} - {nome_empresa} não encontrada.")
                
                livros_empresa = []
                erros_empresa = []
                periodos_empresa = []
                
                # PROCESSAR CADA PERÍODO MENSAL
                for periodo_inicio, periodo_fim in periodos_mensais:
                    mes_referencia = periodo_inicio[:7]  # YYYY-MM
                    
                    update_processing_status(
                        processing_id,
                        progress=max(5, int((operacao_atual / total_operacoes) * 100)),
                        message=f'Processando {nome_empresa} - {mes_referencia} (pasta: {data_final_periodo.strftime("%m-%Y")})...'
                    )
                    
                    livros_periodo = []
                    erros_periodo = []
                    
                    # PROCESSAR CADA LIVRO PARA ESTE PERÍODO
                    for key, livro in livros_a_processar:
                        try:
                            update_processing_status(
                                processing_id,
                                progress=int((operacao_atual / total_operacoes) * 100),
                                message=f'Gerando {livro["nome"]} para {nome_empresa} ({mes_referencia}) → pasta {data_final_periodo.strftime("%m-%Y")}...'
                            )
                            
                            params = {
                                "codi_emp": codigo_empresa, 
                                "data_inicio": periodo_inicio, 
                                "data_fim": periodo_fim, 
                                **livro["params"]
                            }
                            generated_files = livro['funcao'](**params)
                            
                            if not isinstance(generated_files, list):
                                raise TypeError("A função de geração não retornou uma lista de arquivos.")
                            
                            update_processing_status(
                                processing_id,
                                message=f'Movendo {livro["nome"]} para {nome_empresa} ({mes_referencia}) → pasta {data_final_periodo.strftime("%m-%Y")}...'
                            )
                            
                            # USAR DATA DE COMPETÊNCIA ESPECÍFICA + DATA FINAL DO PERÍODO COMPLETO
                            data_competencia = datetime.strptime(periodo_inicio, '%Y-%m-%d').date()
                            dest_path = get_destination_path(client_folder, key, data_competencia, data_final_periodo)
                            
                            arquivos_movidos = 0
                            for file_path_str in generated_files:
                                if not (file_path_str.lower().endswith('.pdf') or file_path_str.lower().endswith('.xlsx')):
                                    continue

                                source_file = Path(file_path_str)
                                if source_file.exists():
                                    # Gerar novo nome do arquivo conforme padrão solicitado
                                    new_filename = get_new_filename(key, codigo_empresa, periodo_inicio, source_file.name)
                                    destination_file = dest_path / new_filename
                                    
                                    # SUBSTITUIR se já existir (mkdir com exist_ok=True já permite isso)
                                    if destination_file.exists():
                                        print(f"[{processing_id[:8]}] Substituindo arquivo existente: {new_filename}")
                                    
                                    # Mover arquivo com o novo nome
                                    shutil.move(str(source_file), str(destination_file))
                                    print(f"[{processing_id[:8]}] Movido: {source_file.name} → {new_filename} (pasta final: {data_final_periodo.strftime('%m-%Y')}, subpasta: {dest_path.name})")
                                    arquivos_movidos += 1
                                else:
                                    raise FileNotFoundError(f"Arquivo gerado não encontrado: {source_file}")
                            
                            if arquivos_movidos > 0:
                                livros_periodo.append(livro['nome'])
                                resultado_geral['total_livros_gerados'] += 1
                            else:
                                erro_msg = f"{livro['nome']}: Nenhum arquivo foi gerado"
                                erros_periodo.append(erro_msg)
                                resultado_geral['total_livros_com_erro'] += 1
                            
                        except Exception as e:
                            erro_msg = f"{livro['nome']}: {type(e).__name__}: {e}"
                            erros_periodo.append(erro_msg)
                            resultado_geral['total_livros_com_erro'] += 1
                            print(f"[{processing_id[:8]}] Erro ao processar {livro['nome']} para {nome_empresa} ({mes_referencia}): {e}")
                        
                        operacao_atual += 1
                    
                    # Adicionar período aos resultados da empresa
                    periodos_empresa.append({
                        'periodo': f"{mes_referencia} ({periodo_inicio} até {periodo_fim})",
                        'livros_gerados': livros_periodo,
                        'erros': erros_periodo
                    })
                    
                    # Consolidar livros da empresa (todos os períodos)
                    livros_empresa.extend(livros_periodo)
                    erros_empresa.extend(erros_periodo)
                
                resultado_geral['empresas_processadas'].append({
                    'empresa': f"{nome_empresa} ({codigo_empresa})",
                    'livros_gerados': list(set(livros_empresa)),  # Remove duplicatas
                    'erros': erros_empresa,
                    'periodos_detalhados': periodos_empresa,
                    'total_periodos': len(periodos_mensais),
                    'pasta_final': data_final_periodo.strftime('%m-%Y')
                })
                
            except Exception as e:
                erro_empresa = f"Erro crítico para {nome_empresa} ({codigo_empresa}): {e}"
                resultado_geral['empresas_com_erro'].append(erro_empresa)
                # Pular todas as operações restantes desta empresa
                operacao_atual += len(periodos_mensais) * len(livros_a_processar)
        
        # Resultado final
        total_empresas = len(empresas_selecionadas)
        empresas_sucesso = len(resultado_geral['empresas_processadas'])
        empresas_erro = len(resultado_geral['empresas_com_erro'])
        
        detalhes_resultado = []
        if resultado_geral['total_livros_gerados'] > 0:
            detalhes_resultado.append(f"{resultado_geral['total_livros_gerados']} livros gerados")
        if resultado_geral['total_livros_com_erro'] > 0:
            detalhes_resultado.append(f"{resultado_geral['total_livros_com_erro']} falhas")
        if len(periodos_mensais) > 1:
            detalhes_resultado.append(f"{len(periodos_mensais)} períodos mensais")
            detalhes_resultado.append(f"pasta final: {data_final_periodo.strftime('%m-%Y')}")
        
        # Mensagem final personalizada
        if len(periodos_mensais) > 1:
            periodo_range = f"período de {periodos_mensais[0][0][:7]} a {periodos_mensais[-1][0][:7]}"
        else:
            periodo_range = f"período de {periodos_mensais[0][0][:7]}"
        
        resultado_final = {
            'success': resultado_geral['total_livros_com_erro'] == 0 and empresas_erro == 0,
            'message': f"Processamento concluído para {periodo_range}: {empresas_sucesso}/{total_empresas} empresas processadas, {', '.join(detalhes_resultado)}.",
            'empresas_processadas': resultado_geral['empresas_processadas'],
            'empresas_com_erro': resultado_geral['empresas_com_erro'],
            'total_empresas': total_empresas,
            'empresas_sucesso': empresas_sucesso,
            'empresas_erro': empresas_erro,
            'total_livros_gerados': resultado_geral['total_livros_gerados'],
            'total_livros_com_erro': resultado_geral['total_livros_com_erro'],
            'total_periodos': len(periodos_mensais),
            'periodos_processados': [f"{p[0][:7]}" for p in periodos_mensais],
            'detalhamento_periodos': len(periodos_mensais) > 1,
            'pasta_final': data_final_periodo.strftime('%m-%Y')
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        resultado_final = {
            'success': False,
            'message': f'Erro crítico na execução: {e}',
            'empresas_com_erro': [str(e)],
            'total_empresas': len(empresas_selecionadas) if 'empresas_selecionadas' in locals() else 0,
            'empresas_sucesso': 0,
            'empresas_erro': 1,
            'total_livros_gerados': 0,
            'total_livros_com_erro': 1
        }
    finally:
        update_processing_status(
            processing_id,
            progress=100,
            message='Concluído!',
            complete=True,
            result=resultado_final
        )
        finish_processing(processing_id)
        print(f"Processamento {processing_id[:8]} finalizado")

@app.route('/')
def index():
    return render_template_string('''
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sistema de Automação - Livros Fiscais</title>
    <link href="https://fonts.googleapis.com/css2?family=Roboto:wght@300;400;500;700&display=swap" rel="stylesheet">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
    <style>
        :root {
            --primary: #1e40af; --primary-light: #3b82f6; --primary-dark: #1e3a8a;
            --secondary: #64748b; --success: #059669; --error: #dc2626; --warning: #d97706; --info: #0ea5e9;
            --gray-50: #f8fafc; --gray-100: #f1f5f9; --gray-200: #e2e8f0; --gray-300: #cbd5e1;
            --gray-400: #94a3b8; --gray-500: #64748b; --gray-600: #475569; --gray-700: #334155;
            --gray-800: #1e293b; --gray-900: #0f172a; --white: #ffffff;
            --shadow: 0 1px 3px 0 rgb(0 0 0 / 0.1), 0 1px 2px -1px rgb(0 0 0 / 0.1);
            --shadow-md: 0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1);
            --shadow-lg: 0 10px 15px -3px rgb(0 0 0 / 0.1), 0 4px 6px -4px rgb(0 0 0 / 0.1);
            --shadow-xl: 0 20px 25px -5px rgb(0 0 0 / 0.1), 0 8px 10px -6px rgb(0 0 0 / 0.1);
        }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Roboto', sans-serif; background-color: var(--gray-50); color: var(--gray-900); line-height: 1.0; min-height: 100vh; }
        .header { background: linear-gradient(135deg, var(--primary) 0%, var(--primary-dark) 100%); color: var(--white); padding: 0.5rem 1rem; box-shadow: var(--shadow-lg); display: flex; align-items: center; gap: 1rem; transition: all 0.3s ease; }
        .header:hover { box-shadow: var(--shadow-xl); }
        .header-logo { height: 40px; object-fit: contain; transition: transform 0.3s ease; }
        .header-logo:hover { transform: scale(1.05); }
        .header h1 { font-size: 1.5rem; font-weight: 700; letter-spacing: -0.025em; }
        .container { max-width: 1000px; margin: 1.5rem auto; padding: 0 1rem; }
        
        /* STATUS DO BANCO */
        .database-status {
            background: var(--white);
            border-radius: 12px;
            padding: 1rem;
            margin-bottom: 1.5rem;
            box-shadow: var(--shadow);
            border-left: 4px solid var(--success);
            transition: all 0.3s ease;
        }
        .database-status.blocked {
            border-left-color: var(--warning);
            background: linear-gradient(135deg, rgba(217, 119, 6, 0.05) 0%, rgba(255, 255, 255, 1) 100%);
        }
        .status-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
        }
        .status-main {
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }
        .status-icon {
            width: 32px;
            height: 32px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 0.875rem;
            color: var(--white);
            animation: pulse 2s infinite;
        }
        .status-icon.safe { background: var(--success); animation: none; }
        .status-icon.blocked { background: var(--warning); }
        @keyframes pulse {
            0%, 100% { transform: scale(1); }
            50% { transform: scale(1.1); }
        }
        .status-info {
            flex: 1;
        }
        .status-title {
            font-weight: 600;
            color: var(--gray-800);
            font-size: 0.9rem;
            margin-bottom: 0.25rem;
        }
        .status-message {
            font-size: 0.8rem;
            color: var(--gray-600);
        }
        .last-backup {
            background: var(--gray-50);
            border-radius: 8px;
            padding: 0.5rem 0.75rem;
            font-size: 0.8rem;
            display: flex;
            align-items: center;
            gap: 0.5rem;
            white-space: nowrap;
        }
        .last-backup-title {
            font-weight: 500;
            color: var(--gray-700);
        }
        .backup-datetime {
            color: var(--gray-800);
            font-weight: 500;
        }
        
        .card { background: var(--white); border-radius: 12px; padding: 1.5rem; box-shadow: var(--shadow); border: 1px solid var(--gray-200); transition: all 0.3s ease; }
        .card:hover { box-shadow: var(--shadow-lg); transform: translateY(-2px); }
        .card-header { display: flex; align-items: center; gap: 0.75rem; margin-bottom: 1rem; padding-bottom: 0.75rem; border-bottom: 1px solid var(--gray-200); }
        .card-header .icon { width: 40px; height: 40px; background: var(--primary); color: var(--white); border-radius: 8px; display: flex; align-items: center; justify-content: center; font-size: 1.125rem; flex-shrink: 0; transition: all 0.3s ease; }
        .card-header .icon:hover { background: var(--primary-dark); transform: scale(1.1) rotate(5deg); }
        .card-header h2 { font-size: 1.25rem; font-weight: 600; color: var(--gray-900); }
        .companies-count { background: var(--primary-light); color: var(--white); padding: 0.2rem 0.5rem; border-radius: 12px; font-size: 0.7rem; font-weight: 600; margin-left: auto; transition: all 0.3s ease; }
        .companies-count:hover { background: var(--primary-dark); transform: scale(1.05); }
        .form-section { margin-bottom: 1.5rem; }
        .form-section:last-child { margin-bottom: 0; }
        .section-title { font-size: 1rem; font-weight: 600; color: var(--gray-800); margin-bottom: 0.75rem; display: flex; align-items: center; gap: 0.5rem; transition: color 0.3s ease; }
        .section-title:hover { color: var(--primary); }
        .section-title i { color: var(--primary); transition: transform 0.3s ease; }
        .section-title:hover i { transform: scale(1.2); }
        
        /* Layout Grid */
        .basic-info-grid { 
            display: grid; 
            grid-template-columns: 1fr 1fr; 
            gap: 1.5rem;
            overflow: visible;
        }
        
        .company-section, .date-section {
            display: flex;
            flex-direction: column;
            min-height: 120px;
            overflow: visible;
        }
        
        .main-label {
            display: flex;
            align-items: center;
            margin-bottom: 0.75rem;
            color: var(--gray-700);
            font-weight: 500;
            font-size: 0.875rem;
            height: 20px;
            flex-shrink: 0;
        }
        .main-label i { width: 16px; text-align: center; margin-right: 8px; color: var(--gray-500); transition: color 0.3s ease; }
        .main-label:hover i { color: var(--primary); }
        
        .company-search-container { 
            position: relative;
            display: flex;
            flex-direction: column;
            flex: 1;
        }
        
        .company-search-input {
            width: 100%;
            padding: 0.65rem 3rem 0.65rem 1rem;
            border: 2px solid var(--gray-200);
            border-radius: 8px;
            font-size: 0.875rem;
            transition: all 0.3s ease;
            background: linear-gradient(135deg, 
                rgba(255, 255, 255, 0.9) 0%, 
                rgba(248, 250, 252, 0.9) 50%, 
                rgba(241, 245, 249, 0.9) 100%);
            font-family: 'Roboto', sans-serif;
            color: var(--gray-800);
            backdrop-filter: blur(10px);
            height: 48px;
            box-sizing: border-box;
        }
        .company-search-input:focus { outline: none; border-color: var(--primary); box-shadow: 0 0 0 3px rgb(59 130 246 / 0.1); transform: scale(1.02); }
        .company-search-input:hover:not(:focus) { border-color: var(--gray-300); }
        .company-search-input:disabled {
            background: var(--gray-100);
            color: var(--gray-500);
            cursor: not-allowed;
        }
        
        /* Date Container Adjustments */
        .date-container {
            display: flex;
            flex-direction: column;
            flex: 1;
            overflow: visible;
        }
        .date-grid { 
            display: grid; 
            grid-template-columns: 1fr 1fr; 
            gap: 1rem; 
            overflow: visible;
        }
        .date-item { 
            display: flex; 
            flex-direction: column; 
            position: relative;
            overflow: visible;
        }
        .date-sub-label { 
            margin-bottom: 0.37rem; 
            font-size: 0.8rem; 
            color: var(--gray-600); 
            font-weight: 500;
            height: 1.2rem;
            display: flex;
            align-items: center;
            transition: all 0.3s ease;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            position: relative;
        }
        .date-sub-label:hover { 
            color: var(--primary); 
            transform: translateY(-1px);
        }
        
        .date-sub-label::before {
            content: '';
            position: absolute;
            bottom: -2px;
            left: 0;
            width: 0;
            height: 2px;
            background: linear-gradient(90deg, var(--primary) 0%, var(--primary-light) 100%);
            transition: width 0.3s ease;
            border-radius: 1px;
        }
        
        .date-sub-label:hover::before {
            width: 100%;
        }
        
        /* Custom Date Input Styling - Simplified for custom calendar */
        input[type="date"] { 
            width: 100%; 
            padding: 0.65rem 3rem 0.65rem 1rem; 
            border: 2px solid var(--gray-200); 
            border-radius: 8px; 
            font-size: 0.875rem; 
            transition: all 0.3s ease;
            background: linear-gradient(135deg, 
                rgba(255, 255, 255, 0.9) 0%, 
                rgba(248, 250, 252, 0.9) 50%, 
                rgba(241, 245, 249, 0.9) 100%);
            position: relative;
            font-family: 'Roboto', sans-serif;
            color: var(--gray-800);
            cursor: pointer;
            backdrop-filter: blur(10px);
            height: 48px; /* Altura fixa */
            box-sizing: border-box;
        }
        input[type="date"]:focus { 
            outline: none; 
            border-color: var(--primary); 
            box-shadow: 0 0 0 3px rgb(59 130 246 / 0.1); 
            transform: scale(1.02);
            background: rgba(255, 255, 255, 1);
        }
        input[type="date"]:hover:not(:focus) { 
            border-color: rgba(59, 130, 246, 0.3); 
            transform: translateY(-1px);
            box-shadow: var(--shadow-md);
            background: linear-gradient(135deg, 
                rgba(255, 255, 255, 1) 0%, 
                rgba(248, 250, 252, 1) 50%, 
                rgba(241, 245, 249, 1) 100%);
        }
        input[type="date"]:disabled {
            background: var(--gray-100);
            color: var(--gray-500);
            cursor: not-allowed;
            border-color: var(--gray-300);
        }
        
        /* Additional hover effects for the date container */
        .date-item:hover input[type="date"]:not(:disabled) {
            border-color: var(--primary-light);
            box-shadow: 0 4px 12px rgba(59, 130, 246, 0.15);
        }
        
        /* CALENDÁRIO PERSONALIZADO */
        .custom-calendar {
            position: absolute;
            top: calc(100% + 0.25rem);
            left: 0;
            background: var(--white);
            border: 1px solid var(--gray-300);
            border-radius: 6px;
            box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
            z-index: 2000;
            display: none;
            padding: 0.6rem;
            width: 240px;
            font-size: 0.8rem;
        }
        
        .custom-calendar.show {
            display: block;
        }
        
        .calendar-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 0.6rem;
            padding-bottom: 0.4rem;
            border-bottom: 1px solid var(--gray-200);
        }
        
        .calendar-nav-btn {
            background: var(--gray-100);
            border: none;
            border-radius: 4px;
            width: 24px;
            height: 24px;
            display: flex;
            align-items: center;
            justify-content: center;
            cursor: pointer;
            transition: all 0.15s ease;
            color: var(--gray-600);
            font-size: 0.9rem;
            font-weight: bold;
        }
        
        .calendar-nav-btn:hover {
            background: var(--primary);
            color: var(--white);
        }
        
        .calendar-month-year {
            font-weight: 600;
            color: var(--gray-800);
            font-size: 0.85rem;
            letter-spacing: 0.025em;
        }
        
        .calendar-grid {
            display: grid;
            grid-template-columns: repeat(7, 1fr);
            gap: 0.1rem;
        }
        
        .calendar-day-header {
            padding: 0.3rem 0.1rem;
            text-align: center;
            font-size: 0.6rem;
            font-weight: 600;
            color: var(--gray-500);
            text-transform: uppercase;
        }
        
        .calendar-day {
            aspect-ratio: 1;
            display: flex;
            align-items: center;
            justify-content: center;
            border-radius: 4px;
            cursor: pointer;
            transition: all 0.15s ease;
            font-size: 0.75rem;
            font-weight: 500;
            position: relative;
            min-height: 28px;
        }
        
        .calendar-day:not(.other-month):hover {
            background: var(--primary-light);
            color: var(--white);
        }
        
        .calendar-day.selected {
            background: var(--primary);
            color: var(--white);
            box-shadow: 0 1px 4px rgba(59, 130, 246, 0.3);
        }
        
        .calendar-day.today {
            background: var(--gray-100);
            color: var(--primary);
            font-weight: 700;
        }
        
        .calendar-day.today:not(.selected):hover {
            background: var(--primary);
            color: var(--white);
        }
        
        .calendar-day.other-month {
            color: var(--gray-300);
            cursor: default;
        }
        
        .calendar-actions {
            display: flex;
            justify-content: space-between;
            margin-top: 0.6rem;
            padding-top: 0.4rem;
            border-top: 1px solid var(--gray-200);
            gap: 0.4rem;
        }
        
        .calendar-btn {
            padding: 0.3rem 0.6rem;
            border: none;
            border-radius: 4px;
            font-size: 0.7rem;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.15s ease;
            flex: 1;
        }
        
        .calendar-btn.clear {
            background: var(--gray-100);
            color: var(--gray-600);
        }
        
        .calendar-btn.clear:hover {
            background: var(--gray-200);
        }
        
        .calendar-btn.today {
            background: var(--primary);
            color: var(--white);
        }
        
        .calendar-btn.today:hover {
            background: var(--primary-dark);
        }
        
        /* Hide native date picker completely */
        .date-item.custom-date input[type="date"]::-webkit-calendar-picker-indicator {
            position: absolute;
            left: 0;
            top: 0;
            width: 100%;
            height: 100%;
            background: transparent;
            cursor: pointer;
            opacity: 0;
        }
        
        .date-item.custom-date input[type="date"]::-webkit-inner-spin-button,
        .date-item.custom-date input[type="date"]::-webkit-clear-button {
            display: none;
        }
        
        .date-item.custom-date input[type="date"]::-webkit-datetime-edit-fields-wrapper {
            pointer-events: none;
        }
        
        /* Add custom calendar icon */
        .date-item.custom-date {
            position: relative;
        }
        
        .date-item.custom-date::after {
            content: '';
            position: absolute;
            right: 0.75rem;
            top: 66%;
            transform: translateY(-50%);
            width: 20px;
            height: 20px;
            background-image: url("data:image/svg+xml,%3csvg xmlns='http://www.w3.org/2000/svg' fill='none' viewBox='0 0 24 24' stroke='%233b82f6' stroke-width='2'%3e%3cpath stroke-linecap='round' stroke-linejoin='round' d='M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z'/%3e%3c/svg%3e");
            background-size: 20px;
            background-repeat: no-repeat;
            background-position: center;
            pointer-events: none;
            transition: all 0.3s ease;
            z-index: 3;
        }
        
        .date-item.custom-date:hover::after {
            transform: translateY(-50%) scale(1.1);
            filter: brightness(1.2);
        }
        
        /* Date field styling */
        input[type="date"]::-webkit-datetime-edit {
            color: var(--gray-800);
            font-weight: 500;
            letter-spacing: 0.025em;
        }
        
        input[type="date"]::-webkit-datetime-edit-fields-wrapper {
            padding: 0;
            display: flex;
            align-items: center;
        }
        
        input[type="date"]::-webkit-datetime-edit-text {
            color: var(--gray-400);
            padding: 0 0.25rem;
            font-weight: 400;
        }
        
        input[type="date"]::-webkit-datetime-edit-month-field,
        input[type="date"]::-webkit-datetime-edit-day-field,
        input[type="date"]::-webkit-datetime-edit-year-field {
            padding: 0.25rem 0.35rem;
            border-radius: 4px;
            transition: all 0.2s ease;
            color: var(--gray-800);
            font-weight: 500;
            min-width: 0;
        }
        
        input[type="date"]::-webkit-datetime-edit-month-field:hover,
        input[type="date"]::-webkit-datetime-edit-day-field:hover,
        input[type="date"]::-webkit-datetime-edit-year-field:hover {
            background: linear-gradient(135deg, var(--primary-light) 0%, var(--primary) 100%);
            color: var(--white);
            transform: scale(1.05);
        }
        
        input[type="date"]::-webkit-datetime-edit-month-field:focus,
        input[type="date"]::-webkit-datetime-edit-day-field:focus,
        input[type="date"]::-webkit-datetime-edit-year-field:focus {
            background: var(--primary);
            color: var(--white);
            outline: none;
            box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.3);
        }
        
        /* Enhanced focus state for entire date input */
        input[type="date"]:focus::-webkit-datetime-edit {
            color: var(--primary-dark);
        }
        
        input[type="date"]:focus::-webkit-datetime-edit-text {
            color: var(--primary);
        }
        
        /* Smooth transitions for all elements */
        input[type="date"] * {
            transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
        }
        
        /* Modern Calendar Container Enhancement */
        .date-item::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: linear-gradient(135deg, transparent 0%, rgba(59, 130, 246, 0.03) 100%);
            border-radius: 8px;
            opacity: 0;
            transition: opacity 0.3s ease;
            pointer-events: none;
            z-index: 1;
        }
        
        .date-item:hover::before {
            opacity: 1;
        }
        
        .date-item input[type="date"] {
            position: relative;
            z-index: 2;
        }
        
        /* Pulse effect on focus */
        input[type="date"]:focus {
            animation: dateInputPulse 0.6s ease-out;
        }
        
        @keyframes dateInputPulse {
            0% {
                box-shadow: 0 0 0 0 rgba(59, 130, 246, 0.4);
            }
            70% {
                box-shadow: 0 0 0 10px rgba(59, 130, 246, 0);
            }
            100% {
                box-shadow: 0 0 0 0 rgba(59, 130, 246, 0);
            }
        }
        
        /* Calendar icon animation */
        @keyframes calendarBounce {
            0%, 20%, 50%, 80%, 100% {
                transform: translateY(-50%);
            }
            40% {
                transform: translateY(-50%) translateY(-3px);
            }
            60% {
                transform: translateY(-50%) translateY(-1px);
            }
        }
        
        @keyframes successFill {
            0% {
                transform: scale(1);
                border-color: var(--gray-200);
                box-shadow: 0 0 0 0 rgba(5, 150, 105, 0);
            }
            50% {
                transform: scale(1.05);
                border-color: var(--success);
                box-shadow: 0 0 0 8px rgba(5, 150, 105, 0.2);
            }
            100% {
                transform: scale(1);
                border-color: var(--success);
                box-shadow: 0 0 0 0 rgba(5, 150, 105, 0);
            }
        }
        
        /* Class for auto-filled animation */
        .date-auto-filled {
            animation: successFill 0.8s cubic-bezier(0.4, 0, 0.2, 1);
        }
        
        /* Calendar hover effects - Final simplified version */
        .calendar-day:not(.other-month):not(.selected):hover {
            background: var(--primary-light);
        }
        
        .calendar-day.selected:hover {
            background: var(--primary-dark);
        }
        
        /* Simple today indicator */
        .calendar-day.today:not(.selected)::after {
            content: '';
            position: absolute;
            bottom: 3px;
            left: 50%;
            transform: translateX(-50%);
            width: 3px;
            height: 3px;
            background: var(--primary);
            border-radius: 50%;
        }
        
        input[type="date"]:focus::-webkit-calendar-picker-indicator {
            animation: calendarBounce 0.8s ease-in-out;
        }
        
        .company-dropdown { position: absolute; top: 100%; left: 0; right: 0; background: var(--white); border: 2px solid var(--gray-200); border-top: none; border-radius: 0 0 8px 8px; max-height: 200px; overflow-y: auto; z-index: 1000; display: none; box-shadow: var(--shadow-md); }
        .company-dropdown.show { display: block; animation: slideDown 0.3s ease; }
        @keyframes slideDown {
            from { opacity: 0; transform: translateY(-10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        .company-item { padding: 0.75rem 1rem; cursor: pointer; border-bottom: 1px solid var(--gray-100); transition: all 0.3s ease; }
        .company-item:hover, .company-item.highlighted { background: var(--primary-light); color: var(--white); }
        .company-item .company-name { font-weight: 500; font-size: 0.875rem; }
        .company-item .company-code { font-size: 0.75rem; color: var(--gray-500); transition: color 0.3s ease; }
        .company-item:hover .company-code, .company-item.highlighted .company-code { color: rgba(255, 255, 255, 0.8); }
        
        .selected-companies {
            display: flex;
            flex-wrap: wrap;
            gap: 0.5rem;
            transition: all 0.3s ease;
        }
        .selected-companies:not(:empty) {
            margin-top: 0.5rem;
        }
        .company-card {
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
            background: linear-gradient(135deg, var(--primary-light) 0%, var(--primary) 100%);
            color: var(--white);
            padding: 0.4rem 0.75rem;
            border-radius: 20px;
            font-size: 0.8rem;
            font-weight: 500;
            animation: slideIn 0.3s ease;
            transition: all 0.3s ease;
            box-shadow: var(--shadow);
        }
        .company-card:hover {
            transform: translateY(-2px) scale(1.05);
            box-shadow: var(--shadow-lg);
            background: linear-gradient(135deg, var(--primary) 0%, var(--primary-dark) 100%);
        }
        .company-card .remove-btn {
            background: rgba(255, 255, 255, 0.2);
            border: none;
            color: var(--white);
            width: 18px;
            height: 18px;
            border-radius: 50%;
            cursor: pointer;
            font-size: 0.7rem;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: all 0.3s ease;
        }
        .company-card .remove-btn:hover {
            background: rgba(255, 255, 255, 0.4);
            transform: scale(1.2) rotate(90deg);
        }
        @keyframes slideIn {
            from { opacity: 0; transform: scale(0.8) translateY(10px); }
            to { opacity: 1; transform: scale(1) translateY(0); }
        }
        
        .livros-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 1rem; }
        .livros-item { 
            display: flex; 
            align-items: center; 
            justify-content: center; 
            padding: 1rem; 
            border-radius: 8px; 
            cursor: pointer; 
            transition: all 0.3s ease; 
            border: 2px solid var(--gray-300); 
            background: linear-gradient(135deg, var(--white) 0%, var(--gray-50) 100%);
            position: relative;
            overflow: hidden;
            margin: 0.25rem;
        }
        .livros-item::before {
            content: '';
            position: absolute;
            top: 0;
            left: -100%;
            width: 100%;
            height: 100%;
            background: linear-gradient(90deg, transparent, rgba(59, 130, 246, 0.1), transparent);
            transition: left 0.5s ease;
        }
        .livros-item:hover::before { left: 100%; }
        .livros-item:not(.disabled):hover { 
            background: var(--gray-50); 
            border-color: var(--gray-400); 
            transform: translateY(-3px) scale(1.02);
            box-shadow: var(--shadow-md);
        }
        .livros-item.checked { 
            border: 3px solid var(--primary); 
            background: linear-gradient(135deg, rgb(59 130 246 / 0.1) 0%, rgb(59 130 246 / 0.05) 100%);
            transform: scale(1.05);
            box-shadow: var(--shadow-md);
        }
        .livros-item.checked:not(.disabled):hover {
            transform: translateY(-3px) scale(1.07);
            box-shadow: var(--shadow-lg);
        }
        .livros-item.checked .livros-label { color: var(--primary); font-weight: 600; }
        .livros-item.disabled {
            opacity: 0.5;
            cursor: not-allowed;
            background: var(--gray-100);
        }
        .livros-label { font-weight: 500; color: var(--gray-700); font-size: 0.875rem; user-select: none; transition: all 0.3s ease; position: relative; z-index: 1; }
        
        .format-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 2rem; }
        .format-item { 
            display: flex; 
            align-items: center; 
            justify-content: center; 
            gap: 0.75rem; 
            padding: 1rem; 
            border-radius: 8px; 
            cursor: pointer; 
            transition: all 0.3s ease; 
            border: 2px solid var(--gray-300); 
            background: linear-gradient(135deg, var(--white) 0%, var(--gray-50) 100%);
            position: relative;
            overflow: hidden;
            margin: 0.25rem;
        }
        .format-item::before {
            content: '';
            position: absolute;
            top: 0;
            left: -100%;
            width: 100%;
            height: 100%;
            background: linear-gradient(90deg, transparent, rgba(59, 130, 246, 0.1), transparent);
            transition: left 0.5s ease;
        }
        .format-item:hover::before { left: 100%; }
        .format-item:not(.disabled):hover { 
            background: var(--gray-50); 
            border-color: var(--gray-400); 
            transform: translateY(-3px) scale(1.02);
            box-shadow: var(--shadow-md);
        }
        .format-item.checked { 
            border: 3px solid var(--primary); 
            background: linear-gradient(135deg, rgb(59 130 246 / 0.1) 0%, rgb(59 130 246 / 0.05) 100%);
            transform: scale(1.05);
            box-shadow: var(--shadow-md);
        }
        .format-item.checked:not(.disabled):hover {
            transform: translateY(-3px) scale(1.07);
            box-shadow: var(--shadow-lg);
        }
        .format-item.checked .format-label { color: var(--primary); font-weight: 600; }
        .format-item.disabled {
            opacity: 0.5;
            cursor: not-allowed;
            background: var(--gray-100);
        }
        .format-label { font-weight: 500; color: var(--gray-700); font-size: 0.875rem; user-select: none; transition: all 0.3s ease; position: relative; z-index: 1; }
        .format-icon { font-size: 1.2rem; transition: all 0.3s ease; position: relative; z-index: 1; }
        .format-icon.fa-file-pdf { color: var(--error); }
        .format-icon.fa-file-excel { color: var(--success); }
        .format-item:not(.disabled):hover .format-icon { transform: scale(1.2) rotate(5deg); }
        
        .btn-primary { 
            display: flex; 
            align-items: center; 
            justify-content: center; 
            gap: 0.5rem; 
            width: 100%; 
            padding: 0.75rem 1.5rem; 
            border: none; 
            border-radius: 8px; 
            font-size: 1rem; 
            font-weight: 600; 
            cursor: pointer; 
            transition: all 0.3s ease; 
            background: linear-gradient(135deg, var(--primary) 0%, var(--primary-dark) 100%); 
            color: var(--white); 
            margin-top: 1.5rem;
            position: relative;
            overflow: hidden;
        }
        .btn-primary::before {
            content: '';
            position: absolute;
            top: 0;
            left: -100%;
            width: 100%;
            height: 100%;
            background: linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.2), transparent);
            transition: left 0.6s ease;
        }
        .btn-primary:hover:not(:disabled)::before { left: 100%; }
        .btn-primary:hover:not(:disabled) { 
            background: linear-gradient(135deg, var(--primary-dark) 0%, var(--primary) 100%); 
            transform: translateY(-2px) scale(1.02); 
            box-shadow: var(--shadow-lg); 
        }
        .btn-primary:active:not(:disabled) { transform: translateY(0) scale(0.98); }
        .btn-primary:disabled { 
            opacity: 0.5; 
            cursor: not-allowed; 
            background: var(--gray-400);
        }
        .btn-text { transition: opacity 0.2s ease; position: relative; z-index: 1; }
        
        #progressContainer { display: none; margin-top: 1.5rem; }
        .progress-bar-wrapper { width: 100%; background-color: var(--gray-200); border-radius: 8px; overflow: hidden; height: 1.25rem; margin-top: 0.5rem; position: relative; box-shadow: inset 0 2px 4px rgba(0, 0, 0, 0.1); }
        .progress-bar { 
            width: 0%; 
            height: 100%; 
            background: linear-gradient(90deg, var(--primary-light), var(--primary)); 
            transition: width 0.4s ease-in-out;
            position: relative;
            overflow: hidden;
        }
        .progress-bar::before {
            content: '';
            position: absolute;
            top: 0;
            left: -100%;
            width: 100%;
            height: 100%;
            background: linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.3), transparent);
            animation: progressShine 2s infinite;
        }
        @keyframes progressShine {
            0% { left: -100%; }
            100% { left: 100%; }
        }
        .progress-text-overlay {
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            display: flex;
            align-items: center;
            justify-content: center;
            color: var(--white);
            font-weight: 500;
            font-size: 0.8rem;
            text-shadow: 0 0 3px rgba(0, 0, 0, 0.5);
            z-index: 2;
        }
        #progressStatus { text-align: center; font-size: 0.9rem; color: var(--gray-700); font-weight: 500; }
        
        .modal { display: none; position: fixed; z-index: 2000; left: 0; top: 0; width: 100%; height: 100%; background-color: rgba(0,0,0,0.5); backdrop-filter: blur(4px); opacity: 0; transition: all 0.3s ease; }
        .modal.show { display: flex; align-items: center; justify-content: center; opacity: 1; }
        .modal-content { background: var(--white); border-radius: 12px; box-shadow: var(--shadow-xl); max-width: 700px; width: 90%; max-height: 80vh; overflow-y: auto; transform: scale(0.7); transition: all 0.3s ease; }
        .modal.show .modal-content { transform: scale(1); }
        .modal-header { display: flex; justify-content: space-between; align-items: center; padding: 1.5rem 2rem; border-bottom: 1px solid var(--gray-200); }
        .modal-header h2 { margin: 0; font-size: 1.5rem; font-weight: 600; color: var(--gray-900); }
        .modal-close { font-size: 2rem; color: var(--gray-400); cursor: pointer; transition: all 0.3s ease; user-select: none; line-height: 1; }
        .modal-close:hover { color: var(--gray-600); transform: scale(1.1) rotate(90deg); }
        .modal-body { padding: 2rem; }
        .modal-body ul { list-style: none; padding-left: 0; margin-top: 0.5rem; }
        .modal-body li { background: var(--gray-50); border: 1px solid var(--gray-200); padding: 0.5rem 1rem; border-radius: 6px; margin-bottom: 0.5rem; display: flex; align-items: center; gap: 0.75rem; font-weight: 500; transition: all 0.3s ease; }
        .modal-body li:hover { background: var(--gray-100); }
        .modal-body li i { font-size: 1.1rem; }
        .modal-body .execution-time { font-weight: 500; color: var(--gray-600); display: flex; align-items: center; gap: 0.5rem; margin-bottom: 1rem; padding-bottom: 1rem; border-bottom: 1px solid var(--gray-200); }
        .modal-footer { display: flex; justify-content: flex-end; gap: 1rem; padding: 1.5rem 2rem; border-top: 1px solid var(--gray-200); background: var(--gray-50); border-radius: 0 0 12px 12px; }
        .btn-secondary { 
            padding: 0.5rem 1rem; 
            background: var(--gray-200); 
            border: 1px solid var(--gray-300); 
            border-radius: 6px; 
            cursor: pointer; 
            transition: all 0.3s ease;
        }
        .btn-secondary:hover {
            background: var(--gray-300);
            transform: translateY(-1px);
            box-shadow: var(--shadow);
        }
        .success { color: var(--success); } 
        .error { color: var(--error); }
        .company-section-header { display: flex; align-items: center; gap: 0.5rem; margin-bottom: 1.5rem; }
        
        .form-disabled {
            opacity: 0.6;
            pointer-events: none;
        }
        
        @media (max-width: 768px) { 
            .basic-info-grid { grid-template-columns: 1fr; } 
            .livros-grid { grid-template-columns: 1fr 1fr; } 
            .date-grid { grid-template-columns: 1fr; }
        }
    </style>
</head>
<body>
    <header class="header"><img src="/static/logo.jpg" alt="Logo" class="header-logo"><h1>Sistema de Automação - Livros Fiscais</h1></header>
    <div class="container">
        <div class="card">
            <div class="card-header">
                <div class="icon"><i class="fas fa-cogs"></i></div>
                <h2>Configuração de Geração</h2>
                <div id="lastBackup" class="last-backup" style="display: none;">
                    <span class="last-backup-title">Última Atualização do Banco:</span>
                    <span id="backupDateTime" class="backup-datetime">-</span>
                </div>
            </div>
            
            <!-- STATUS DO BANCO (Só aparece quando bloqueado) -->
            <div id="databaseStatus" class="database-status" style="display: none;">
                <div class="status-header">
                    <div class="status-main">
                        <div id="statusIcon" class="status-icon blocked">
                            <i class="fas fa-sync-alt"></i>
                        </div>
                        <div class="status-info">
                            <div id="statusTitle" class="status-title">Processamento Bloqueado</div>
                            <div id="statusMessage" class="status-message">Banco em atualização...</div>
                        </div>
                    </div>
                </div>
            </div>
            <form id="livrosForm">
                <div class="form-section">
                    <div class="section-title"><i class="fas fa-info-circle"></i>Informações Básicas</div>
                    <div class="basic-info-grid">
                        <div class="company-section">
                            <div class="company-section-header">
                                <label class="main-label"><i class="fas fa-building"></i>Empresas</label>
                                <span class="companies-count" id="companiesCount">0 selecionadas</span>
                            </div>
                            <div class="company-search-container">
                                <input type="text" id="companySearch" class="company-search-input" placeholder="Digite para buscar e selecionar..." autocomplete="off">
                                <div id="companyDropdown" class="company-dropdown"></div>
                                <div id="selectedCompanies" class="selected-companies"></div>
                            </div>
                        </div>
                        <div class="date-section">
                            <label class="main-label"><i class="fas fa-calendar-days"></i>Período de Competência</label>
                            <div class="date-container">
                                <div class="date-grid">
                                    <div class="date-item">
                                        <label class="date-sub-label">Data Inicial</label>
                                        <input type="date" id="dataInicio" name="dataInicio" required>
                                    </div>
                                    <div class="date-item">
                                        <label class="date-sub-label">Data Final</label>
                                        <input type="date" id="dataFim" name="dataFim" required>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="form-section">
                    <div class="section-title"><i class="fas fa-book"></i>Livros Fiscais</div>
                    <div class="livros-grid">
                        <div class="livros-item checked" onclick="toggleLivro(this)"><input type="checkbox" id="livroIPI" name="livroIPI" checked style="display: none;"><span class="livros-label">IPI</span></div>
                        <div class="livros-item checked" onclick="toggleLivro(this)"><input type="checkbox" id="livroISS" name="livroISS" checked style="display: none;"><span class="livros-label">ISS</span></div>
                        <div class="livros-item checked" onclick="toggleLivro(this)"><input type="checkbox" id="livroICMS" name="livroICMS" checked style="display: none;"><span class="livros-label">ICMS</span></div>
                        <div class="livros-item checked" onclick="toggleLivro(this)"><input type="checkbox" id="livroEntradas" name="livroEntradas" checked style="display: none;"><span class="livros-label">Entradas</span></div>
                        <div class="livros-item checked" onclick="toggleLivro(this)"><input type="checkbox" id="livroSaidas" name="livroSaidas" checked style="display: none;"><span class="livros-label">Saídas</span></div>
                    </div>
                </div>
                <div class="form-section">
                    <div class="section-title"><i class="fas fa-file-export"></i>Formatos de Exportação</div>
                    <div class="format-grid">
                        <div class="format-item checked" onclick="toggleFormat(this)"><input type="checkbox" id="gerarPdf" name="gerarPdf" checked style="display: none;"><i class="fas fa-file-pdf format-icon"></i><span class="format-label">PDF</span></div>
                        <div class="format-item checked" onclick="toggleFormat(this)"><input type="checkbox" id="gerarExcel" name="gerarExcel" checked style="display: none;"><i class="fas fa-file-excel format-icon"></i><span class="format-label">Excel</span></div>
                    </div>
                </div>
                <button type="submit" class="btn-primary" id="submitBtn"><span class="btn-text">Gerar Livros Fiscais</span></button>
                <div id="progressContainer">
                    <div id="progressStatus">Iniciando...</div>
                    <div class="progress-bar-wrapper">
                        <div id="progressBar" class="progress-bar"></div>
                        <div id="progressText" class="progress-text-overlay">0%</div>
                    </div>
                </div>
            </form>
        </div>
    </div>
    
    <div id="resultModal" class="modal">
        <div class="modal-content">
            <div class="modal-header"><h2 id="modalTitle"></h2><span class="modal-close" id="modalCloseBtn">×</span></div>
            <div class="modal-body" id="modalBody"></div>
            <div class="modal-footer"><button class="btn-secondary" id="modalCloseBtn2">Fechar</button></div>
        </div>
    </div>
<script>
document.addEventListener('DOMContentLoaded', function() {
    const form = document.getElementById('livrosForm');
    const submitBtn = document.getElementById('submitBtn');
    const companySearch = document.getElementById('companySearch');
    const companyDropdown = document.getElementById('companyDropdown');
    const selectedCompaniesContainer = document.getElementById('selectedCompanies');
    const companiesCount = document.getElementById('companiesCount');
    const progressContainer = document.getElementById('progressContainer');
    const progressBar = document.getElementById('progressBar');
    const progressStatus = document.getElementById('progressStatus');
    const progressText = document.getElementById('progressText');
    const modal = document.getElementById('resultModal');
    const dataInicio = document.getElementById('dataInicio');
    const dataFim = document.getElementById('dataFim');
    
    // Elementos do status do banco
    const databaseStatus = document.getElementById('databaseStatus');
    const statusIcon = document.getElementById('statusIcon');
    const statusTitle = document.getElementById('statusTitle');
    const statusMessage = document.getElementById('statusMessage');
    const lastBackup = document.getElementById('lastBackup');
    const backupDateTime = document.getElementById('backupDateTime');
    
    let companiesData = [];
    let selectedCompanies = [];
    let statusInterval = null;
    let startTime = null;
    let currentProcessingId = null;
    let databaseStatusInterval = null;
    let currentDatabaseStatus = { atualizacao_em_andamento: true, connected: false };
    
    // Variáveis do calendário personalizado
    let currentMonth = new Date().getMonth();
    let currentYear = new Date().getFullYear();
    let activeInput = null;

    const initForm = () => {
        const today = new Date();
        const firstDay = new Date(today.getFullYear(), today.getMonth(), 1).toISOString().split('T')[0];
        const lastDay = new Date(today.getFullYear(), today.getMonth() + 1, 0).toISOString().split('T')[0];
        dataInicio.value = firstDay;
        dataFim.value = lastDay;
        
        // Mostrar estado inicial bloqueado
        updateDatabaseStatusUI(currentDatabaseStatus);
        updateFormAvailability(currentDatabaseStatus);
        
        // Inicializar calendários personalizados
        initCustomCalendars();
        
        // Iniciar monitoramento do banco
        startDatabaseStatusMonitoring();
    };
    
    // SISTEMA DE MONITORAMENTO DO BANCO
    const startDatabaseStatusMonitoring = () => {
        // Buscar status inicial
        updateDatabaseStatus();
        
        // Atualizar a cada 3 segundos
        databaseStatusInterval = setInterval(updateDatabaseStatus, 3000);
    };
    
    const updateDatabaseStatus = async () => {
        try {
            const response = await fetch('/database_status');
            const data = await response.json();
            
            if (data.success) {
                currentDatabaseStatus = data.data;
                updateDatabaseStatusUI(data.data);
                updateFormAvailability(data.data);
            }
        } catch (error) {
            console.error('Erro ao buscar status do banco:', error);
            currentDatabaseStatus = { atualizacao_em_andamento: true, connected: false, erro: 'Erro de comunicação' };
            updateDatabaseStatusUI(currentDatabaseStatus);
            updateFormAvailability(currentDatabaseStatus);
        }
    };
    
    const updateDatabaseStatusUI = (status) => {
        const isUpdating = status.atualizacao_em_andamento;
        const isConnected = status.connected;
        
        // Sempre mostrar a última atualização se disponível
        if (status.ultima_atualizacao && status.ultima_atualizacao.data) {
            lastBackup.style.display = 'flex';
            backupDateTime.textContent = status.ultima_atualizacao.data;
        } else {
            lastBackup.style.display = 'none';
        }
        
        // Verificar se deve mostrar bloqueio
        const shouldBlock = !isConnected || isUpdating;
        
        if (shouldBlock) {
            // Mostrar status de bloqueio
            databaseStatus.style.display = 'block';
            databaseStatus.classList.add('blocked');
            
            statusIcon.className = 'status-icon blocked';
            statusIcon.innerHTML = '<i class="fas fa-sync-alt"></i>';
            statusTitle.textContent = 'Processamento Bloqueado';
            
            if (!isConnected) {
                statusMessage.textContent = 'Banco em atualização';
            } else if (isUpdating) {
                statusMessage.textContent = status.progresso || 'Banco em atualização...';
            }
        } else {
            // Sistema disponível - esconder status de bloqueio
            databaseStatus.style.display = 'none';
            databaseStatus.classList.remove('blocked');
        }
    };
    
    const updateFormAvailability = (status) => {
        const shouldBlock = !status.connected || status.atualizacao_em_andamento;
        const formElements = form.querySelectorAll('input, button, .livros-item, .format-item');
        
        if (!shouldBlock) {
            // Sistema disponível
            form.classList.remove('form-disabled');
            submitBtn.disabled = false;
            submitBtn.innerHTML = '<span class="btn-text">Gerar Livros Fiscais</span>';
            
            formElements.forEach(el => {
                if (el.classList.contains('livros-item') || el.classList.contains('format-item')) {
                    el.classList.remove('disabled');
                } else {
                    el.disabled = false;
                }
            });
        } else {
            // Sistema bloqueado
            form.classList.add('form-disabled');
            submitBtn.disabled = true;
            submitBtn.innerHTML = '<span class="btn-text">Processamento Bloqueado</span>';
            
            formElements.forEach(el => {
                if (el.classList.contains('livros-item') || el.classList.contains('format-item')) {
                    el.classList.add('disabled');
                } else {
                    el.disabled = true;
                }
            });
        }
    };
    
    // SISTEMA DE CALENDÁRIO PERSONALIZADO
    const initCustomCalendars = () => {
        const dateItems = document.querySelectorAll('.date-item');
        dateItems.forEach(item => {
            const input = item.querySelector('input[type="date"]');
            const calendar = createCustomCalendar();
            item.classList.add('custom-date');
            item.style.position = 'relative';
            item.appendChild(calendar);
            
            input.addEventListener('click', (e) => {
                e.preventDefault();
                showCustomCalendar(calendar, input);
            });
            
            input.addEventListener('focus', (e) => {
                e.preventDefault();
                showCustomCalendar(calendar, input);
            });
        });
        
        // Fechar calendário ao clicar fora
        document.addEventListener('click', (e) => {
            if (!e.target.closest('.date-item')) {
                hideAllCalendars();
            }
        });
    };
    
    const createCustomCalendar = () => {
        const calendar = document.createElement('div');
        calendar.className = 'custom-calendar';
        calendar.innerHTML = `
            <div class="calendar-header">
                <button type="button" class="calendar-nav-btn" data-action="prev">‹</button>
                <div class="calendar-month-year"></div>
                <button type="button" class="calendar-nav-btn" data-action="next">›</button>
            </div>
            <div class="calendar-grid">
                <div class="calendar-day-header">D</div>
                <div class="calendar-day-header">S</div>
                <div class="calendar-day-header">T</div>
                <div class="calendar-day-header">Q</div>
                <div class="calendar-day-header">Q</div>
                <div class="calendar-day-header">S</div>
                <div class="calendar-day-header">S</div>
            </div>
            <div class="calendar-actions">
                <button type="button" class="calendar-btn clear">Limpar</button>
                <button type="button" class="calendar-btn today">Hoje</button>
            </div>
        `;
        
        // Adicionar event listeners
        calendar.querySelector('[data-action="prev"]').addEventListener('click', () => navigateMonth(-1, calendar));
        calendar.querySelector('[data-action="next"]').addEventListener('click', () => navigateMonth(1, calendar));
        calendar.querySelector('.clear').addEventListener('click', () => clearDate(calendar));
        calendar.querySelector('.today').addEventListener('click', () => selectToday(calendar));
        
        return calendar;
    };
    
    const showCustomCalendar = (calendar, input) => {
        hideAllCalendars();
        activeInput = input;
        
        // Definir data atual se input tem valor
        if (input.value) {
            const date = new Date(input.value + 'T00:00:00');
            currentMonth = date.getMonth();
            currentYear = date.getFullYear();
        } else {
            const today = new Date();
            currentMonth = today.getMonth();
            currentYear = today.getFullYear();
        }
        
        updateCalendar(calendar);
        
        // Reset position styles
        calendar.style.top = '';
        calendar.style.bottom = '';
        calendar.style.left = '';
        calendar.style.right = '';
        
        // Posicionamento inteligente
        const rect = input.getBoundingClientRect();
        const viewportHeight = window.innerHeight;
        const viewportWidth = window.innerWidth;
        const calendarHeight = 280;
        const calendarWidth = 240;
        
        // Posicionamento vertical
        if (rect.bottom + calendarHeight > viewportHeight && rect.top > calendarHeight) {
            calendar.style.bottom = 'calc(100% + 0.25rem)';
        } else {
            calendar.style.top = 'calc(100% + 0.25rem)';
        }
        
        // Posicionamento horizontal
        if (rect.left + calendarWidth > viewportWidth) {
            calendar.style.right = '0';
        } else {
            calendar.style.left = '0';
        }
        
        calendar.classList.add('show');
    };
    
    const hideAllCalendars = () => {
        document.querySelectorAll('.custom-calendar').forEach(cal => {
            cal.classList.remove('show');
        });
        activeInput = null;
    };
    
    const updateCalendar = (calendar) => {
        const monthNames = [
            'Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho',
            'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'
        ];
        
        // Atualizar cabeçalho
        calendar.querySelector('.calendar-month-year').textContent = 
            `${monthNames[currentMonth]} ${currentYear}`;
        
        // Limpar dias existentes
        const existingDays = calendar.querySelectorAll('.calendar-day');
        existingDays.forEach(day => day.remove());
        
        // Gerar dias do calendário
        const firstDay = new Date(currentYear, currentMonth, 1);
        const lastDay = new Date(currentYear, currentMonth + 1, 0);
        const startDate = new Date(firstDay);
        startDate.setDate(startDate.getDate() - firstDay.getDay());
        
        const grid = calendar.querySelector('.calendar-grid');
        
        for (let i = 0; i < 42; i++) {
            const date = new Date(startDate);
            date.setDate(startDate.getDate() + i);
            
            const dayElement = document.createElement('div');
            dayElement.className = 'calendar-day';
            dayElement.textContent = date.getDate();
            
            const isCurrentMonth = date.getMonth() === currentMonth;
            const isToday = date.toDateString() === new Date().toDateString();
            const isSelected = activeInput && activeInput.value === formatDate(date);
            
            if (!isCurrentMonth) dayElement.classList.add('other-month');
            if (isToday) dayElement.classList.add('today');
            if (isSelected) dayElement.classList.add('selected');
            
            if (isCurrentMonth) {
                dayElement.addEventListener('click', () => selectDate(date, calendar));
            }
            
            grid.appendChild(dayElement);
        }
    };
    
    const navigateMonth = (direction, calendar) => {
        currentMonth += direction;
        if (currentMonth < 0) {
            currentMonth = 11;
            currentYear--;
        } else if (currentMonth > 11) {
            currentMonth = 0;
            currentYear++;
        }
        updateCalendar(calendar);
    };
    
    const selectDate = (date, calendar) => {
        if (activeInput) {
            activeInput.value = formatDate(date);
            activeInput.dispatchEvent(new Event('change'));
            
            // Adicionar animação
            activeInput.classList.add('date-auto-filled');
            setTimeout(() => {
                activeInput.classList.remove('date-auto-filled');
            }, 800);
        }
        hideAllCalendars();
    };
    
    const clearDate = (calendar) => {
        if (activeInput) {
            activeInput.value = '';
            activeInput.dispatchEvent(new Event('change'));
        }
        hideAllCalendars();
    };
    
    const selectToday = (calendar) => {
        const today = new Date();
        selectDate(today, calendar);
    };
    
    const formatDate = (date) => {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
    };
    
    // Auto-completar data final quando data inicial for alterada
    dataInicio.addEventListener('change', function() {
        if (this.value) {
            const [year, month, day] = this.value.split('-').map(Number);
            const lastDayOfMonth = new Date(year, month, 0).getDate();
            const monthStr = month.toString().padStart(2, '0');
            const dayStr = lastDayOfMonth.toString().padStart(2, '0');
            
            dataFim.value = `${year}-${monthStr}-${dayStr}`;
            
            dataFim.classList.add('date-auto-filled');
            setTimeout(() => {
                dataFim.classList.remove('date-auto-filled');
            }, 800);
        }
    });
    
    const updateCompaniesCount = () => {
        companiesCount.textContent = `${selectedCompanies.length} selecionada${selectedCompanies.length !== 1 ? 's' : ''}`;
    };
    
    const renderSelectedCompanies = () => {
        selectedCompaniesContainer.innerHTML = selectedCompanies.map(company => 
            `<div class="company-card" data-code="${company.code}">
                <span>${company.name} (${company.code})</span>
                <button type="button" class="remove-btn" onclick="removeCompany(${company.code})">&times;</button>
            </div>`
        ).join('');
        updateCompaniesCount();
    };
    
    window.removeCompany = (code) => {
        selectedCompanies = selectedCompanies.filter(c => c.code !== code);
        renderSelectedCompanies();
    };
    
    window.toggleLivro = (element) => {
        if (!element.classList.contains('disabled')) {
            toggleCheckbox(element, 'livro', '.livros-item');
        }
    };
    window.toggleFormat = (element) => {
        if (!element.classList.contains('disabled')) {
            toggleCheckbox(element, 'formato', '.format-item');
        }
    };
    
    const toggleCheckbox = (element, groupName, itemSelector) => {
        const checkbox = element.querySelector('input[type="checkbox"]');
        const min = 1;
        const checkedCount = form.querySelectorAll(`${itemSelector} input:checked`).length;
        if (!checkbox.checked || checkedCount > min) {
            checkbox.checked = !checkbox.checked;
            element.classList.toggle('checked', checkbox.checked);
        } else {
            showToast(`Selecione pelo menos 1 ${groupName}.`, 'warning');
        }
    };

    const loadCompanies = async () => {
        try {
            const response = await fetch('/companies');
            const data = await response.json();
            if (data.success) companiesData = data.companies;
            else showToast('Erro ao carregar empresas.', 'error');
        } catch (error) { showToast('Falha na comunicação para buscar empresas.', 'error'); }
    };

    const filterCompanies = (term) => {
        const lowerTerm = term.toLowerCase();
        const selected = new Set(selectedCompanies.map(c => c.code));
        return companiesData.filter(c => 
            !selected.has(c.code) && 
            (c.name.toLowerCase().includes(lowerTerm) || String(c.code).includes(lowerTerm))
        ).slice(0, 10);
    };
    
    const renderDropdown = (companies) => {
        companyDropdown.innerHTML = companies.length ? companies.map(c => 
            `<div class="company-item" data-code="${c.code}" data-name="${c.name}">
                <div class="company-name">${c.name}</div>
                <div class="company-code">Código: ${c.code}</div>
            </div>`
        ).join('') : `<div style="padding: 1rem; text-align: center; color: var(--gray-500);">Nenhum resultado</div>`;
        companyDropdown.classList.add('show');
    };

    companySearch.addEventListener('input', () => {
        const term = companySearch.value;
        if (term.length < 2) {
            companyDropdown.classList.remove('show');
            return;
        }
        renderDropdown(filterCompanies(term));
    });

    companyDropdown.addEventListener('click', (e) => {
        const item = e.target.closest('.company-item');
        if (item) {
            const newCompany = { code: parseInt(item.dataset.code), name: item.dataset.name };
            if (!selectedCompanies.find(c => c.code === newCompany.code)) {
                selectedCompanies.push(newCompany);
                renderSelectedCompanies();
                companySearch.value = '';
                companyDropdown.classList.remove('show');
            }
        }
    });

    document.addEventListener('click', (e) => {
        if (!e.target.closest('.company-search-container')) companyDropdown.classList.remove('show');
    });
    
    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        
        // Verificação final do status do banco
        if (!currentDatabaseStatus.connected || currentDatabaseStatus.atualizacao_em_andamento) {
            let errorMsg = 'Não é possível processar no momento: ';
            if (!currentDatabaseStatus.connected) {
                errorMsg += 'Monitor do banco desconectado';
            } else {
                errorMsg += 'Banco em atualização';
            }
            showToast(errorMsg, 'error');
            return;
        }
        
        if (selectedCompanies.length === 0) {
            showToast('Por favor, selecione pelo menos uma empresa.', 'error');
            return;
        }
        
        startTime = Date.now();
        const formData = new FormData(form);
        const data = {
            empresas_selecionadas: selectedCompanies,
            data_inicio: formData.get('dataInicio'),
            data_fim: formData.get('dataFim'),
            gerar_pdf: !!formData.get('gerarPdf'),
            gerar_excel: !!formData.get('gerarExcel'),
            livros_selecionados: { 
                ipi: !!formData.get('livroIPI'), 
                iss: !!formData.get('livroISS'), 
                icms: !!formData.get('livroICMS'), 
                entradas: !!formData.get('livroEntradas'), 
                saidas: !!formData.get('livroSaidas') 
            }
        };
        
        try {
            const response = await fetch('/gerar_livros', { 
                method: 'POST', 
                headers: { 'Content-Type': 'application/json' }, 
                body: JSON.stringify(data) 
            });
            const result = await response.json();
            
            if (result.success) {
                currentProcessingId = result.processing_id;
                setLoading(true);
                startPolling(result.processing_id);
                showToast(`Processamento iniciado! ID: ${result.processing_id.substring(0, 8)}`, 'success');
            } else {
                handleResult(result);
            }
        } catch (error) {
            handleResult({ success: false, message: `Erro de comunicação: ${error.message}` });
        }
    });

    const setLoading = (isLoading) => {
        if (isLoading) {
            submitBtn.style.display = 'none';
            progressContainer.style.display = 'block';
            updateProgress({ progress: 0, message: 'Iniciando o processo...' });
        } else {
            setTimeout(() => {
                submitBtn.style.display = 'flex';
                progressContainer.style.display = 'none';
            }, 1000);
        }
    };
    
    const startPolling = (processingId) => {
        if (statusInterval) clearInterval(statusInterval);
        
        statusInterval = setInterval(async () => {
            try {
                const response = await fetch(`/status/${processingId}`);
                const status = await response.json();
                
                if (processingId === currentProcessingId) {
                    updateProgress(status);
                    
                    if (status.complete) {
                        stopPolling();
                        const duration = formatDuration(Date.now() - startTime);
                        handleResult(status.result, duration);
                        setLoading(false);
                        currentProcessingId = null;
                    }
                }
            } catch (error) {
                console.error('Erro ao consultar status:', error);
            }
        }, 1000);
    };
    
    const stopPolling = () => { clearInterval(statusInterval); statusInterval = null; };

// FUNÇÃO ATUALIZADA PARA MOSTRAR PROGRESSO COM PASTA FINAL
    const updateProgress = (status) => {
        const percent = Math.min(100, Math.max(0, status.progress || 0));
        progressBar.style.width = percent + '%';
        progressText.textContent = percent + '%';
        
        // DESTACAR ORGANIZAÇÃO POR PASTA FINAL
        let message = status.message || 'Processando...';
        
        // Adicionar indicador visual se detectar múltiplos períodos na mensagem
        if (message.includes('meses:') || message.includes('pasta:')) {
            message = `📁 ${message}`;
        } else if (message.includes('Gerando') && message.includes('→')) {
            // Destacar processamento com indicação de pasta final
            message = `⚡ ${message}`;
        } else if (message.includes('Movendo') && message.includes('→')) {
            // Destacar movimentação para pasta final
            message = `📦 ${message}`;
        }
        
        progressStatus.textContent = message;
    };
    
    // FUNÇÃO ATUALIZADA PARA EXIBIR RESULTADOS COM MÚLTIPLOS PERÍODOS
// FUNÇÃO ATUALIZADA PARA EXIBIR RESULTADOS COM ORGANIZAÇÃO POR PASTA FINAL
const handleResult = (result, duration) => {
    const type = result.success ? 'success' : 'error';
    const modalTitle = type === 'success' ? 'Processamento Concluído' : 'Erro no Processamento';
    
    let modalBody = '';
    if (duration) {
        modalBody += `<p class="execution-time"><i class="fas fa-clock"></i> Tempo total: ${duration}</p>`;
    }
    
    modalBody += `<p class="${type}">${result.message}</p>`;

    // RESUMO GERAL COM DESTAQUE PARA ORGANIZAÇÃO
    if (result.total_empresas) {
        modalBody += `<div style="margin-top: 1rem; padding: 0.75rem; background: var(--gray-50); border-radius: 8px; font-size: 0.9rem;">
            <strong>📊 Resumo Geral:</strong><br>
            • Empresas processadas: ${result.empresas_sucesso}/${result.total_empresas}<br>
            • Livros gerados: ${result.total_livros_gerados}<br>`;
            
        // DESTACAR ORGANIZAÇÃO POR PASTA FINAL
        if (result.total_periodos && result.total_periodos > 1) {
            modalBody += `• <span style="color: var(--primary); font-weight: 600;">Períodos processados: ${result.total_periodos} meses (${result.periodos_processados.join(', ')})</span><br>`;
            modalBody += `• <span style="color: var(--warning); font-weight: 600;">📁 Todos os livros salvos na pasta do mês final: ${result.pasta_final}</span><br>`;
        } else if (result.periodos_processados && result.periodos_processados.length > 0) {
            modalBody += `• Período: ${result.periodos_processados[0]}<br>`;
        }
            
        if (result.total_livros_com_erro > 0) {
            modalBody += `• Falhas: ${result.total_livros_com_erro}<br>`;
        }
        modalBody += `</div>`;
    }

    // EMPRESAS PROCESSADAS COM DETALHAMENTO DE PERÍODOS
    if (result.empresas_processadas && result.empresas_processadas.length > 0) {
        modalBody += `<h4 style="margin-top:1.5rem; color: var(--success);">✅ Empresas Processadas:</h4>`;
        
        result.empresas_processadas.forEach(empresa => {
            modalBody += `<div style="margin-bottom: 1rem; padding: 1rem; background: var(--gray-50); border-radius: 8px; border-left: 4px solid var(--success);">
                <strong>${empresa.empresa}</strong><br>`;
            
            // MOSTRAR PASTA FINAL SE MÚLTIPLOS PERÍODOS
            if (empresa.pasta_final && empresa.total_periodos > 1) {
                modalBody += `<div style="margin-top: 0.5rem; padding: 0.5rem; background: linear-gradient(135deg, rgba(255, 193, 7, 0.1) 0%, rgba(255, 193, 7, 0.05) 100%); border-radius: 4px; border-left: 3px solid var(--warning);">
                    <small style="color: var(--warning); font-weight: 600;">📁 Organização: Todos os livros salvos na pasta ${empresa.pasta_final}</small>
                </div>`;
            }
            
            // SE HÁ MÚLTIPLOS PERÍODOS, MOSTRAR DETALHAMENTO
            if (empresa.periodos_detalhados && empresa.periodos_detalhados.length > 1) {
                modalBody += `<div style="margin-top: 0.5rem;">`;
                modalBody += `<small style="color: var(--primary); font-weight: 600;">📅 Detalhamento por subpasta:</small><br>`;
                
                empresa.periodos_detalhados.forEach(periodo => {
                    modalBody += `<div style="margin-left: 1rem; margin-top: 0.25rem; padding: 0.5rem; background: var(--white); border-radius: 4px; border-left: 2px solid var(--primary);">`;
                    modalBody += `<strong style="font-size: 0.85rem; color: var(--primary);">${periodo.periodo}</strong><br>`;
                    
                    if (periodo.livros_gerados.length > 0) {
                        modalBody += `<small style="color: var(--success);">✓ Livros: ${periodo.livros_gerados.join(', ')}</small><br>`;
                    }
                    if (periodo.erros.length > 0) {
                        modalBody += `<small style="color: var(--error);">❌ Erros: ${periodo.erros.join(', ')}</small>`;
                    }
                    modalBody += `</div>`;
                });
                modalBody += `</div>`;
            } else {
                // PERÍODO ÚNICO - EXIBIÇÃO SIMPLIFICADA
                if (empresa.livros_gerados.length > 0) {
                    modalBody += `<small style="color: var(--success);">✓ Livros gerados: ${empresa.livros_gerados.join(', ')}</small><br>`;
                }
                if (empresa.erros.length > 0) {
                    modalBody += `<small style="color: var(--error);">❌ Erros: ${empresa.erros.join(', ')}</small>`;
                }
            }
            modalBody += `</div>`;
        });
    }

    // EMPRESAS COM ERRO
    if (result.empresas_com_erro && result.empresas_com_erro.length > 0) {
         modalBody += `<h4 style="margin-top:1.5rem; color: var(--error);">❌ Empresas com Erro:</h4>
                       <ul>${result.empresas_com_erro.map(e => `<li><i class="fas fa-times-circle error"></i>${e}</li>`).join('')}</ul>`;
    }
    
    // DICA SOBRE ORGANIZAÇÃO DE ARQUIVOS - ATUALIZADA
    if (result.success && result.total_livros_gerados > 0) {
        let organizacaoTexto = '';
        if (result.total_periodos > 1) {
            organizacaoTexto = `Os livros de todos os períodos foram organizados em subpastas (formato MM-YYYY) dentro da pasta do mês final (${result.pasta_final}), na seção "livros_gerados_pelo_robo".`;
        } else {
            organizacaoTexto = `Os livros foram salvos em subpastas organizadas por mês (formato MM-YYYY) dentro de "livros_gerados_pelo_robo".`;
        }
        
        modalBody += `<div style="margin-top: 1.5rem; padding: 0.75rem; background: linear-gradient(135deg, rgba(59, 130, 246, 0.1) 0%, rgba(59, 130, 246, 0.05) 100%); border-radius: 8px; border-left: 4px solid var(--primary);">
            <i class="fas fa-folder-open" style="color: var(--primary);"></i> 
            <strong style="color: var(--primary);">Organização dos Arquivos:</strong><br>
            <small style="color: var(--gray-700);">${organizacaoTexto}</small>
        </div>`;
    }
    
    openModal(modalTitle, modalBody);
    showToast(result.message, type);
};
    
    const formatDuration = (ms) => {
        if (ms < 1000) return `${ms} ms`;
        const totalSeconds = Math.floor(ms / 1000);
        const minutes = Math.floor(totalSeconds / 60);
        const seconds = totalSeconds % 60;
        let result = '';
        if (minutes > 0) {
            result += `${minutes} minuto${minutes > 1 ? 's' : ''}`;
        }
        if (seconds > 0) {
            if (minutes > 0) result += ' e ';
            result += `${seconds} segundo${seconds > 1 ? 's' : ''}`;
        }
        return result;
    };

    const openModal = (title, content) => {
        modal.querySelector('#modalTitle').textContent = title;
        modal.querySelector('#modalBody').innerHTML = content;
        modal.style.display = 'flex';
        setTimeout(() => modal.classList.add('show'), 10);
    };

    const closeModal = () => {
        modal.classList.remove('show');
        setTimeout(() => modal.style.display = 'none', 300);
    };
    
    modal.querySelectorAll('.modal-close, #modalCloseBtn2').forEach(btn => btn.addEventListener('click', closeModal));
    modal.addEventListener('click', e => { if (e.target === modal) closeModal(); });

    const showToast = (message, type = 'info') => {
        const toast = document.createElement('div');
        const icons = {success: 'check-circle', error: 'times-circle', warning: 'exclamation-triangle', info: 'info-circle'};
        toast.innerHTML = `<i class="fas fa-${icons[type]}" style="margin-right: 0.5rem;"></i> ${message}`;
        Object.assign(toast.style, {
            position: 'fixed', top: '20px', right: '20px', padding: '1rem', borderRadius: '8px', color: 'var(--white)', 
            transform: 'translateX(120%)', transition: 'transform 0.5s ease', zIndex: 3000, 
            background: `var(--${type})`, boxShadow: 'var(--shadow-lg)'
        });
        document.body.appendChild(toast);
        setTimeout(() => toast.style.transform = 'translateX(0)', 100);
        setTimeout(() => {
            toast.style.transform = 'translateX(120%)';
            setTimeout(() => toast.remove(), 500);
        }, 4000);
    };

    // Limpeza ao sair da página
    window.addEventListener('beforeunload', () => {
        if (databaseStatusInterval) {
            clearInterval(databaseStatusInterval);
        }
        if (statusInterval) {
            clearInterval(statusInterval);
        }
    });

    initForm();
    loadCompanies();
});
</script>
</body>
</html>
    ''')

@app.route('/database_status')
def get_database_status():
    """Endpoint para obter o status do banco de dados"""
    try:
        global db_monitor
        if db_monitor is None:
            return jsonify({
                'success': True,
                'data': {
                    'connected': False,
                    'atualizacao_em_andamento': True,
                    'progresso': 'Monitor não inicializado',
                    'erro': 'Sistema ainda inicializando...'
                }
            })
        
        status = db_monitor.get_status()
        return jsonify({
            'success': True,
            'data': status
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/gerar_livros', methods=['POST'])
def iniciar_geracao_livros():
    # Verificação crítica antes de iniciar
    global db_monitor
    if db_monitor is None or not db_monitor.is_safe_to_process():
        banco_status = db_monitor.get_status() if db_monitor else {'connected': False, 'atualizacao_em_andamento': True}
        erro_msg = "Processamento bloqueado: "
        if not banco_status.get('connected'):
            erro_msg += "Monitor do banco desconectado"
        elif banco_status.get('atualizacao_em_andamento'):
            erro_msg += f"Banco em atualização - {banco_status.get('progresso', 'Atualizando...')}"
        else:
            erro_msg += "Sistema não está pronto"
        
        return jsonify({'success': False, 'message': erro_msg}), 400
    
    data = request.get_json()
    required_fields = ['empresas_selecionadas', 'data_inicio', 'data_fim', 'livros_selecionados']
    if not all(k in data for k in required_fields):
        return jsonify({'success': False, 'message': 'Dados incompletos.'}), 400
    
    if not data['empresas_selecionadas']:
        return jsonify({'success': False, 'message': 'Nenhuma empresa selecionada.'}), 400
    
    # Criar um novo ID para este processamento
    processing_id = create_processing_id()
    start_new_processing(processing_id)
    
    # Iniciar processamento em thread separada
    thread = threading.Thread(target=gerar_livros_multiplas_empresas, args=(
        processing_id,
        data['empresas_selecionadas'],
        data['data_inicio'], 
        data['data_fim'],
        data.get('gerar_pdf', False), 
        data.get('gerar_excel', False), 
        data['livros_selecionados']
    ))
    thread.daemon = True
    thread.start()
    
    print(f"Iniciado processamento {processing_id[:8]} para {len(data['empresas_selecionadas'])} empresas")
    
    return jsonify({
        'success': True, 
        'message': 'Processo de geração iniciado.',
        'processing_id': processing_id
    })

@app.route('/status/<processing_id>')
def get_status(processing_id):
    """Obtém o status de um processamento específico"""
    return jsonify(get_processing_status(processing_id))

@app.route('/status')
def get_legacy_status():
    """Endpoint legacy - retorna erro informativo"""
    return jsonify({
        'error': 'Use /status/<processing_id> para obter o status de um processamento específico'
    }), 400

@app.route('/active_processings')
def get_active_processings():
    """Endpoint para listar todos os processamentos ativos"""
    return jsonify({
        'success': True,
        'active_processings': get_all_active_processings()
    })

@app.route('/companies')
def get_companies_route():
    try:
        return jsonify({'success': True, 'companies': get_companies()})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/static/logo.jpg')
def serve_logo():
    logo_path = 'logo.jpg'
    if not os.path.exists(logo_path):
        try:
            from PIL import Image, ImageDraw, ImageFont
            img = Image.new('RGB', (200, 40), color = (25, 45, 90))
            d = ImageDraw.Draw(img)
            try: font = ImageFont.truetype("arial.ttf", 15)
            except IOError: font = ImageFont.load_default()
            d.text((10,10), "Sua Logo Aqui", font=font, fill=(255,255,255))
            img.save(logo_path)
            print(f"AVISO: '{logo_path}' não encontrado. Um substituto foi criado.")
        except ImportError:
            return "Logo não encontrada e Pillow não instalado.", 404
    return send_file(logo_path, mimetype='image/jpeg')

if __name__ == '__main__':
    # Para testar localmente SEM usar R:\, descomente a linha abaixo:
    # os.environ['SIMULATE_ENV'] = 'true'
    
    if os.path.exists('output_robo'):
        shutil.rmtree('output_robo')
    
    print("🚀 Iniciando Sistema de Automação - Livros Fiscais")
    print(f"🔗 WebSocket configurado para: {WEBSOCKET_URI}")
    print("📊 Monitor do banco será iniciado após o Flask")
    print("="*60)
    
    # Inicializar monitor do banco de forma não-bloqueante
    init_database_monitor()
    
    try:
        app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
    except KeyboardInterrupt:
        print("\n🔄 Encerrando sistema...")
        if db_monitor:
            db_monitor.stop_monitoring()
        print("✅ Sistema encerrado")