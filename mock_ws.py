#!/usr/bin/env python3
"""
Servidor WebSocket Mock para testar o sistema de monitoramento do banco.
Este script simula o comportamento do monitor de backup real.

Para usar:
1. Execute este arquivo: python websocket_mock_server.py
2. Execute o sistema principal em outro terminal: python app.py
3. Acesse http://localhost:5000 e veja o monitoramento funcionando
"""

import asyncio
import websockets
import json
import random
from datetime import datetime, timedelta

# Configurações
HOST = "localhost"
PORT = 8765

class MockDatabaseMonitor:
    def __init__(self):
        self.current_status = {
            'atualizacao_em_andamento': False,  # Começa como FALSE
            'ultima_atualizacao': {
                'data': '23/07/2025 16:00',
                'tipo': 'Somente modificações',
                'arquivo': '81318_20250723_1600M.dom',
                'baixado_em': '2025-07-23 16:50:29'
            },
            'progresso': 'Aguardando próximo horário de backup: 19:00',
            'erro': None,
            'proximo_horario_backup': '19:00'
        }
        self.connected_clients = set()
        self.simulation_state = 'waiting'  # waiting, updating, completed, error
        self.update_start_time = None
    
    async def register_client(self, websocket):
        """Registra um novo cliente"""
        self.connected_clients.add(websocket)
        print(f"📱 Cliente conectado. Total: {len(self.connected_clients)}")
        
        # Enviar status inicial
        await self.send_to_client(websocket, {
            "type": "status",
            "timestamp": datetime.now().isoformat(),
            "data": self.current_status
        })
    
    async def unregister_client(self, websocket):
        """Remove um cliente"""
        self.connected_clients.discard(websocket)
        print(f"📱 Cliente desconectado. Total: {len(self.connected_clients)}")
    
    async def send_to_client(self, websocket, message):
        """Envia mensagem para um cliente específico"""
        try:
            await websocket.send(json.dumps(message))
        except websockets.exceptions.ConnectionClosed:
            pass
    
    async def broadcast(self, message):
        """Envia mensagem para todos os clientes conectados"""
        if self.connected_clients:
            message["timestamp"] = datetime.now().isoformat()
            disconnected = []
            
            for client in self.connected_clients:
                try:
                    await self.send_to_client(client, message)
                except websockets.exceptions.ConnectionClosed:
                    disconnected.append(client)
            
            # Remove clientes desconectados
            for client in disconnected:
                self.connected_clients.discard(client)
    
    async def simulate_backup_cycle(self):
        """Simula um ciclo completo de backup"""
        print("🔄 Iniciando simulação de ciclo de backup...")
        
        # Fase 1: Verificando (ESTÁ atualizando)
        self.simulation_state = 'checking'
        self.current_status.update({
            'atualizacao_em_andamento': True,
            'progresso': 'Verificando disponibilidade de novos backups...',
            'erro': None
        })
        
        await self.broadcast({
            "type": "checking",
            "data": {"message": "Verificando disponibilidade de novos backups..."}
        })
        await asyncio.sleep(3)
        
        # Fase 2: Baixando (ESTÁ atualizando)
        self.current_status['progresso'] = 'Baixando arquivo de backup...'
        await self.broadcast({
            "type": "progress",
            "data": {"message": "Baixando arquivo de backup..."}
        })
        await asyncio.sleep(2)
        
        # Fase 3: Aplicando (ESTÁ atualizando)
        self.current_status['progresso'] = 'Aplicando atualização no banco de dados...'
        await self.broadcast({
            "type": "progress",
            "data": {"message": "Aplicando atualização no banco de dados..."}
        })
        await asyncio.sleep(4)
        
        # Decidir resultado (90% sucesso, 10% erro para realismo)
        if random.random() < 0.9:
            # Sucesso - NÃO está mais atualizando
            self.simulation_state = 'completed'
            novo_arquivo = f"81318_{datetime.now().strftime('%Y%m%d_%H%M')}M.dom"
            
            self.current_status.update({
                'atualizacao_em_andamento': False,
                'progresso': f'Aguardando próximo backup...',
                'ultima_atualizacao': {
                    'data': datetime.now().strftime('%d/%m/%Y %H:%M'),
                    'tipo': 'Somente modificações',
                    'arquivo': novo_arquivo,
                    'baixado_em': datetime.now().isoformat()
                },
                'erro': None
            })
            
            await self.broadcast({
                "type": "completed",
                "data": {
                    "message": f"Backup aplicado com sucesso!",
                    "arquivo": novo_arquivo
                }
            })
            print(f"✅ Simulação concluída com sucesso: {novo_arquivo}")
            
        else:
            # Erro - NÃO está mais atualizando
            self.simulation_state = 'error'
            erro_msg = random.choice([
                "Erro de conexão com o servidor de backup",
                "Arquivo corrompido detectado",
                "Falha na validação do backup",
                "Timeout na aplicação do backup"
            ])
            
            self.current_status.update({
                'atualizacao_em_andamento': False,
                'progresso': 'Aguardando próximo backup...',
                'erro': erro_msg
            })
            
            await self.broadcast({
                "type": "error",
                "data": {"message": erro_msg}
            })
            print(f"❌ Simulação terminou com erro: {erro_msg}")
        
        # Calcular próximo horário
        proxima_hora = datetime.now() + timedelta(hours=2)
        self.current_status['proximo_horario_backup'] = proxima_hora.strftime('%H:%M')
        
        # Enviar estado de aguardando (NÃO está atualizando)
        await self.broadcast({
            "type": "waiting",
            "data": {"message": f"Aguardando próximo horário de backup: {proxima_hora.strftime('%H:%M')}"}
        })
        
        # Voltar ao estado de espera
        self.simulation_state = 'waiting'
    
    async def run_simulation(self):
        """Loop principal da simulação"""
        print("🎮 Simulação iniciada. Pressione Ctrl+C para parar.")
        
        try:
            while True:
                if self.simulation_state == 'waiting':
                    # Simula espera entre 30-120 segundos antes do próximo backup
                    wait_time = random.randint(30, 120)
                    print(f"⏰ Próxima simulação em {wait_time} segundos...")
                    
                    await asyncio.sleep(wait_time)
                    
                    if self.connected_clients:  # Só simula se tem clientes conectados
                        await self.simulate_backup_cycle()
                    else:
                        print("📵 Nenhum cliente conectado, pulando simulação...")
                else:
                    await asyncio.sleep(1)
        
        except KeyboardInterrupt:
            print("\n🛑 Simulação interrompida pelo usuário")

# Instância global do monitor
monitor = MockDatabaseMonitor()

async def handle_client(websocket, path):
    """Manipula conexões de clientes WebSocket"""
    await monitor.register_client(websocket)
    
    try:
        # Mantém a conexão ativa
        async for message in websocket:
            # Por enquanto, apenas recebe mas não processa mensagens do cliente
            print(f"📨 Mensagem recebida: {message}")
    
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        await monitor.unregister_client(websocket)

async def main():
    """Função principal"""
    print("🚀 Iniciando Servidor WebSocket Mock")
    print(f"🔗 Endereço: ws://{HOST}:{PORT}")
    print("="*50)
    
    # Iniciar servidor WebSocket
    server = await websockets.serve(handle_client, HOST, PORT)
    print(f"✅ Servidor WebSocket ativo em ws://{HOST}:{PORT}")
    
    # Iniciar simulação em paralelo
    simulation_task = asyncio.create_task(monitor.run_simulation())
    
    try:
        await asyncio.gather(
            server.wait_closed(),
            simulation_task
        )
    except KeyboardInterrupt:
        print("\n🔄 Encerrando servidor...")
        server.close()
        await server.wait_closed()
        print("✅ Servidor encerrado")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Até logo!")