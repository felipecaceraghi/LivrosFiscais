# Dockerização da Aplicação Livros Fiscais

Este documento explica como usar Docker para executar a aplicação de geração de livros fiscais.

## Pré-requisitos

- Docker instalado
- Docker Compose instalado
- Arquivo `sqlanywhere17-client.tar.gz` na raiz do projeto

## Estrutura dos Arquivos

```
LivrosFiscais/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── sqlanywhere17-client.tar.gz
├── main.py
├── LivroICMS.py
├── LivroEntradas.py
├── LivroSaidas.py
├── LivroIpi.py
├── LivroIss.py
└── output_robo/
```

## Como usar

### 1. Construir e executar com Docker Compose

```bash
# Construir e iniciar os serviços
docker-compose up --build

# Executar em segundo plano
docker-compose up -d --build

# Parar os serviços
docker-compose down

# Ver logs
docker-compose logs -f livros-fiscais
```

### Configuração Opcional com .env

Você pode criar um arquivo `.env` baseado no `.env.example` para customizar as configurações:

```bash
# Copiar arquivo de exemplo
cp .env.example .env

# Editar as configurações conforme necessário
nano .env
```

Depois descomente a linha `env_file` no `docker-compose.yml`.

### 2. Usando apenas Docker

```bash
# Construir a imagem
docker build -t livros-fiscais .

# Executar o container com todas as configurações
docker run -d \
  --name livros-fiscais-app \
  -p 5000:5000 \
  -v $(pwd)/output_robo:/app/output_robo \
  -v $(pwd)/logs:/app/logs \
  -v "/home/roboestatistica/rede/Acesso Digital:/home/roboestatistica/rede/Acesso Digital" \
  --add-host "NOTE-GO-273.go.local:192.168.51.8" \
  livros-fiscais
```

## Acessando a aplicação

Após iniciar os containers, a aplicação estará disponível em:
- **URL**: http://localhost:5000

## Volumes

- `./output_robo:/app/output_robo` - Diretório onde são salvos os arquivos PDF/XLSX gerados
- `./logs:/app/logs` - Diretório para logs da aplicação (opcional)
- `/home/roboestatistica/rede/Acesso Digital:/home/roboestatistica/rede/Acesso Digital` - Volume de rede para acesso a recursos compartilhados

## Configuração de Rede

O container está configurado com:

### Extra Hosts
- `NOTE-GO-273.go.local:192.168.51.8` - Mapeamento de host para acesso ao servidor de banco de dados

### Timezone
- Configurado para `America/Sao_Paulo` (horário de Brasília)
- Volumes mapeados para sincronizar timezone com o host
- Locale configurado para `pt_BR.UTF-8`

Esta configuração permite que o container acesse o servidor de banco de dados através do hostname `NOTE-GO-273.go.local`, resolvendo para o IP `192.168.51.8`, e garante que todas as datas sejam formatadas corretamente no padrão brasileiro.

## Variáveis de Ambiente

As seguintes variáveis são configuradas automaticamente:

```bash
FLASK_APP=main.py
FLASK_ENV=production
FLASK_HOST=0.0.0.0
FLASK_PORT=5000
SQLANY17=/opt/sqlanywhere17
DOCKER=true
```

## Monitoramento

O container inclui um healthcheck que verifica se a aplicação está respondendo:
- Intervalo: 30 segundos
- Timeout: 10 segundos
- Tentativas: 3

## Troubleshooting

### Problemas Comuns

#### 1. Erro "Device or resource busy" no output_robo
Este erro ocorre quando o volume está montado. A aplicação foi corrigida para lidar com isso automaticamente.

#### 2. Erro de locale pt_BR.UTF-8
O Dockerfile foi configurado para instalar e configurar o locale brasileiro automaticamente.

#### 3. Problemas de conectividade com o banco
Verifique se o host está sendo resolvido e se a porta está acessível:

```bash
# Verificar se o host está sendo resolvido corretamente
docker-compose exec livros-fiscais nslookup NOTE-GO-273.go.local

# Testar conectividade na porta do banco (assumindo porta 2638)
docker-compose exec livros-fiscais nc -zv NOTE-GO-273.go.local 2638
```

### Comandos de Diagnóstico

#### Verificar status dos containers
```bash
docker-compose ps
```

#### Ver logs detalhados
```bash
docker-compose logs livros-fiscais
```

#### Entrar no container para debug
```bash
docker-compose exec livros-fiscais bash
```

#### Verificar instalação do SQL Anywhere
```bash
docker-compose exec livros-fiscais ls -la /opt/sqlanywhere17/
```

#### Testar conectividade ODBC
```bash
docker-compose exec livros-fiscais odbcinst -q -d
```

#### Verificar volume de rede
```bash
# Verificar se o volume de rede está montado corretamente
docker-compose exec livros-fiscais ls -la "/home/roboestatistica/rede/Acesso Digital"
```

#### Verificar configuração de timezone
```bash
# Verificar timezone do container
docker-compose exec livros-fiscais date
docker-compose exec livros-fiscais cat /etc/timezone

# Verificar locale
docker-compose exec livros-fiscais locale

# Verificar variáveis de ambiente de timezone
docker-compose exec livros-fiscais env | grep TZ
```

#### Testar formato de data brasileiro
```bash
# Testar comando date no container
docker-compose exec livros-fiscais date '+%d/%m/%Y %H:%M:%S'

# Verificar locale de tempo
docker-compose exec livros-fiscais locale -k LC_TIME
```

## Desenvolvimento

Para desenvolvimento, você pode montar o código fonte como volume:

```yaml
volumes:
  - .:/app
  - ./output_robo:/app/output_robo
```

## Produção

Para produção, considere:
1. Usar um servidor web robusto (nginx como proxy reverso)
2. Configurar logs externos
3. Usar secrets para credenciais do banco
4. Implementar backup dos arquivos gerados

## Comandos Úteis

```bash
# Rebuild sem cache
docker-compose build --no-cache

# Limpar containers e volumes
docker-compose down -v

# Ver uso de recursos
docker stats

# Limpar imagens não utilizadas
docker system prune -a
```
