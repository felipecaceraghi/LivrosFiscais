# Use Python 3.11 slim como base
FROM python:3.11-slim

# Definir diretório de trabalho
WORKDIR /app

# Instalar dependências do sistema necessárias
RUN apt-get update && apt-get install -y \
    build-essential \
    unixodbc \
    unixodbc-dev \
    curl \
    wget \
    tar \
    gzip \
    libc6-dev \
    gcc \
    g++ \
    netcat-openbsd \
    dnsutils \
    locales \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Configurar timezone de Brasília
ENV TZ=America/Sao_Paulo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Configurar locale pt_BR
RUN sed -i '/pt_BR.UTF-8/s/^# //g' /etc/locale.gen && \
    locale-gen pt_BR.UTF-8

ENV LANG=pt_BR.UTF-8
ENV LANGUAGE=pt_BR:pt
ENV LC_ALL=pt_BR.UTF-8
ENV LC_TIME=pt_BR.UTF-8

# Copiar e instalar driver SQL Anywhere 17
COPY sqlanywhere17-client.tar.gz* /tmp/
RUN if [ -f /tmp/sqlanywhere17-client.tar.gz ]; then \
        cd /tmp && \
        tar -xzf sqlanywhere17-client.tar.gz && \
        SETUP_DIR=$(find /tmp -name "setup" -executable -type f | head -1 | xargs dirname) && \
        cd "$SETUP_DIR" && \
        ./setup -silent -nogui -I_accept_the_license_agreement -sqlany-dir /opt/sqlanywhere17 && \
        cd / && \
        rm -rf /tmp/* ; \
    fi

# Configurar variáveis de ambiente para SQL Anywhere
ENV SQLANY17=/opt/sqlanywhere17
ENV PATH=$SQLANY17/bin64:$SQLANY17/bin32:$PATH
ENV LD_LIBRARY_PATH=$SQLANY17/lib64:$SQLANY17/lib32:$LD_LIBRARY_PATH
ENV DOCKER=true

# Configurar ODBC para SQL Anywhere
RUN if [ -d /opt/sqlanywhere17 ]; then \
        echo "[SQL Anywhere 17]" > /etc/odbcinst.ini && \
        echo "Description=SQL Anywhere 17 ODBC Driver" >> /etc/odbcinst.ini && \
        echo "Driver=/opt/sqlanywhere17/lib64/libdbodbc17.so" >> /etc/odbcinst.ini && \
        echo "Setup=/opt/sqlanywhere17/lib64/libdbodbc17.so" >> /etc/odbcinst.ini && \
        echo "FileUsage=1" >> /etc/odbcinst.ini; \
    fi

# Copiar requirements.txt primeiro para aproveitar cache do Docker
COPY requirements.txt .

# Instalar dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código da aplicação
COPY . .

# Criar diretório para arquivos de saída
RUN mkdir -p output_robo

# Expor porta da aplicação Flask
EXPOSE 5000

# Definir variáveis de ambiente para Flask
ENV FLASK_APP=main.py
ENV FLASK_ENV=production
ENV FLASK_HOST=0.0.0.0
ENV FLASK_PORT=5000

# Comando para iniciar a aplicação
CMD ["python", "main.py"]
