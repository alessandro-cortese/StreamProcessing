# Usa un'immagine base Ubuntu
FROM ubuntu:22.04

# Evita interazioni durante installazioni
ENV DEBIAN_FRONTEND=noninteractive

# Installa Java 17, Maven, wget, curl e net-tools (utile per debug)
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    maven \
    wget \
    curl \
    net-tools \
    iputils-ping \
    unzip \
    git \
    nano \
    bash \
    && rm -rf /var/lib/apt/lists/*

# Imposta JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Crea directory di lavoro
WORKDIR /app

# Copia eventuale script di avvio
COPY start_job.sh /app/start_job.sh
