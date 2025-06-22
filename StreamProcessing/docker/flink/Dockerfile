# Usa una base Flink stabile con Scala compatibile
FROM flink:1.19.0

# Aggiorna pacchetti e installa dipendenze Python + Maven + JDK 16
RUN apt-get update -y && \
    apt-get install -y \
    ca-certificates \
    python3 \
    python3-pip \
    openjdk-17-jdk-headless \
    maven && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Imposta JAVA_HOME per Java 16
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Copia configurazione Flink
COPY requirements.txt /opt/flink/conf/requirements.txt
COPY start_job.sh /opt/flink

# Installa requirements Python
RUN pip3 install -r /opt/flink/conf/requirements.txt
