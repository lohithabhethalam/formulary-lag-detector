FROM eclipse-temurin:17-jdk-jammy

RUN apt-get update && \
    apt-get install -y python3.11 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python3", "spark_lag.py"]