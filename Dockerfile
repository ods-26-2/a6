FROM python:3.12-slim

WORKDIR /app

# Instala apenas as dependências leves necessárias para o servidor web + Kafka em nuvem
RUN pip install --no-cache-dir confluent-kafka==2.8.2 numpy==1.26.4

# Copia os arquivos necessários do componente A6
COPY lexicon_a6.json /app/lexicon_a6.json
COPY sugestao_correcao_pipeline.py /app/sugestao_correcao_pipeline.py
COPY kafka_integration.py /app/kafka_integration.py
COPY mock_s6_e_i4.py /app/mock_s6_e_i4.py
COPY servidor_a6.py /app/servidor_a6.py
COPY avatar/ybot.glb /app/avatar/ybot.glb
COPY trajectories_mock/ /app/trajectories_mock/

# Variáveis de ambiente padrão (Cloud Run injeta PORT automaticamente)
ENV PORT=8080
ENV KAFKA_BOOTSTRAP_SERVERS=34.66.235.91:9093
ENV PYTHONUNBUFFERED=1

EXPOSE 8080

CMD ["python", "servidor_a6.py", "--host", "0.0.0.0"]

