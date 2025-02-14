FROM debian:12-slim

RUN apt-get update && apt-get install -y \
    mariadb-client \
    python3 \
    python3-boto3

WORKDIR /app
COPY backup.py .

ENTRYPOINT ["python3", "backup.py"]
