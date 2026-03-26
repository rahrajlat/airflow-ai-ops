FROM ankane/pgvector:latest

ENV POSTGRES_USER=admin
ENV POSTGRES_PASSWORD=admin
ENV POSTGRES_DB=dbt_db

# Install pgvector
RUN apt-get update && \
    apt-get install -y \
        postgresql-server-dev-16 \
        build-essential \
        git && \
    git clone https://github.com/pgvector/pgvector.git && \
    cd pgvector && \
    make && \
    make install && \
    rm -rf /pgvector

EXPOSE 5432