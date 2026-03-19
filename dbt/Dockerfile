FROM python:3.9-slim

WORKDIR /dbt

RUN apt-get update && apt-get install -y \
    git \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    dbt-core==1.7.0 \
    dbt-postgres==1.7.0 \
    protobuf==4.21.12

COPY . /dbt/

RUN mkdir -p /root/.dbt && \
    cp /dbt/profiles.yml /root/.dbt/profiles.yml

CMD ["dbt", "run"]