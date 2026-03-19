FROM alpine:latest

RUN apk add --no-cache \
    postgresql \
    postgresql-client \
    bash

RUN mkdir -p /var/lib/postgresql/data && \
    chown -R postgres:postgres /var/lib/postgresql

EXPOSE 5432

CMD ["postgres", "-D", "/var/lib/postgresql/data"]
