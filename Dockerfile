# Build stage for Java application
FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /app
COPY pom.xml .
COPY src ./src

RUN mvn clean package -DskipTests

# Runtime stage with PostgreSQL 17 and JDK 17
FROM ubuntu:22.04

LABEL maintainer="NineData <support@ninedata.cloud>"
LABEL description="NineData DBBench - TPC-C Database Benchmark Tool with PostgreSQL 17"

# Avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install PostgreSQL 17 and OpenJDK 17
RUN apt-get update && apt-get install -y \
    gnupg2 \
    wget \
    lsb-release \
    curl \
    sudo \
    && echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && apt-get update \
    && apt-get install -y \
    postgresql-17 \
    openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Configure PostgreSQL data directory
RUN rm -rf /var/lib/postgresql/17/main && mkdir -p /var/lib/postgresql/17/main && chown -R postgres:postgres /var/lib/postgresql/17

USER postgres
RUN /usr/lib/postgresql/17/bin/initdb -D /var/lib/postgresql/17/main --encoding=UTF8 --locale=C \
    && echo "host all all 0.0.0.0/0 md5" >> /var/lib/postgresql/17/main/pg_hba.conf \
    && echo "listen_addresses='*'" >> /var/lib/postgresql/17/main/postgresql.conf \
    && echo "max_connections=200" >> /var/lib/postgresql/17/main/postgresql.conf \
    && echo "shared_buffers=256MB" >> /var/lib/postgresql/17/main/postgresql.conf \
    && echo "effective_cache_size=512MB" >> /var/lib/postgresql/17/main/postgresql.conf \
    && echo "work_mem=16MB" >> /var/lib/postgresql/17/main/postgresql.conf \
    && echo "maintenance_work_mem=128MB" >> /var/lib/postgresql/17/main/postgresql.conf \
    && echo "checkpoint_completion_target=0.9" >> /var/lib/postgresql/17/main/postgresql.conf \
    && echo "wal_buffers=16MB" >> /var/lib/postgresql/17/main/postgresql.conf \
    && echo "random_page_cost=1.1" >> /var/lib/postgresql/17/main/postgresql.conf

# Create database and user
RUN /usr/lib/postgresql/17/bin/pg_ctl -D /var/lib/postgresql/17/main start -w \
    && psql --command "ALTER USER postgres WITH PASSWORD 'postgres';" \
    && psql --command "CREATE DATABASE tpcc OWNER postgres;" \
    && /usr/lib/postgresql/17/bin/pg_ctl -D /var/lib/postgresql/17/main stop -w

USER root

# Copy the built jar
COPY --from=builder /app/target/dbbench-1.0.0.jar /app/dbbench.jar

# Create startup script
COPY <<'EOF' /app/start.sh
#!/bin/bash
set -e

# Start PostgreSQL
echo "Starting PostgreSQL 17..."
su - postgres -c "/usr/lib/postgresql/17/bin/pg_ctl -D /var/lib/postgresql/17/main start -w"

# Wait for PostgreSQL to be ready
until su - postgres -c "psql -c 'SELECT 1'" > /dev/null 2>&1; do
    echo "Waiting for PostgreSQL..."
    sleep 1
done
echo "PostgreSQL is ready."

# Start DBBench
echo "Starting DBBench..."
exec java $JAVA_OPTS -jar /app/dbbench.jar
EOF

RUN chmod +x /app/start.sh

# Expose ports
EXPOSE 1929 5432

# Environment variables
ENV JAVA_OPTS="-Xms512m -Xmx1024m"
ENV DB_TYPE=postgresql
ENV DB_JDBC_URL=jdbc:postgresql://localhost:5432/tpcc
ENV DB_USERNAME=postgres
ENV DB_PASSWORD=postgres
ENV DB_POOL_SIZE=50
ENV BENCHMARK_WAREHOUSES=10
ENV BENCHMARK_TERMINALS=50
ENV BENCHMARK_DURATION=60

WORKDIR /app
CMD ["/app/start.sh"]
