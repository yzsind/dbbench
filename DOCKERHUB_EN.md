# NineData DBBench

**Ready-to-Use TPC-C Database Benchmark Tool**

DBBench is a professional database performance testing tool that fully implements the TPC-C benchmark specification. It supports 8 major databases and provides a real-time web monitoring dashboard.

## Image Highlights

- **Ready to Use**: Built-in PostgreSQL 17, no external database configuration required
- **Multi-Database Support**: MySQL, PostgreSQL, Oracle, SQL Server, DB2, TiDB, OceanBase, Dameng
- **Real-Time Monitoring**: Web UI with live TPS, latency, CPU, memory, network metrics
- **Full TPC-C**: All 5 transaction types (New-Order, Payment, Order-Status, Delivery, Stock-Level)
- **Flexible Configuration**: Environment variable support for various testing scenarios

## Quick Start

### Using Built-in PostgreSQL (Simplest)

```bash
docker run -d -p 1929:1929 --name dbbench yzsind/dbbench:latest
```

Open http://localhost:1929 in your browser to start testing.

### Connect to External MySQL

```bash
docker run -d -p 1929:1929 \
  -e DB_TYPE=mysql \
  -e DB_JDBC_URL="jdbc:mysql://host.docker.internal:3306/tpcc?useSSL=false&rewriteBatchedStatements=true" \
  -e DB_USERNAME=root \
  -e DB_PASSWORD=yourpassword \
  --name dbbench yzsind/dbbench:latest
```

### Connect to External PostgreSQL

```bash
docker run -d -p 1929:1929 \
  -e DB_TYPE=postgresql \
  -e DB_JDBC_URL="jdbc:postgresql://host.docker.internal:5432/tpcc" \
  -e DB_USERNAME=postgres \
  -e DB_PASSWORD=yourpassword \
  --name dbbench yzsind/dbbench:latest
```

### Connect to Oracle

```bash
docker run -d -p 1929:1929 \
  -e DB_TYPE=oracle \
  -e DB_JDBC_URL="jdbc:oracle:thin:@host.docker.internal:1521:orcl" \
  -e DB_USERNAME=system \
  -e DB_PASSWORD=yourpassword \
  --name dbbench yzsind/dbbench:latest
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_TYPE` | Database type | postgresql |
| `DB_JDBC_URL` | JDBC connection URL | jdbc:postgresql://localhost:5432/tpcc |
| `DB_USERNAME` | Database username | postgres |
| `DB_PASSWORD` | Database password | postgres |
| `DB_POOL_SIZE` | Connection pool size | 50 |
| `BENCHMARK_WAREHOUSES` | Number of warehouses (data scale) | 10 |
| `BENCHMARK_TERMINALS` | Concurrent terminals | 50 |
| `BENCHMARK_DURATION` | Test duration in seconds | 60 |
| `JAVA_OPTS` | JVM options | -Xms512m -Xmx1024m |

## Supported Databases

| Database | JDBC URL Format |
|----------|-----------------|
| MySQL | `jdbc:mysql://host:3306/database` |
| PostgreSQL | `jdbc:postgresql://host:5432/database` |
| Oracle | `jdbc:oracle:thin:@host:1521:sid` |
| SQL Server | `jdbc:sqlserver://host:1433;databaseName=db` |
| DB2 | `jdbc:db2://host:50000/database` |
| TiDB | `jdbc:mysql://host:4000/database` |
| OceanBase | `jdbc:oceanbase://host:2881/database` |
| Dameng | `jdbc:dm://host:5236/database` |

## Usage Workflow

1. **Start Container**: Run the docker run command
2. **Access Web UI**: Open http://localhost:1929
3. **Configure Connection**: Set database connection in the config panel (skip if using built-in PostgreSQL)
4. **Load Data**: Click "Load Data" to generate test data
5. **Run Benchmark**: Click "Start" to begin the benchmark
6. **View Results**: Monitor TPS, latency, and system resources in real-time

## Data Scale Reference

Each warehouse contains approximately:
- Customer: 30,000 rows
- Order: 30,000 rows
- Order-Line: ~300,000 rows
- Stock: 100,000 rows

**Recommended Configurations:**
- Quick Test: 1-2 warehouses, 5-10 terminals
- Standard Test: 10 warehouses, 50 terminals
- Stress Test: 100+ warehouses, 200+ terminals

## Port Reference

| Port | Purpose |
|------|---------|
| 1929 | Web UI and REST API |
| 5432 | Built-in PostgreSQL (optional) |

## Built-in PostgreSQL Connection

To connect directly to the built-in database:
- Host: `localhost:5432`
- Database: `tpcc`
- Username: `postgres`
- Password: `postgres`

```bash
# Expose PostgreSQL port
docker run -d -p 1929:1929 -p 5432:5432 --name dbbench yzsind/dbbench:latest
```

## REST API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/benchmark/config` | GET/POST | Get/Update configuration |
| `/api/benchmark/test-connection` | POST | Test database connection |
| `/api/benchmark/load` | POST | Load test data |
| `/api/benchmark/start` | POST | Start benchmark |
| `/api/benchmark/stop` | POST | Stop benchmark |
| `/api/benchmark/status` | GET | Get current status |
| `/api/metrics/current` | GET | Get real-time metrics |

## Metrics

### Transaction Metrics
- TPS (Transactions Per Second)
- Success / Failure rate
- Average / Max / Min latency
- Per-transaction type statistics

### System Metrics
- CPU usage
- Memory usage
- Network I/O
- Disk I/O

### Database Metrics
- Active connections
- Cache hit ratio
- Row lock waits
- Slow query count

## Source Code & Documentation

- GitHub: https://github.com/yzsind/dbbench
- Issues: https://github.com/yzsind/dbbench/issues

## License

Apache License 2.0
