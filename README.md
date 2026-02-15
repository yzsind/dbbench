# NineData DBBench

<p align="center">
  <img src="src/main/resources/static/img/logo.svg" alt="DBBench Logo" width="128" height="128">
</p>

<p align="center">
  <strong>TPC-C Database Benchmark Tool</strong>
</p>

<p align="center">
  A universal TPC-C benchmark tool supporting multiple databases with real-time web monitoring dashboard.
</p>

---

## Features

- **Multi-Database Support**: MySQL, PostgreSQL, Oracle, SQL Server, DB2, TiDB, OceanBase, Dameng
- **Full TPC-C Implementation**: All 5 transaction types with configurable mix
- **Real-Time Dashboard**: Web UI with live charts and metrics
- **Dual Mode**: CLI for automation, Web UI for interactive use
- **Comprehensive Metrics**: Transaction, Database, and OS metrics collection
- **Easy Configuration**: JDBC URL based connection, auto database type detection

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

#### Main Dashboard
![Main Dashboard](src/main/resources/static/img/main1.jpg)

#### Configuration Panel
![Configuration](src/main/resources/static/img/config1.jpg)

## Quick Start

### Prerequisites

- Java 17+ (or Docker)
- Maven 3.6+ (for building from source)
- Target database with an empty database created (not needed for Docker)

### Docker Quick Start (Recommended)

The Docker image includes both PostgreSQL 17 and DBBench in a single container - no external database required:

```bash
# Clone the repository
git clone https://github.com/yzsind/dbbench.git
cd dbbench

# Build and run
docker build -t dbbench:latest .
docker run -d -p 1929:1929 -p 5432:5432 --name dbbench dbbench:latest
```

Open http://localhost:1929 in your browser.

**Built-in PostgreSQL credentials:**
- Host: `localhost:5432`
- Database: `tpcc`
- Username: `postgres`
- Password: `postgres`

The database is pre-configured and ready to use. Just click "Load Data" in the web UI to start.

### Connect to External Database

To use an external database instead of the built-in PostgreSQL:

```bash
docker run -d -p 1929:1929 \
  -e DB_TYPE=mysql \
  -e DB_JDBC_URL="jdbc:mysql://host.docker.internal:3306/tpcc?useSSL=false&rewriteBatchedStatements=true" \
  -e DB_USERNAME=root \
  -e DB_PASSWORD=password \
  --name dbbench dbbench:latest
```

### Build from Source

```bash
git clone https://github.com/yzsind/dbbench.git
cd dbbench
mvn clean package -DskipTests
```

### Web Mode

Start the web server:

```bash
java -jar target/dbbench-1.0.0.jar
```

Open http://localhost:1929 in your browser.

From the web dashboard you can:
1. Configure database connection and test it
2. Load TPC-C test data
3. Start/Stop benchmark
4. Monitor real-time metrics (TPS, CPU, Network, etc.)
5. View detailed logs

### CLI Mode

```bash
# Load data only
java -jar target/dbbench-1.0.0.jar \
  --jdbcurl "jdbc:mysql://127.0.0.1:3306/tpcc?useSSL=false&rewriteBatchedStatements=true" \
  -u root -p password \
  -w 10 \
  --load-only

# Run benchmark (data must be loaded first)
java -jar target/dbbench-1.0.0.jar \
  --jdbcurl "jdbc:mysql://127.0.0.1:3306/tpcc" \
  -u root -p password \
  -c 50 -d 300

# Clean existing data and reload
java -jar target/dbbench-1.0.0.jar \
  --jdbcurl "jdbc:mysql://127.0.0.1:3306/tpcc" \
  -u root -p password \
  -w 5 \
  --clean
```

## CLI Options

| Option | Description | Default |
|--------|-------------|---------|
| `--jdbcurl` | JDBC connection URL (required) | - |
| `-u, --user` | Database username | root |
| `-p, --password` | Database password | (empty) |
| `-w, --warehouses` | Number of warehouses | 1 |
| `-c, --terminals` | Concurrent threads | 10 |
| `-d, --duration` | Test duration in seconds | 60 |
| `--pool-size` | Connection pool size | 50 |
| `--load-threads` | Parallel threads for data loading | 4 |
| `--load-only` | Only load data, skip benchmark | false |
| `--clean` | Clean existing data and reload | false |
| `-h, --help` | Show help message | - |
| `-V, --version` | Show version | - |

## Configuration File

Edit `src/main/resources/application.properties` for default settings:

```properties
# Server (default port: 1929)
server.port=1929

# Database Connection
db.type=mysql
db.jdbc-url=jdbc:mysql://127.0.0.1:3306/tpcc?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true
db.username=root
db.password=password

# Connection Pool
db.pool.size=50
db.pool.min-idle=10

# Benchmark Settings
benchmark.warehouses=10
benchmark.terminals=50
benchmark.duration=60
benchmark.think-time=false
benchmark.load-concurrency=4

# Transaction Mix (TPC-C Standard, must total 100%)
benchmark.mix.new-order=45
benchmark.mix.payment=43
benchmark.mix.order-status=4
benchmark.mix.delivery=4
benchmark.mix.stock-level=4
```

## Docker Environment Variables

All configuration can be overridden via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `SERVER_PORT` | Web server port | 1929 |
| `DB_TYPE` | Database type | mysql |
| `DB_JDBC_URL` | JDBC connection URL | jdbc:mysql://... |
| `DB_USERNAME` | Database username | root |
| `DB_PASSWORD` | Database password | (empty) |
| `DB_POOL_SIZE` | Connection pool size | 50 |
| `BENCHMARK_WAREHOUSES` | Number of warehouses | 10 |
| `BENCHMARK_TERMINALS` | Concurrent threads | 50 |
| `BENCHMARK_DURATION` | Test duration (seconds) | 60 |
| `BENCHMARK_LOAD_CONCURRENCY` | Data loading threads | 4 |
| `JAVA_OPTS` | JVM options | -Xms512m -Xmx1024m |

## REST API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/benchmark/config` | GET | Get current configuration |
| `/api/benchmark/config` | POST | Update configuration |
| `/api/benchmark/test-connection` | POST | Test database connection |
| `/api/benchmark/init` | POST | Initialize database connection |
| `/api/benchmark/load` | POST | Load TPC-C data |
| `/api/benchmark/clean` | POST | Clean test data |
| `/api/benchmark/start` | POST | Start benchmark |
| `/api/benchmark/stop` | POST | Stop benchmark |
| `/api/benchmark/status` | GET | Get current status |
| `/api/benchmark/logs` | GET | Get activity logs |
| `/api/metrics/current` | GET | Get current metrics |
| `/api/metrics/tps-history` | GET | Get TPS history |

## WebSocket

Connect to `ws://localhost:8080/ws/metrics` for real-time metrics streaming.

Message types:
- Metrics update: `{ "transaction": {...}, "os": {...}, "database": {...} }`
- Status change: `{ "type": "status", "status": "RUNNING" }`
- Progress update: `{ "type": "progress", "progress": 50, "message": "Loading..." }`
- Log entry: `{ "type": "log", "log": { "level": "INFO", "message": "..." } }`

## TPC-C Transaction Mix

| Transaction | Default % | Description |
|-------------|-----------|-------------|
| New-Order | 45% | Creates new orders with 5-15 line items |
| Payment | 43% | Processes customer payments |
| Order-Status | 4% | Queries order status (read-only) |
| Delivery | 4% | Processes pending deliveries |
| Stock-Level | 4% | Checks stock levels (read-only) |

## Metrics Collected

### Transaction Metrics
- Throughput (TPS)
- Total transactions count
- Success/Failure counts and rates
- Latency (average, min, max)
- Per-transaction type breakdown

### Database Metrics
- Active connections
- Buffer pool / Cache hit ratio
- Row lock waits
- Slow queries count

### OS Metrics
- CPU usage (%)
- Memory usage (%)
- Load average (1m, 5m, 15m)
- Network I/O (bytes/sec)
- Disk I/O (bytes/sec)

## Data Scale

Per warehouse (approximately):
| Table | Rows |
|-------|------|
| Item | 100,000 (shared) |
| Warehouse | 1 |
| District | 10 |
| Customer | 30,000 |
| History | 30,000 |
| Order | 30,000 |
| New-Order | 9,000 |
| Order-Line | ~300,000 |
| Stock | 100,000 |

**Recommended sizing:**
- Small test: 1-2 warehouses, 5-10 terminals
- Medium test: 10 warehouses, 50 terminals
- Large test: 100+ warehouses, 200+ terminals

## Screenshots

### Web Dashboard
The web dashboard provides real-time monitoring with:
- Configuration panel with connection testing
- Performance summary (TPS, latency, success rate)
- System resources (CPU, Memory)
- Database metrics
- TPS chart over time
- CPU and Network I/O charts
- Transaction breakdown table
- Activity logs

## Development

### Project Structure

```
src/main/java/com/ninedata/dbbench/
├── cli/                    # CLI runner
├── config/                 # Configuration classes
├── database/               # Database adapters
├── engine/                 # Benchmark engine
├── metrics/                # Metrics collection
├── tpcc/                   # TPC-C implementation
│   ├── loader/             # Data loader
│   └── transaction/        # Transaction implementations
└── web/                    # REST API & WebSocket

src/main/resources/
├── static/                 # Web UI (HTML, CSS, JS)
├── application.properties  # Default configuration
└── banner.txt              # Startup banner
```

### Building from Source

```bash
# Build
mvn clean package

# Run tests
mvn test

# Run with Maven
mvn spring-boot:run
```

## License

Apache License 2.0

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgments

- TPC-C Benchmark Specification: [TPC](http://www.tpc.org/tpcc/)
- Built with Spring Boot, Chart.js, OSHI
