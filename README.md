# DBBench - TPC-C Database Benchmark Tool

A universal TPC-C benchmark tool supporting multiple databases with real-time monitoring.

## Supported Databases

- MySQL
- PostgreSQL
- Oracle
- SQL Server
- DB2
- Dameng (达梦)
- OceanBase
- TiDB

## Quick Start

### Prerequisites

- Java 17+
- Maven 3.6+
- Target database running

### Build

```bash
mvn clean package -DskipTests
```

### CLI Mode

```bash
# Basic usage with MySQL
java -jar target/dbbench-1.0.0.jar \
  -t mysql \
  -H 127.0.0.1 \
  -P 3306 \
  -d tpcc \
  -u sysbench \
  -p sysbench \
  -w 1 \
  -c 5 \
  -r 60

# Load data only
java -jar target/dbbench-1.0.0.jar --load-only -w 2

# Skip loading, run benchmark only
java -jar target/dbbench-1.0.0.jar --skip-load -c 10 -r 120
```

### Web Mode

```bash
# Start web server
java -jar target/dbbench-1.0.0.jar --server.port=8080

# Or with Spring Boot
mvn spring-boot:run
```

Then open http://localhost:8080 in your browser.

## CLI Options

| Option | Description | Default |
|--------|-------------|---------|
| `-t, --type` | Database type | mysql |
| `-H, --host` | Database host | 127.0.0.1 |
| `-P, --port` | Database port | 3306 |
| `-d, --database` | Database name | tpcc |
| `-u, --user` | Username | sysbench |
| `-p, --password` | Password | sysbench |
| `-w, --warehouses` | Number of warehouses | 1 |
| `-c, --terminals` | Concurrent connections | 5 |
| `-r, --duration` | Test duration (seconds) | 60 |
| `--load-only` | Only load data | false |
| `--skip-load` | Skip data loading | false |
| `--pool-size` | Connection pool size | 50 |

## Configuration (application.properties)

```properties
# Database
db.type=mysql
db.host=127.0.0.1
db.port=3306
db.name=tpcc
db.username=sysbench
db.password=sysbench
db.pool.size=50

# Benchmark
benchmark.warehouses=1
benchmark.terminals=5
benchmark.duration=60
benchmark.think-time=true

# Transaction Mix (TPC-C Standard)
benchmark.mix.new-order=45
benchmark.mix.payment=43
benchmark.mix.order-status=4
benchmark.mix.delivery=4
benchmark.mix.stock-level=4
```

## REST API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/benchmark/init` | POST | Initialize database connection |
| `/api/benchmark/load` | POST | Load TPC-C data |
| `/api/benchmark/start` | POST | Start benchmark |
| `/api/benchmark/stop` | POST | Stop benchmark |
| `/api/benchmark/status` | GET | Get current status |
| `/api/benchmark/results` | GET | Get benchmark results |
| `/api/metrics/current` | GET | Get current metrics |
| `/api/metrics/history` | GET | Get metrics history |

## WebSocket

Connect to `ws://localhost:8080/ws/metrics` for real-time metrics streaming.

## TPC-C Transaction Mix

- **New-Order (45%)**: Creates new orders with 5-15 line items
- **Payment (43%)**: Processes customer payments
- **Order-Status (4%)**: Queries order status (read-only)
- **Delivery (4%)**: Processes pending deliveries
- **Stock-Level (4%)**: Checks stock levels (read-only)

## Metrics Collected

### Transaction Metrics
- Throughput (TPS)
- Success/Failure counts
- Latency (avg, min, max)
- Per-transaction breakdown

### Database Metrics (MySQL)
- Buffer pool hit ratio
- Row lock waits
- Active connections
- Slow queries

### OS Metrics
- CPU usage
- Memory usage
- Load average
- Disk I/O
- Network I/O

## Data Scale

Per warehouse:
- 100,000 items
- 10 districts
- 30,000 customers
- 30,000 orders
- ~300,000 order lines

## License

MIT
