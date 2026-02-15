# NineData DBBench

**开箱即用的 TPC-C 数据库基准测试工具**

DBBench 是一款专业的数据库性能测试工具，完整实现 TPC-C 基准测试规范，支持 8 种主流数据库，提供实时 Web 监控面板。

## 镜像特点

- **开箱即用**: 内置 PostgreSQL 17，无需额外配置数据库
- **多数据库支持**: MySQL、PostgreSQL、Oracle、SQL Server、DB2、TiDB、OceanBase、达梦
- **实时监控**: Web UI 实时展示 TPS、延迟、CPU、内存、网络等指标
- **完整 TPC-C**: 实现全部 5 种事务类型（New-Order、Payment、Order-Status、Delivery、Stock-Level）
- **灵活配置**: 支持环境变量配置，适配各种测试场景

## 快速开始

### 使用内置 PostgreSQL（最简单）

```bash
docker run -d -p 1929:1929 --name dbbench ninedata/dbbench:latest
```

打开浏览器访问 http://localhost:1929，即可开始测试。

### 连接外部 MySQL

```bash
docker run -d -p 1929:1929 \
  -e DB_TYPE=mysql \
  -e DB_JDBC_URL="jdbc:mysql://host.docker.internal:3306/tpcc?useSSL=false&rewriteBatchedStatements=true" \
  -e DB_USERNAME=root \
  -e DB_PASSWORD=yourpassword \
  --name dbbench ninedata/dbbench:latest
```

### 连接外部 PostgreSQL

```bash
docker run -d -p 1929:1929 \
  -e DB_TYPE=postgresql \
  -e DB_JDBC_URL="jdbc:postgresql://host.docker.internal:5432/tpcc" \
  -e DB_USERNAME=postgres \
  -e DB_PASSWORD=yourpassword \
  --name dbbench ninedata/dbbench:latest
```

### 连接 Oracle

```bash
docker run -d -p 1929:1929 \
  -e DB_TYPE=oracle \
  -e DB_JDBC_URL="jdbc:oracle:thin:@host.docker.internal:1521:orcl" \
  -e DB_USERNAME=system \
  -e DB_PASSWORD=yourpassword \
  --name dbbench ninedata/dbbench:latest
```

## 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `DB_TYPE` | 数据库类型 | postgresql |
| `DB_JDBC_URL` | JDBC 连接 URL | jdbc:postgresql://localhost:5432/tpcc |
| `DB_USERNAME` | 数据库用户名 | postgres |
| `DB_PASSWORD` | 数据库密码 | postgres |
| `DB_POOL_SIZE` | 连接池大小 | 50 |
| `BENCHMARK_WAREHOUSES` | 仓库数量（数据规模） | 10 |
| `BENCHMARK_TERMINALS` | 并发终端数 | 50 |
| `BENCHMARK_DURATION` | 测试时长（秒） | 60 |
| `JAVA_OPTS` | JVM 参数 | -Xms512m -Xmx1024m |

## 支持的数据库

| 数据库 | JDBC URL 格式 |
|--------|---------------|
| MySQL | `jdbc:mysql://host:3306/database` |
| PostgreSQL | `jdbc:postgresql://host:5432/database` |
| Oracle | `jdbc:oracle:thin:@host:1521:sid` |
| SQL Server | `jdbc:sqlserver://host:1433;databaseName=db` |
| DB2 | `jdbc:db2://host:50000/database` |
| TiDB | `jdbc:mysql://host:4000/database` |
| OceanBase | `jdbc:oceanbase://host:2881/database` |
| 达梦 | `jdbc:dm://host:5236/database` |

## 使用流程

1. **启动容器**: 运行 docker run 命令
2. **访问 Web UI**: 打开 http://localhost:1929
3. **配置连接**: 在配置面板中设置数据库连接（使用内置 PostgreSQL 可跳过）
4. **加载数据**: 点击 "Load Data" 生成测试数据
5. **运行测试**: 点击 "Start" 开始基准测试
6. **查看结果**: 实时监控 TPS、延迟、系统资源等指标

## 数据规模参考

每个仓库约包含：
- Customer: 30,000 行
- Order: 30,000 行
- Order-Line: ~300,000 行
- Stock: 100,000 行

**推荐配置：**
- 快速测试: 1-2 仓库, 5-10 终端
- 标准测试: 10 仓库, 50 终端
- 压力测试: 100+ 仓库, 200+ 终端

## 端口说明

| 端口 | 用途 |
|------|------|
| 1929 | Web UI 和 REST API |
| 5432 | 内置 PostgreSQL（可选暴露） |

## 内置 PostgreSQL 连接信息

如需直接连接内置数据库：
- Host: `localhost:5432`
- Database: `tpcc`
- Username: `postgres`
- Password: `postgres`

```bash
# 暴露 PostgreSQL 端口
docker run -d -p 1929:1929 -p 5432:5432 --name dbbench ninedata/dbbench:latest
```

## REST API

| 接口 | 方法 | 说明 |
|------|------|------|
| `/api/benchmark/config` | GET/POST | 获取/更新配置 |
| `/api/benchmark/test-connection` | POST | 测试数据库连接 |
| `/api/benchmark/load` | POST | 加载测试数据 |
| `/api/benchmark/start` | POST | 开始测试 |
| `/api/benchmark/stop` | POST | 停止测试 |
| `/api/benchmark/status` | GET | 获取当前状态 |
| `/api/metrics/current` | GET | 获取实时指标 |

## 监控指标

### 事务指标
- TPS（每秒事务数）
- 成功率 / 失败率
- 平均延迟 / 最大延迟 / 最小延迟
- 各事务类型统计

### 系统指标
- CPU 使用率
- 内存使用率
- 网络 I/O
- 磁盘 I/O

### 数据库指标
- 活跃连接数
- 缓存命中率
- 行锁等待
- 慢查询数

## 源码与文档

- GitHub: https://github.com/yzsind/dbbench
- 问题反馈: https://github.com/yzsind/dbbench/issues

## 许可证

Apache License 2.0
