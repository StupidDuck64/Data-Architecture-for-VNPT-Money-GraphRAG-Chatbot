# VNPT Money Platform

> Monorepo tích hợp **Data Pipeline** (DE Team) + **GraphRAG Chatbot** (AI Team) vào một hệ thống duy nhất, chạy bằng một lệnh.

---

## Kiến trúc tổng thể

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                        VNPT Money Platform                                 ║
╠═══════════════════════════════╦════════════════════════════════════════════╣
║   DATA ENGINEERING TEAM       ║   AI ENGINEERING TEAM                      ║
║   data_pipeline/              ║   chatbot/                                 ║
║                               ║                                            ║
║  [VNPTPay Website]            ║  [User]                                    ║
║       │ Playwright / BS4      ║     │ HTTP :8000                           ║
║       ▼                       ║     ▼                                      ║
║  VNPTScraper                  ║  Chainlit App                              ║
║       │ clean                 ║     │                                      ║
║       ▼                       ║     ├─ IntentParserLocal  (rule-based)     ║
║  DataCleaner                  ║     │  ──OR──                              ║
║       │ load                  ║     ├─ IntentParserLLM    (GPT-4o-mini)    ║
║       ▼                       ║     │                                      ║
║  Neo4jLoader ─────────────────╫────▶ Neo4j  :7687  (DÙNG CHUNG)           ║
║       │ export_csv            ║     │  Graph DB – nodes + relationships    ║
║       ▼                       ║     │                                      ║
║  shared_data/ ────────────────╫────▶ /var/lib/neo4j/import                ║
║  (10 CSV files)               ║     │   └─ LOAD CSV khi re-seed            ║
║       │ backup                ║     │                                      ║
║       ▼                       ║     ├─ ConstrainedVectorSearch             ║
║  MinIO  :9000/:9001           ║     │   (fulltext fallback nếu không có    ║
║  raw/ clean/ backup/          ║     │    OpenAI API key)                   ║
║       ▲                       ║     │                                      ║
║  Airflow  :8080               ║     └─ Redis  :6379  (session cache db=1)  ║
║  schedule: thứ Hai 02:00 UTC  ║                                            ║
╠═══════════════════════════════╩════════════════════════════════════════════╣
║  MONITORING                                                                ║
║  Chatbot metrics :8001  →  Prometheus :9090  →  Grafana :3000              ║
╚════════════════════════════════════════════════════════════════════════════╝
```

### Luồng dữ liệu (Data Flow)

```
[Airflow DAG – mỗi thứ Hai 02:00 UTC]

  generate_run_id
       │
       ▼
  crawl_data              ←  Playwright scrape VNPTPay
       │
       ▼
  clean_and_save          ←  DataCleaner + lưu MinIO raw/
       │
       ▼
  load_neo4j              ←  Neo4jLoader.load() → Neo4j:7687
       │
       ▼
  export_csv_to_shared    ←  Neo4jLoader.export_to_csv_dir("/shared_data")
       │                      10 CSV files → ./shared_data/ (host bind-mount)
       ▼
  backup                  ←  MinIO backup/
       │
       ▼
  notify
```

### Điểm tích hợp: `shared_data/`

| Chiều | Actor | Đường dẫn | Cơ chế |
|-------|-------|-----------|--------|
| **Ghi** | pipeline container | `/shared_data/` | `export_to_csv_dir()` sau mỗi run |
| **Mount** | neo4j container | `/var/lib/neo4j/import` | bind-mount → dùng `LOAD CSV` khi re-seed |
| **Query** | chatbot container | `bolt://neo4j:7687` | Bolt trực tiếp – không cần re-ingest |

---

## Cấu trúc thư mục

```
VNPT_Money_Platform/
├── docker-compose.yml            ← Một file duy nhất – chạy toàn bộ hệ thống
├── .env                          ← Biến môi trường (copy từ .env.example)
├── .env.example
├── seed_neo4j.cypher             ← Script seed nodes (dùng 1 lần)
├── seed_rels.cypher              ← Script seed relationships (dùng 1 lần)
├── monitoring/
│   └── prometheus.yml
├── shared_data/                  ← Cầu nối pipeline ↔ Neo4j (10 CSV files)
│
├── chatbot/                      ← AI Team (Chainlit + GraphRAG)
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── src/
│   │   ├── app.py                ← Chainlit entry point
│   │   ├── pipeline.py           ← Orchestrate: intent → retrieval → rank → response
│   │   ├── intent_parser.py      ← Rule-based (local) + LLM hybrid parser
│   │   ├── retrieval.py          ← Vector search + fulltext fallback + graph traversal
│   │   ├── ranking.py            ← Multi-signal RRF ranker
│   │   ├── decision_engine.py    ← Direct answer / clarify / escalate
│   │   ├── response_generator.py ← Template (no API) hoặc GPT-4o-mini
│   │   └── schema.py             ← Config, enums, dataclasses
│   └── monitoring/grafana/       ← Grafana dashboards
│
└── data_pipeline/                ← DE Team (Airflow + Scrapers)
    ├── docker/
    │   ├── Dockerfile.airflow
    │   └── Dockerfile.pipeline
    ├── airflow/dags/
    │   └── vnpt_pipeline_dag.py  ← DAG (có task export_csv_to_shared)
    ├── pipeline/
    │   ├── run_pipeline.py       ← CLI runner (--export-dir flag)
    │   ├── crawlers/
    │   ├── loaders/
    │   │   └── neo4j_loader.py   ← export_to_csv_dir() method
    │   ├── transforms/
    │   └── backup/
    └── external_data/            ← Seed CSVs tĩnh ban đầu
```

---

## Bắt đầu từ đầu (First-Time Setup)

### Bước 1 – Chuẩn bị file môi trường

```bash
cd VNPT_Money_Platform
cp .env.example .env
```

Mở `.env` và điền:

```dotenv
NEO4J_PASSWORD=<mật khẩu tùy chọn>
OPENAI_API_KEY=<sk-... nếu muốn dùng LLM mode, để trống nếu không>

# Tạo Fernet key cho Airflow:
# python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
AIRFLOW_FERNET_KEY=<key vừa tạo>
AIRFLOW_SECRET_KEY=<chuỗi bí mật bất kỳ>

USE_LLM=false          # false = không cần API | true = dùng GPT-4o-mini
ENABLE_MONITORING=false
```

### Bước 2 – Khởi động toàn bộ hệ thống

```bash
docker compose -f docker-compose.yml --env-file .env up -d
```

Kiểm tra tất cả service đã lên:

```bash
docker ps --filter "name=vnpt_" --format "table {{.Names}}\t{{.Status}}"
```

---

## Lấy dữ liệu (Data Commands)

### Seed lần đầu – nạp dữ liệu tĩnh vào Neo4j

Chạy sau khi `vnpt_neo4j` đã healthy lần đầu:

```bash
# 1. Seed nodes (Group, Topic, Problem, Answer)
docker cp seed_neo4j.cypher vnpt_neo4j:/var/lib/neo4j/import/seed_neo4j.cypher
docker exec vnpt_neo4j cypher-shell -u neo4j -p neo4jpassword \
    -f /var/lib/neo4j/import/seed_neo4j.cypher

# 2. Seed relationships (HAS_TOPIC, HAS_PROBLEM, HAS_ANSWER)
docker cp seed_rels.cypher vnpt_neo4j:/var/lib/neo4j/import/seed_rels.cypher
docker exec vnpt_neo4j cypher-shell -u neo4j -p neo4jpassword \
    -f /var/lib/neo4j/import/seed_rels.cypher
```

Kiểm tra dữ liệu đã vào:

```bash
docker exec vnpt_neo4j cypher-shell -u neo4j -p neo4jpassword \
    "MATCH (n) RETURN labels(n)[0] AS label, count(n) AS cnt ORDER BY cnt DESC"
# Kết quả mong đợi: ~462 Problem, ~462 Answer, ~156 Topic, ~4 Group

docker exec vnpt_neo4j cypher-shell -u neo4j -p neo4jpassword \
    "MATCH ()-[r]->() RETURN type(r) AS rel, count(r) AS cnt"
# Kết quả mong đợi: HAS_ANSWER 462, HAS_PROBLEM 458, HAS_TOPIC 156
```

### Crawl lại dữ liệu mới từ VNPTPay (one-shot)

```bash
docker compose -f docker-compose.yml --env-file .env \
    --profile oneshot up pipeline
```

Pipeline sẽ tự chạy đủ: crawl → clean → load Neo4j → export CSVs → backup MinIO → kết thúc.

### Trigger pipeline qua Airflow UI

```
1. Mở http://localhost:8080  (admin / admin)
2. Tìm DAG "vnpt_money_pipeline"
3. Bật toggle ON → nhấn nút ▶ Trigger DAG
```

### Xem dữ liệu trên Neo4j Browser

```cypher
-- Mở http://localhost:7474 (neo4j / neo4jpassword), chạy:
MATCH (g:Group)-[:HAS_TOPIC]->(t:Topic)-[:HAS_PROBLEM]->(p:Problem)-[:HAS_ANSWER]->(a:Answer)
RETURN g.name, count(p) AS problems
ORDER BY problems DESC
```

---

## Chạy AI Chatbot

### Chế độ không cần API (mặc định, `USE_LLM=false`)

Dùng **rule-based intent parser** + **Neo4j fulltext keyword search** + **template response**.
Không tốn phí, không cần internet. Hoạt động ngay sau khi data đã seed.

```bash
# Không cần thay đổi gì nếu .env đã có USE_LLM=false
# Kiểm tra chatbot đang healthy:
docker ps --filter "name=vnpt_chatbot" --format "{{.Names}}\t{{.Status}}"
```

Truy cập: **http://localhost:8000**

### Chế độ dùng GPT-4o-mini (`USE_LLM=true`)

Dùng **LLM intent parser** + **OpenAI vector embedding search** + **GPT-4o-mini response**.
Độ chính xác cao hơn, nhưng mỗi câu hỏi tốn ~2–4 giây và phí API.

```bash
# 1. Điền OPENAI_API_KEY và đổi USE_LLM trong .env:
#    OPENAI_API_KEY=sk-...
#    USE_LLM=true

# 2. Rebuild và restart chatbot:
docker compose -f docker-compose.yml --env-file .env \
    up -d --build --force-recreate chatbot
```

### So sánh hai chế độ

| Tính năng | `USE_LLM=false` | `USE_LLM=true` |
|-----------|:-----------:|:----------:|
| Intent parsing | Rule-based | GPT-4o-mini |
| Retrieval | Fulltext keyword | Vector (embedding) + keyword |
| Response sinh ra | Template có sẵn | GPT-4o-mini tổng hợp |
| Cần `OPENAI_API_KEY` | ✗ | ✓ |
| Tốc độ phản hồi | ~200 ms | ~2–4 s |
| Chi phí | Miễn phí | ~$0.001 / câu |

### Xem logs chatbot

```bash
docker logs vnpt_chatbot --tail 50 --follow
```

---

## Dừng và dọn dẹp

```bash
# Dừng tất cả, giữ nguyên dữ liệu (volumes)
docker compose -f docker-compose.yml --env-file .env down

# Dừng và xóa toàn bộ volumes (mất hết dữ liệu Neo4j, MinIO, Airflow!)
docker compose -f docker-compose.yml --env-file .env down -v
```

---

## Truy cập các Service

| Service | URL | Tài khoản mặc định |
|---------|-----|--------------------|
| **Chatbot** | http://localhost:8000 | — |
| **Neo4j Browser** | http://localhost:7474 | `neo4j` / `neo4jpassword` |
| **Airflow UI** | http://localhost:8080 | `admin` / `admin` |
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin` |
| **Grafana** | http://localhost:3000 | `admin` / `admin123` |
| **Prometheus** | http://localhost:9090 | — |

---

## Câu hỏi thường gặp

**Pipeline và Chatbot dùng chung Neo4j – có xung đột không?**
Không. Pipeline dùng `MERGE` (idempotent). Thời gian load < 30 giây, Chatbot vẫn query bình thường trong lúc đó.

**`ingest_data_v3.py` của Chatbot còn dùng không?**
Không cần trong unified setup. Giữ lại để AI team dev/test độc lập.

**Muốn chạy từng project riêng?**
Mỗi thư mục con (`chatbot/`, `data_pipeline/`) vẫn có `docker-compose.standalone.yml` để chạy độc lập.

---

## Tài nguyên hệ thống

> Bảng này thể hiện **giới hạn memory** được cấu hình trong `docker-compose.yml`.
> CPU không bị hard-limit. Tổng RAM khuyến nghị máy chủ: **≥ 8 GB**.

| Container | Image | Memory Limit | Memory Reserve | CPU (ước tính) | Vai trò |
|-----------|-------|:---:|:---:|:---:|---------|
| `vnpt_neo4j` | neo4j:5.19-community | **1 GB** | 512 MB | Trung bình | Graph DB – Heap 512 MB + Pagecache 256 MB |
| `vnpt_chatbot` | python:3.11-slim | **1 GB** | 256 MB | Thấp – Trung | Chainlit app; tăng khi `USE_LLM=true` |
| `vnpt_airflow_web` | python:3.11 (custom) | *(không limit)* | — | Thấp | Airflow Gunicorn webserver |
| `vnpt_airflow_scheduler` | python:3.11 (custom) | *(không limit)* | — | Thấp | DAG scheduling loop |
| `vnpt_airflow_worker` | python:3.11 (custom) | *(không limit)* | — | **Cao** (khi crawl) | Celery worker – spike khi chạy pipeline |
| `vnpt_prometheus` | prom/prometheus | **512 MB** | — | Rất thấp | Scrape metrics mỗi 15 s, retention 7 ngày |
| `vnpt_grafana` | grafana/grafana | **512 MB** | — | Rất thấp | Dashboard rendering |
| `vnpt_minio` | minio/minio | *(không limit)* | — | Rất thấp | Object storage – thực tế ~100–200 MB |
| `vnpt_redis` | redis:7-alpine | **256 MB** | — | Rất thấp | Celery broker (db=0) + session cache (db=1) |
| `vnpt_postgres` | postgres:15-alpine | *(không limit)* | — | Rất thấp | Airflow metadata DB – thực tế ~50 MB |
| `vnpt_pipeline` *(oneshot)* | python:3.11 (custom) | *(không limit)* | — | **Cao** (khi chạy) | Chỉ chạy khi `--profile oneshot`, tự thoát |

**Tổng memory limit được khai báo: ~3.25 GB**

**Khuyến nghị phần cứng tối thiểu:**

| Môi trường | RAM | CPU | Disk |
|------------|:---:|:---:|:---:|
| Development (local) | 8 GB | 4 cores | 20 GB |
| Production | 16 GB | 8 cores | 50 GB |
