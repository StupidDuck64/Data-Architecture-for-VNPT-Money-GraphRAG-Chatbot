\# VNPT Money Agent – Data Pipeline (MinIO + Neo4j + Airflow)

Tài liệu này được viết theo hướng **“hướng dẫn thao tác triển khai + diễn giải kỹ thuật tương ứng”** để có thể dùng trực tiếp trong báo cáo thực tập (Data Engineering/AI).

Hệ thống Data Pipeline có nhiệm vụ:

1. **Thu thập dữ liệu** từ trang trợ giúp VNPT Money: `https://vnptpay.vn/web/trogiup`
2. **Lưu dữ liệu thô** vào MinIO (S3-compatible Data Lake)
3. **Làm sạch/chuẩn hóa** (HTML→Markdown, normalize Unicode, loại nhiễu, dedup, chunking)
4. **Nạp dữ liệu** vào Neo4j theo mô hình Knowledge Graph (MERGE idempotent)
5. **Sao lưu** đồ thị Neo4j về MinIO
6. **Điều phối tự động** theo lịch chạy bằng Apache Airflow

---

## 1) Kiến trúc tổng quan

```text
vnptpay.vn/web/trogiup
        │
        ▼
┌──────────────────────┐      HTML/JSON      ┌──────────────────────────┐
│  VNPTScraper          │ ─── raw/ ────────►  │  MinIO Data Lake          │
│  - requests + BS4     │                     │  bucket: vnpt-datalake/   │
│  - Playwright fallback│                     │    raw/… (90 ngày)        │
└──────────┬───────────┘                     │    clean/… (365 ngày)     │
           │ structured dict                  │    backup/…               │
           ▼                                  └───────────▲──────────────┘
┌──────────────────────┐          JSON                    │
│  DataCleaner           │ ─── clean/ ─────────────────────┘
│  - HTML→Markdown       │
│  - Unicode NFC         │
│  - strip noise         │
│  - dedup, validate      │
│  - chunk + overlap      │
└──────────┬───────────┘
           │ cleaned dict
           ▼
┌──────────────────────┐     Cypher MERGE      ┌──────────────────────────┐
│  Neo4jLoader           │ ───────────────────► │  Neo4j (Graph DB)         │
│  - constraints/indexes │                     │  (:Group)-[:HAS_TOPIC]->  │
│  - batched upsert       │                     │  (:Topic)-[:HAS_PROBLEM]->│
└──────────┬───────────┘                     │  (:Problem)-[:HAS_ANSWER]->│
           │                                  │  (:Answer)                 │
           ▼                                  └───────────▲──────────────┘
┌──────────────────────┐                                  │
│  BackupManager         │ ─── backup/ ────────────────────┘
└──────────────────────┘

Orchestration:
  Apache Airflow DAG: vnpt_money_data_pipeline
  Schedule: Every Monday 02:00 (UTC)  (*xem lưu ý timezone ở dưới*)
```

---

## 2) Cấu trúc thư mục

```text
pipeline/
  crawlers/vnpt_scraper.py        # Web scraper (requests + Playwright)
  storage/minio_client.py         # MinIO client + lifecycle policy
  transforms/data_cleaner.py      # Làm sạch + chunking + dedup
  loaders/neo4j_loader.py         # Upsert graph (MERGE, batch)
  backup/backup_manager.py        # Export Neo4j → MinIO
  run_pipeline.py                 # Orchestrator (one-shot)

airflow/dags/vnpt_pipeline_dag.py # DAG điều phối từng stage

docker-compose.yml                # Neo4j, MinIO, Redis, Postgres, Airflow, oneshot pipeline
docker/Dockerfile.pipeline        # Image chạy pipeline
docker/Dockerfile.airflow         # Image chạy Airflow

external_data/                    # CSV mẫu (nodes_*, rels_*)
src/                              # Code chatbot core (retrieval/intent/metrics...)
requirements.pipeline.txt
requirements.txt
.env.example
```

---

## 3) Yêu cầu hệ thống

**Bắt buộc**
- Docker Desktop (Windows) + Docker Compose
- Khuyến nghị: RAM ≥ 8GB

**Tuỳ chọn (chạy local không Docker)**
- Python 3.11+
- Có thể cần cài Playwright browser: `playwright install chromium`

---

## 4) Cấu hình biến môi trường (.env)

### 4.1 Tạo file `.env`

```powershell
Copy-Item .env.example .env
```

### 4.2 Các biến bắt buộc

- **Neo4j**: `NEO4J_PASSWORD` (và URI nếu chạy local)
- **Airflow**: `AIRFLOW_FERNET_KEY`, `AIRFLOW_SECRET_KEY`

Sinh Fernet key:

```powershell
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

**Gợi ý**: `AIRFLOW_SECRET_KEY` có thể đặt là một chuỗi random đủ dài.

### 4.3 Biến tuỳ chọn

- `OPENAI_API_KEY`: chỉ cần nếu muốn tạo embeddings (vector search)
- `SLACK_WEBHOOK_URL`: chỉ cần nếu muốn DAG gửi notify (nếu bạn bật phần notify webhook)

---

## 5) Khởi chạy hệ thống bằng Docker Compose (khuyến nghị)

### 5.1 Bật các service nền tảng

Chạy Neo4j + MinIO + Redis + Postgres trước:

```powershell
docker-compose up -d neo4j minio minio-init redis postgres
```

Giải thích:
- `minio-init` sẽ **tạo bucket** `vnpt-datalake` (nếu chưa có).
- Neo4j có healthcheck, MinIO/Redis/Postgres cũng có healthcheck.

### 5.2 Khởi tạo Airflow (migrate DB + tạo admin user)

```powershell
docker-compose up -d airflow-init
```

### 5.3 Chạy Airflow webserver/scheduler/worker

```powershell
docker-compose up -d airflow-webserver airflow-scheduler airflow-worker
```

### 5.4 Truy cập giao diện

| Thành phần | URL | Ghi chú |
|---|---|---|
| Airflow | http://localhost:8080 | user/pass lấy từ `.env` (mặc định admin/admin) |
| MinIO Console | http://localhost:9001 | mặc định minioadmin/minioadmin |
| Neo4j Browser | http://localhost:7474 | user `neo4j`, pass từ `.env` |

---

## 6) Chạy pipeline “one-shot” (không cần Airflow)

Cách này phù hợp để demo/thử nghiệm nhanh.

### 6.1 Chạy pipeline trong container

```powershell
docker-compose --profile oneshot run --rm pipeline
```

Pipeline sẽ thực hiện theo thứ tự:
1) Crawl → 2) Save raw (MinIO) → 3) Clean → 4) Save clean (MinIO) → 5) Load Neo4j → 6) Backup Neo4j (MinIO)

### 6.2 Chạy với Playwright (nếu trang render JS)

Trong Docker image, Playwright Chromium đã được cài; bạn chỉ cần truyền flag khi chạy local. Với oneshot container, pipeline tự fallback nếu static HTML rỗng.

### 6.3 Xoá graph trước khi nạp (tuỳ chọn)

Nếu chạy local trực tiếp script (mục 7), bạn có thể dùng `--clear` để xoá trước khi nạp.

---

## 7) Chạy pipeline local (không Docker) – phục vụ phát triển

> Dùng khi bạn muốn debug nhanh từng module Python.

### 7.1 Cài dependencies

```powershell
pip install -r requirements.pipeline.txt
```

Nếu cần Playwright:

```powershell
playwright install chromium
```

### 7.2 Chạy pipeline

```powershell
python pipeline/run_pipeline.py
```

Các tuỳ chọn:

```powershell
# Render JS bằng Playwright
python pipeline/run_pipeline.py --playwright

# Xoá toàn bộ Neo4j trước khi nạp
python pipeline/run_pipeline.py --clear

# Bỏ qua bước backup
python pipeline/run_pipeline.py --skip-backup
```

---

## 8) Vận hành định kỳ bằng Airflow

### 8.1 Cơ chế DAG

DAG: `vnpt_money_data_pipeline` được triển khai theo pattern:

`generate_run_id → crawl → clean_and_save → load_neo4j → backup → notify`

Nguyên tắc vận hành:
- **XCom chỉ truyền “key nhẹ”** (run_id, MinIO object keys, stats), không truyền JSON nặng để tránh phình DB Airflow.
- Nếu MinIO tạm thời không khả dụng ở stage clean, có nhánh fallback “re-crawl” (last resort).

### 8.2 Lịch chạy

Trong DAG hiện tại, lịch là `0 2 * * 1` (02:00 **UTC** mỗi Thứ Hai). Nếu bạn báo cáo theo giờ Việt Nam, nên ghi rõ quy đổi: 02:00 UTC ≈ 09:00 giờ Việt Nam (UTC+7).

### 8.3 Trigger thủ công

1) Vào Airflow UI → tìm DAG `vnpt_money_data_pipeline`
2) Toggle ON
3) Bấm **Trigger DAG**

---

## 9) Chi tiết kỹ thuật từng stage (để đưa vào báo cáo)

### 9.1 Stage Crawl – chiến lược thu thập đa tầng

Mục tiêu: lấy nội dung FAQ/trợ giúp từ `vnptpay.vn/web/trogiup` và chuẩn hoá về schema gồm:

- `groups`, `topics`, `problems`, `answers`
- `rels_has_topic`, `rels_has_problem`, `rels_has_answer`

Chiến lược 3 lớp:
1) **Static HTML (requests + BeautifulSoup)**: nhanh, ít tài nguyên.
2) **JS rendering (Playwright headless Chromium)**: kích hoạt khi HTML tĩnh không có nội dung mong đợi.
3) **Plain-text fallback**: trích xuất text dump nếu không nhận diện được cấu trúc.

Ngoài ra, pipeline gắn metadata truy vết (`source_url`, `crawled_at`) cho từng Problem/Answer để phục vụ kiểm chứng và debug.

### 9.2 Stage Raw Storage – Data Lake trên MinIO

Bucket mặc định: `vnpt-datalake`.

Layout (phân vùng theo thời gian + run_id):

```text
vnpt-datalake/
  raw/YYYY/MM/DD/HH/<run_id>/
    page.html
    raw_data.json
  clean/YYYY/MM/DD/HH/<run_id>/
    cleaned_data.json
  backup/<run_id>/
    neo4j_export.json
```

Lifecycle policy tự áp dụng khi tạo bucket:
- `raw/` hết hạn sau **90 ngày**
- `clean/` hết hạn sau **365 ngày**

### 9.3 Stage Clean – làm sạch, chuẩn hoá, chunking

Các bước xử lý chính:
- **HTML → Markdown** (giữ cấu trúc danh sách/heading để downstream LLM dễ hiểu)
- **Chuẩn hoá Unicode (NFC)** + sửa một số lỗi encoding thường gặp
- **Collapse whitespace** và loại dòng nhiễu (menu/footer/copyright/URL rời)
- **Dedup theo `id`** để đảm bảo tính idempotent
- **Chunking Answer dài** (mặc định > 1500 ký tự):
  - Tách theo paragraph/sentence ưu tiên ngữ nghĩa
  - Có **overlap ~100 ký tự** giữa các chunk để không “đứt” ngữ cảnh
  - Sinh thêm các quan hệ `rels_has_answer` để Problem trỏ tới từng chunk

### 9.4 Stage Load – nạp Knowledge Graph vào Neo4j

Node labels:

```text
(:Group {id, name, description, order})
(:Topic {id, name, group_id, keywords, order})
(:Problem {id, title, description, intent, keywords, sample_questions, status, source_url, crawled_at})
(:Answer {id, summary, content, steps, notes, status, source_url, crawled_at, ...})
```

Relationships:

```text
(:Group)-[:HAS_TOPIC]->(:Topic)-[:HAS_PROBLEM]->(:Problem)-[:HAS_ANSWER]->(:Answer)
```

Nguyên tắc nạp:
- Dùng `MERGE (n:Label {id: ...})` → chạy lặp không tạo trùng.
- Ghi theo batch để tăng tốc.
- Tạo constraint unique theo `id` và tạo fulltext indexes phục vụ fallback search.

### 9.5 Stage Backup – sao lưu Neo4j về MinIO

Trước mỗi lần cập nhật, export graph ra JSON và lưu vào:

`backup/<run_id>/neo4j_export.json`

Mục tiêu: hỗ trợ rollback khi có lỗi dữ liệu, hoặc so sánh chênh lệch giữa các lần chạy.

---

## 10) Kiểm tra kết quả sau khi chạy

### 10.1 Kiểm tra MinIO

1) Vào MinIO Console: http://localhost:9001
2) Mở bucket `vnpt-datalake`
3) Kiểm tra có thư mục `raw/`, `clean/`, `backup/` theo thời gian

### 10.2 Kiểm tra Neo4j bằng Cypher (Neo4j Browser)

Ví dụ đếm số node:

```cypher
MATCH (g:Group)   RETURN count(g) AS groups;
MATCH (t:Topic)   RETURN count(t) AS topics;
MATCH (p:Problem) RETURN count(p) AS problems;
MATCH (a:Answer)  RETURN count(a) AS answers;
```

Ví dụ kiểm tra một đường dẫn Group→Topic→Problem→Answer:

```cypher
MATCH (g:Group)-[:HAS_TOPIC]->(t:Topic)-[:HAS_PROBLEM]->(p:Problem)-[:HAS_ANSWER]->(a:Answer)
RETURN g.name, t.name, p.title, a.summary
LIMIT 10;
```

---

## 11) Backup/Restore (thao tác phục vụ báo cáo)

Ví dụ chạy bằng Python (local):

```python
from pipeline.backup.backup_manager import BackupManager

mgr = BackupManager()
manifest = mgr.run_backup("my_run_id")
mgr.restore_backup("my_run_id", clear_existing=True)
keys = mgr.list_backups()
```

---

## 12) (Tuỳ chọn) Embeddings & Vector Search

### 12.1 Khi nào cần embeddings?

Embeddings dùng để vector search (cosine similarity) trên `Problem.embedding`. Nếu bạn chưa cấu hình `OPENAI_API_KEY`, hệ thống vẫn chạy được với cơ chế fulltext fallback ở tầng chatbot.

### 12.2 Tạo embeddings và vector index

- Mô hình mặc định: `text-embedding-3-small`
- Số chiều: 1536
- Vector index Neo4j: `problem_embedding_index`

Lưu ý: vector index được tạo bằng Cypher `CREATE VECTOR INDEX ... vector.dimensions=1536, vector.similarity_function='cosine'`.

---

## 13) Troubleshooting (lỗi thường gặp)

1) **Neo4j login thất bại**
   - Kiểm tra `NEO4J_PASSWORD` trong `.env` khớp với compose.

2) **MinIO không thấy bucket `vnpt-datalake`**
   - Đảm bảo service `minio-init` đã chạy xong (status completed successfully).

3) **Không crawl được nội dung do trang render JS**
   - Chạy với Playwright (local) hoặc để pipeline tự fallback (Docker image đã có Chromium).

4) **Vector search báo thiếu index**
   - Bạn cần chạy bước tạo vector index/embeddings trước khi gọi `db.index.vector.queryNodes`.


loader = Neo4jLoader()
loader.generate_embeddings(model="text-embedding-3-small")
loader.close()
```

Requires `OPENAI_API_KEY` in `.env`.

---

## Extending the Pipeline

| Goal | Where to edit |
|------|--------------|
| Change crawl schedule | `airflow/dags/vnpt_pipeline_dag.py` → `schedule_interval` |
| Add new selectors | `pipeline/crawlers/vnpt_scraper.py` → `_parse_strategy_accordion()` |
| Change cleaning rules | `pipeline/transforms/data_cleaner.py` → `_NOISE_PATTERNS` |
| Add new node labels | `pipeline/loaders/neo4j_loader.py` + extended Cypher |
| Slack / email alerts | `airflow/dags/vnpt_pipeline_dag.py` → `_notify()` |
