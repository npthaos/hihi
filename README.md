#  FiNTA — Financial Sentiment & Real-time Stock Analytics

> **Hệ thống Big Data** tự động thu thập, phân tích tâm lý thị trường từ báo chí và mạng xã hội, kết hợp với dữ liệu giao dịch thực tế để cung cấp dashboard trực quan và cảnh báo thời gian thực cho thị trường chứng khoán Việt Nam.

---

## 📋 Mục lục

- [Giới thiệu](#-giới-thiệu)
- [Demo](#-demo)
- [Kiến trúc hệ thống](#-kiến-trúc-hệ-thống)
- [Tính năng chính](#-tính-năng-chính)
- [Công nghệ sử dụng](#-công-nghệ-sử-dụng)
- [Cài đặt & Khởi chạy](#-cài-đặt--khởi-chạy)
- [Cấu trúc thư mục](#-cấu-trúc-thư-mục)
- [Database Schema](#-database-schema)
- [API Reference](#-api-reference)
- [Workflow vận hành](#-workflow-vận-hành)
- [Giám sát hệ thống](#-giám-sát-hệ-thống)
- [Kiểm thử](#-kiểm-thử)
- [Troubleshooting](#-troubleshooting)
- [Roadmap](#-roadmap)

---

## 📖 Giới thiệu

### Bài toán

Nhà đầu tư trên thị trường chứng khoán Việt Nam phải xử lý lượng thông tin khổng lồ, phân mảnh từ nhiều nguồn:

- **Báo chí tài chính**: CafeF, Vietstock, VnExpress cập nhật liên tục.
- **Mạng xã hội**: Thảo luận trên FireAnt, bình luận video YouTube về chứng khoán.
- **Dữ liệu thị trường**: Biến động giá, khối lượng giao dịch, chỉ báo kỹ thuật.

Việc theo dõi thủ công và phát hiện mối liên hệ giữa **tâm lý cộng đồng** và **biến động giá** là không khả thi, dẫn đến quyết định đầu tư thiếu cơ sở hoặc trễ nhịp thị trường.

### Giải pháp

**FiNTA** xây dựng pipeline Big Data theo mô hình Lambda/Kappa, xử lý đồng thời dữ liệu batch và streaming qua 6 tầng:

| Tầng | Chức năng |
|------|-----------|
| **Ingestion** | Thu thập tự động từ báo chí, FireAnt, YouTube, API thị trường |
| **Messaging** | Apache Kafka đệm và phân phối dữ liệu đầu vào |
| **Lakehouse** | MinIO lưu trữ dữ liệu theo 3 tầng Bronze → Silver → Gold |
| **Processing & NLP** | Spark xử lý + PhoBERT/ViSoBERT phân tích cảm xúc tiếng Việt |
| **Serving** | ClickHouse + Redis phục vụ truy vấn tốc độ cao |
| **Presentation** | Dashboard React hiển thị trực quan, cảnh báo thời gian thực |

---

## 🖥️ Demo

![Trang Tổng quan](images/dashboard-overview1.png)
![Trang Tổng quan](images/dashboard-overview.png)
![Trang Tổng quan](images/dashboard-2.png)
![Trang Tìm kiếm](images/search_page.png)
![Trang Tin Thị trường](images/news.png)
![Trang Cảnh báo](images/alert-center.png)
![Trang Chủ đề](images/hot_topic.png)


---

## 🏗️ Kiến trúc hệ thống

![Kiến trúc tổng thể của hệ thống FiNTA. Hệ thống gồm 5 tầng: (1)thu thập dữ liệu, (2) hàng đợi Kafka, (3) xử lý streaming với Spark, (4) suy luận AI với PhoBERT, và (5) phục vụ và trực quan hóa.](images/ArchitectureDiagramBD.drawio.png)

### Luồng dữ liệu chi tiết

**Luồng Content/Sentiment:**
```
Báo chí / MXH → Kafka → Bronze Parquet (MinIO)
                              ↓ Spark Streaming
                         Silver (làm sạch, tách từ, nhận diện ticker)
                              ↓ Spark Batch + NLP API
                         Gold fact_entity_sentiment_daily
```

**Luồng Market Data:**
```
vnstock / FireAnt API → JSONL → Great Expectations Validation
                                      ↓ Spark Job
                              Bronze/Silver → Gold fact_market_daily
                                      ↓ JOIN với sentiment
                              fact_entity_market_sentiment_daily
```

**Luồng Serving:**
```
Gold (Trino) → ClickHouse → Redis Cache → FastAPI Backend → React Dashboard
                          └─────────────→ Alert Dispatcher → Webhook
```

---

## ✨ Tính năng chính

### Thu thập dữ liệu đa nguồn
- Crawler tự động tin tức CafeF, Vietstock, VnExpress mỗi **5 phút** trong phiên giao dịch.
- Thu thập bình luận YouTube từ các video phân tích chứng khoán hàng ngày.
- Ingest posts và blog từ FireAnt.

### Phân tích kỹ thuật thị trường
Tự động tính toán các chỉ báo cho toàn bộ watchlist VN30 + VNINDEX:

| Nhóm chỉ báo | Chỉ tiêu cụ thể |
|---|---|
| Xu hướng | SMA 20/50, EMA 12/26 |
| Động lượng | MACD (Signal, Histogram), RSI 14 |
| Biến động | Bollinger Bands, Volatility 5D |
| Hiệu suất | Return 1D/3D/5D, Range Pct, Volume Z-Score |

### Phân tích cảm xúc bằng AI (NLP)
- **PhoBERT**: Phân tích sentiment văn bản báo chí chính thống.
- **ViSoBERT**: Phân tích sentiment mạng xã hội tiếng Việt không chuẩn.
- Phân loại tâm lý: **Bullish / Bearish / Neutral**.
- Gán nhãn chủ đề: vĩ mô, kết quả kinh doanh, cổ tức, kỹ thuật, giao dịch khối ngoại...
- Cơ chế **fallback Rule-based** tự động khi NLP API gặp sự cố.

### Hệ thống cảnh báo thị trường
| Loại cảnh báo | Điều kiện |
|---|---|
| RSI Oversold | RSI < 30 |
| RSI Overbought | RSI > 70 |
| MACD Crossover | MACD cắt đường Signal |
| Volume Spike | Volume Z-Score > 2.0 |
| Bollinger Breakout | Giá vượt Upper Band hoặc thủng Lower Band |

### Dashboard trực quan
- **Sector Heatmap**: Bản đồ nhiệt cảm xúc theo ngành.
- **Sentiment-Price Chart**: Biểu đồ tương quan giá cổ phiếu và điểm sentiment.
- **Live News Feed**: Tin tức phân loại màu sắc theo cảm xúc.
- **Alerts Center**: Trung tâm theo dõi tín hiệu cảnh báo thị trường.

---

## 🛠️ Công nghệ sử dụng

| Tầng | Công nghệ | Phiên bản | Mục đích |
|------|-----------|-----------|----------|
| **Ingestion** | Python, BeautifulSoup4, APScheduler | — | Crawler báo chí, FireAnt |
| | YouTube Data API v3 | — | Thu thập bình luận YouTube |
| | `vnstock` | 4.0.4 | Dữ liệu OHLCV thị trường VN |
| **Messaging** | Apache Kafka (KRaft) | 7.4.0 | Message broker, buffer dữ liệu |
| **Lakehouse** | MinIO | Latest | Object storage S3-compatible |
| **Processing** | Apache Spark | 3.5.1 | Batch & Streaming xử lý song song |
| **Orchestration** | Apache Airflow | 2.9.3 | Điều phối lịch chạy pipeline |
| **NLP AI** | PhoBERT, ViSoBERT, HuggingFace | — | Phân tích cảm xúc tiếng Việt |
| | FastAPI, PyTorch | — | Model serving API |
| **Data Quality** | Great Expectations | 1.18.0 | Kiểm định chất lượng dữ liệu |
| **Serving DB** | ClickHouse | 24.8 | OLAP columnar DB tốc độ cao |
| **Cache** | Redis | Alpine | Cache realtime & alert state |
| **Query Engine** | Trino | 443 | SQL trên Lakehouse (Hive catalog) |
| **Backend** | FastAPI | — | REST API cho Dashboard |
| **Frontend** | React, TypeScript, Vite, TailwindCSS, Recharts | — | Giao diện người dùng |
| **BI** | Apache Superset | 4.0.2 | Báo cáo & biểu đồ bổ sung |
| **Monitoring** | Prometheus, Grafana, cAdvisor | — | Giám sát hệ thống |

---

## 🚀 Cài đặt & Khởi chạy

### Yêu cầu hệ thống

| Thành phần | Tối thiểu | Khuyến nghị |
|---|---|---|
| CPU | 8 cores | 12+ cores |
| RAM | 16 GB | **32 GB** |
| Disk | 50 GB SSD | 100 GB SSD |
| OS | Ubuntu 20.04 / macOS | Ubuntu 22.04 LTS |

**Phần mềm cần thiết:**
- Docker Engine v20.10+ & Docker Compose v2.20+
- Python v3.10+
- Node.js v18+ & npm v9+

---

### Bước 1 — Clone repository

```bash
git clone https://github.com/mihuyen/FINTA-BigData
cd FINTA-BigData
```

### Bước 2 — Cấu hình môi trường

```bash
cp .env.example .env
```

Mở `.env` và điền các thông số bắt buộc:

| Biến | Mô tả |
|------|-------|
| `YOUTUBE_API_KEY` | Lấy từ Google Cloud Console |
| `FIREANT_ACCESS_TOKEN` | Token truy cập FireAnt API |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | Credentials MinIO |
| `MARKET_TICKER_GROUPS` | Ví dụ: `VN30` |
| `HF_TOKEN` | HuggingFace token để tải PhoBERT/ViSoBERT |

### Bước 3 — Khởi chạy hạ tầng Docker

```bash
# Di chuyển tới thư mục chứa docker-compose.yml
cd ..

# Khởi chạy toàn bộ cluster
docker compose up -d
```

Các dịch vụ chính sau khi khởi chạy:

| Dịch vụ | Cổng | Giao diện |
|---------|------|-----------|
| Airflow | `8082` | [http://localhost:8082](http://localhost:8082) |
| MinIO Console | `9001` | [http://localhost:9001](http://localhost:9001) |
| Spark Master UI | `8080` | [http://localhost:8080](http://localhost:8080) |
| Grafana | `3000` | [http://localhost:3000](http://localhost:3000) |
| NLP API | `8002` | [http://localhost:8002/docs](http://localhost:8002/docs) |
| Trino UI | `8081` | [http://localhost:8081](http://localhost:8081) |
| Apache Superset | `8088` | [http://localhost:8088](http://localhost:8088) |

> **Tài khoản mặc định:** `admin` / `admin` (Airflow, Grafana, Superset); MinIO: `admin` / `password123`

### Bước 4 — Khởi tạo ClickHouse

```bash
chmod +x scripts/deploy_clickhouse.sh
./scripts/deploy_clickhouse.sh
```

### Bước 5 — Đăng ký bảng Gold trong Trino

```bash
docker exec -i finta-trino trino < orchestration/trino/sql/register_gold_tables.sql
```

Kiểm tra bảng đã đăng ký:
```bash
docker exec finta-trino trino --execute "SHOW TABLES FROM finta.gold"
```

### Bước 6 — Cài đặt và chạy Dashboard

**Backend:**
```bash
cd backend-dashboard
python3 -m venv venv && source venv/bin/activate
pip install -r app/requirements.txt
```

**Frontend:**
```bash
cd frontend
npm install && npm run build
```

**Khởi chạy (chọn một trong hai cách):**

```bash
# Production mode — Backend phục vụ cả frontend tĩnh tại :8000
./scripts/run_dashboard.sh

# Development mode — Vite dev server tại :5174, HMR hỗ trợ
./scripts/run_dashboard_5174.sh
```

Dashboard: [http://localhost:8000](http://localhost:8000) (production) hoặc [http://localhost:5174](http://localhost:5174) (dev)

---

## 📂 Cấu trúc thư mục

```
FINTA-BigData/
├── backend-dashboard/        # FastAPI REST API cho Dashboard
├── frontend/                 # React / Vite SPA
├── ingestion/                # Thu thập dữ liệu
│   ├── crawl/                # Crawler CafeF, Vietstock, VnExpress
│   ├── kafka_producer/       # Producer đẩy data vào Kafka
│   └── market/               # Loader dữ liệu OHLCV (vnstock, FireAnt)
├── nlp/                      # NLP Pipeline
│   ├── sentiment_model/      # Huấn luyện PhoBERT sentiment
│   ├── social_sentiment_model/ # Huấn luyện ViSoBERT (mạng xã hội)
│   ├── topic_model/          # Phân loại chủ đề tài chính
│   └── serving/              # FastAPI NLP inference server
├── spark_processing/         # Spark jobs
│   ├── bronze_kafka_spark_job.py   # Kafka → Bronze
│   ├── unified_silver_spark_job.py # Bronze → Silver
│   ├── gold_spark_job.py           # Silver + NLP → Gold
│   └── market_spark_job.py         # Indicators + Alert signals
├── orchestration/
│   ├── airflow/dags/         # Airflow DAGs điều phối pipeline
│   ├── trino/                # Trino catalog config & DDL
│   └── superset/             # Superset config
├── quality/                  # Great Expectations validation
├── serving/                  # Sync ClickHouse, Redis cache, Webhook alerts
├── deploy/                   # Docker, systemd, ClickHouse schema
├── scripts/                  # Scripts tiện ích khởi chạy
├── docs/                     # Tài liệu kiến trúc chi tiết
├── data/                     # Dữ liệu local (gitignored)
└── checkpoints/              # Trọng số mô hình NLP (gitignored)
```

---

## 🗄️ Database Schema

### Bảng Gold trong Trino (Lakehouse)

| Bảng | Mô tả |
|------|-------|
| `dim_content` | Metadata bài viết (tiêu đề, URL, tác giả, thời gian) |
| `dim_entity` | Danh mục mã cổ phiếu, sàn giao dịch, phân ngành |
| `dim_source` | Danh mục nguồn báo chí và mạng xã hội |
| `dim_date` | Lịch giao dịch |
| `fact_entity_sentiment` | Sentiment chi tiết từng lượt đề cập ticker |
| `fact_entity_sentiment_daily` | Tổng hợp sentiment theo ticker × ngày × nguồn |
| `fact_market_daily` | OHLCV + chỉ báo kỹ thuật theo ticker × ngày |
| `fact_market_alert_signal` | Lịch sử tín hiệu cảnh báo đã kích hoạt |
| `fact_entity_market_sentiment_daily` | JOIN giá cổ phiếu + sentiment + phản ứng giá |

### Đồng bộ sang ClickHouse

```bash
# Chạy thủ công (thông thường do Airflow tự điều phối)
docker exec -it finta-airflow-scheduler bash -lc \
  'cd /opt/airflow/project && python serving/sync_clickhouse_from_trino.py'
```

---

## 📡 API Reference

Tài liệu Swagger đầy đủ: [http://localhost:8000/docs](http://localhost:8000/docs)

### Dashboard API (`/api/*`)

| Method | Endpoint | Mô tả |
|--------|----------|-------|
| `GET` | `/api/market-index-summary` | Thông số VNINDEX, VN30 và đồ thị giá ngắn hạn |
| `GET` | `/api/market-movers` | Top tăng mạnh / giảm mạnh / volume đột biến |
| `GET` | `/api/sentiment-timeline` | Xu hướng điểm sentiment theo ngày |
| `GET` | `/api/sector-heatmap` | Dữ liệu heatmap cảm xúc theo ngành |
| `GET` | `/api/ticker-detail` | Hồ sơ doanh nghiệp, giá hiện tại, chỉ báo kỹ thuật |
| `GET` | `/api/ticker-market-series` | Lịch sử giá + sentiment phục vụ biểu đồ |
| `GET` | `/api/live-news` | Tin tức nóng phân loại theo sentiment thời gian thực |
| `GET` | `/api/news-page` | Danh sách tin tức phân trang, lọc theo ticker/sentiment |
| `GET` | `/api/alerts-page` | Trung tâm cảnh báo (RSI, MACD, Bollinger, Volume) |
| `GET` | `/api/topics` | Bảng xếp hạng chủ đề được thảo luận nhiều nhất |
| `GET` | `/api/search` | Tìm kiếm mã cổ phiếu và tin tức |

### NLP API (`http://localhost:8002`)

| Method | Endpoint | Mô tả |
|--------|----------|-------|
| `GET` | `/health` | Trạng thái PhoBERT / fallback mode |
| `POST` | `/predict/sentiment` | Sentiment một văn bản |
| `POST` | `/predict/topic` | Phân loại chủ đề một văn bản |
| `POST` | `/predict/full` | Sentiment + topic đồng thời |
| `POST` | `/predict/batch` | Batch inference tối đa 32 văn bản (dùng bởi Spark Gold) |

---

## ⚙️ Workflow vận hành

Toàn bộ pipeline được điều phối tự động bởi **Apache Airflow**:

### DAG 1: `finta_mini_batch_pipeline`
**Lịch chạy:** Mỗi 10 phút (`*/10 * * * *`)

```
schedule_guard
    └── check_kafka_topics
        ├── ensure_bronze_kafka_stream      (Kafka → Bronze Parquet)
        ├── ensure_unified_silver_stream    (Bronze → Silver)
        ├── youtube_crawl_run_once          (YouTube → Kafka)
        ├── spark_gold_inference            (Silver + NLP API → Gold)
        ├── spark_gold_dimensions           (Cập nhật dim tables)
        └── sync_gold_partition_metadata    (Trino SYNC)
```

### DAG 2: `finta_market_pipeline`
**Lịch chạy:** Mỗi 15 phút, 8h–17h, Thứ Hai–Thứ Sáu (`*/15 8-17 * * 1-5`)

```
market_ingestion                    (Tải OHLCV từ vnstock/FireAnt)
    └── validate_market_raw_with_gx (Great Expectations validation)
        └── spark_market_processing  (Tính indicator, tạo alert signals, JOIN sentiment)
            └── sync_market_partition_metadata
                └── ensure_clickhouse_schema
                    └── sync_clickhouse_serving
                        ├── update_redis_market_cache
                        └── dispatch_market_notifications
```

---

## 📊 Giám sát hệ thống

Hệ thống giám sát qua **Prometheus + Grafana**:

| Nguồn metrics | Thông tin thu thập |
|---|---|
| FastAPI Backend (`/metrics`) | Độ trễ API, số request, mã HTTP status |
| Airflow Pushgateway (`:9091`) | Tiến độ và trạng thái task |
| cAdvisor (`:8085`) | CPU, RAM từng container Docker |

Truy cập **Grafana** tại [http://localhost:3000](http://localhost:3000) → Dashboards → **FiNTA Observability** để xem:
- Request throughput và p95 latency.
- Mức tiêu thụ CPU/RAM theo container.
- Trạng thái lỗi các task Airflow.
- Data freshness lag (độ tươi dữ liệu).

---

## 🧪 Kiểm thử

### Kiểm định chất lượng dữ liệu (Great Expectations)

```bash
docker exec finta-airflow-scheduler bash -lc \
  'cd /opt/airflow/project && python quality/market_gx_validation.py --fail-on-error'
```

Kết quả JSON tại `data/quality/market/`.

### Kiểm tra kết nối Trino

```bash
docker exec -it finta-airflow-scheduler \
  python /opt/airflow/project/backend-dashboard/test_trino_conn.py
```

### Xác thực dữ liệu Gold Parquet

```bash
docker exec finta-spark-master /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/ivy-spark \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark/work-dir/spark_processing/verify_gold_output.py
```

---

## 🔧 Troubleshooting

| Sự cố | Nguyên nhân | Cách khắc phục |
|-------|-------------|----------------|
| **Lỗi kết nối Trino** | `TRINO_HOST` cấu hình sai | Kiểm tra `backend-dashboard/app/config.py`, đảm bảo `TRINO_HOST=localhost` và port `8081` |
| **Spark Worker OOM (Exit 137)** | Worker vượt giới hạn RAM Docker | Giảm `SPARK_WORKER_MEMORY` xuống `3g` hoặc `4g` trong `docker-compose.yml`, tăng swap cho host |
| **Không crawl được YouTube** | `YOUTUBE_API_KEY` sai hoặc hết quota | Kiểm tra key trong `.env`, restart `finta-airflow-scheduler` |
| **Airflow lỗi Docker socket** | User Airflow (UID 50000) không có quyền | Chạy `sudo chmod 666 /var/run/docker.sock` trên host |
| **vnstock báo rate limit** | Quá nhiều request đồng thời | Tăng `MARKET_DAILY_CHUNK_SLEEP_SECONDS=70`, giảm `MARKET_DAILY_CHUNK_SIZE=10` trong `.env` |
| **NLP API fallback / PhoBERT lỗi** | Chưa có checkpoint mô hình | Khai báo `HF_TOKEN` trong `.env`, đảm bảo máy có internet để tải từ HuggingFace Hub |

---

## 17. Kế hoạch phát triển (Roadmap)

1. **Hướng thứ nhất – Cải thiện độ chính xác cho nguồn YouTube:** Fine-tune mô hình ASR trên dữ liệu tài chính tiếng Việt (khoảng 100 giờ video có phụ đề) để giảm lỗi nhận dạng từ chuyên ngành. Đồng thời, kết hợp transcript với metadata (title, description) để cải thiện độ tin cậy.
2. **Hướng thứ hai – Mở rộng sang dữ liệu đa phương thức:** Tích hợp thị giác máy tính để phân tích biểu đồ giá, biểu đồ kỹ thuật, và hình ảnh trong bài đăng FireAnt. Các mô hình đa phương thức như CLIP hoặc ViLT có thể được fine-tune để trích xuất tín hiệu cảm xúc từ hình ảnh.
3. **Hướng thứ ba – Phát hiện biệt danh tự động:** Xây dựng một pipeline unsupervised learning để phát hiện các biệt danh mới từ dữ liệu FireAnt, sử dụng kỹ thuật phân cụm từ (word clustering) dựa trên ngữ cảnh xuất hiện.
4. **Hướng thứ tư – Giảm độ trễ AI inference:** Áp dụng các kỹ thuật tối ưu hóa mô hình như quantization (giảm độ chính xác số học từ FP32 xuống INT8) hoặc pruning (cắt bỏ các trọng số không quan trọng) để giảm thời gian suy luận của PhoBERT, có thể cắt giảm 30-50% độ trễ mà không ảnh hưởng đáng kể đến độ chính xác.

---

## 👥 Nhóm phát triển

Hệ thống được phát triển bởi:
* **Nguyễn Xuân Trường Khải**
* **Vương Thuỳ Linh**
* **Nguyễn Đức Mạnh**
* **Bùi Huyền Mi**
* **Nguyễn Phương Thảo**

---
