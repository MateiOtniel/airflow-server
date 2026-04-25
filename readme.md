# Airflow Bank Data Pipeline

Pipeline de date bancare orchestrat cu Apache Airflow, care procesează date zilnice prin Spark și le scrie în BigQuery. Include atât joburi batch (CSV din GCS), cât și joburi de streaming (Spark Structured Streaming cu stream-stream join).

---

## Arhitectura generala

```
┌──────────────────────────────────────────────────────────────────────┐
│                         Google Cloud Storage                         │
│                                                                      │
│  sales/sales_YYYY_MM_DD.csv        loans/loans_YYYY_MM_DD.csv       │
│  accounts/accounts_YYYY_MM_DD.csv  delay_fees/delay_fees_*.csv      │
│  orders/date=YYYY-MM-DD/*.csv      transactions/date=YYYY-MM-DD/*.csv│
│  clients/date=YYYY-MM-DD/*.csv                                       │
└───────────────────────┬──────────────────────────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     Apache Airflow (Docker)                          │
│                                                                      │
│  Webserver  ·  Scheduler  ·  Worker (Celery)  ·  Flower             │
│                                                                      │
│  DAGs:  daily_sales  ·  daily_loan_analytics                        │
│         daily_spending_analytics                                     │
│         daily_orders  ·  daily_transactions                          │
└───────────────────────┬──────────────────────────────────────────────┘
                        │  SparkSubmitOperator
                        ▼
┌──────────────────────────────────────────────────────────────────────┐
│                      PySpark Jobs                                    │
│                                                                      │
│  Batch:     daily_sales.py  ·  daily_loan_analytics.py              │
│             daily_spending_analytics.py                              │
│                                                                      │
│  Streaming: daily_orders_total.py                                    │
│             daily_transactions_with_clients.py                       │
└───────────────────────┬──────────────────────────────────────────────┘
                        │  BigQuery Connector (direct write)
                        ▼
┌──────────────────────────────────────────────────────────────────────┐
│               BigQuery  (dataset: bank_raw_daily_ingest_analytics)   │
│                                                                      │
│  daily_sales  ·  daily_loan_analytics  ·  daily_spending_analytics  │
│  daily_streaming_orders  ·  daily_transaction_totals                 │
└──────────────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────┐
│  Cloud SQL (PostgreSQL) via proxy   │
│  Tabela dag_metadata – audit log    │
└─────────────────────────────────────┘
```

---

## Stack tehnologic

| Componenta | Versiune |
|---|---|
| Apache Airflow | 2.10.5 |
| Python | 3.11 |
| PySpark | ≥ 3.1.3 |
| Executor | CeleryExecutor + Redis |
| Metadate Airflow | PostgreSQL 15 (local Docker) |
| Audit log DAG-uri | Cloud SQL (PostgreSQL) via Cloud SQL Proxy |
| Stocare date brute | Google Cloud Storage |
| Destinatie analize | BigQuery |
| Notificari | Gmail SMTP → EmailOperator |

---

## Structura proiectului

```
airflow-server/
├── dags/                          # definitii DAG-uri Airflow
│   ├── daily_loan_analytics.py
│   ├── daily_orders.py
│   ├── daily_sales.py
│   ├── daily_spending_analytics.py
│   ├── daily_transactions.py
│   └── helpers/
│       └── logging.py             # callback audit → Cloud SQL
│
├── scripts/
│   ├── spark_jobs/
│   │   └── daily_sales.py         # job batch: vanzari + conturi
│   ├── spark_sql_jobs/
│   │   ├── daily_loan_analytics.py    # job batch: analiza imprumuturi
│   │   └── daily_spending_analytics.py # job batch: analiza cheltuieli
│   └── spark_streaming_jobs/
│       ├── daily_orders_total.py       # streaming: totaluri comenzi
│       └── daily_transactions_with_clients.py  # streaming: join tranzactii
│
├── plugins/
│   ├── parser.py          # parse argparse CLI pentru joburi Spark
│   ├── writer.py          # write_to_bigquery + write_single_csv
│   └── data_categories.py # constante (tipuri de cont, categorii, etc.)
│
├── scripts/data_generation/
│   ├── bank_data_simulator.py          # genereaza CSV-uri batch (GCS)
│   ├── streaming_bank_data_simulator.py # genereaza date streaming tranzactii+clienti
│   └── streaming_orders_data_simulator.py # genereaza date streaming comenzi
│
├── Dockerfile             # imagine custom Airflow + Java + jars Spark/BQ/GCS
├── docker-compose.yaml    # stack complet: proxy, redis, postgres, airflow
├── requirements.txt       # dependente Python
├── .env                   # variabile de mediu (GCP project, bucket, etc.)
└── secrets/               # fisiere secrete (montate ca Docker secrets)
    ├── google-service-account-key.json
    ├── metadata_db_conn
    ├── smtp_user
    └── smtp_password
```

---

## DAG-uri

### `daily_sales`

Calculeaza totalul cheltuielilor si balanta de deschidere per client pentru ziua respectiva.

```
GCS sensor (sales)    ──┐
                        ├──► SparkSubmit (daily_sales.py) ──► Email
GCS sensor (accounts) ──┘
```

**Input GCS:**
- `sales/sales_YYYY_MM_DD.csv` — tranzactii (transaction_id, client_id, transaction_date, amount, merchant, category)
- `accounts/accounts_YYYY_MM_DD.csv` — conturi noi (account_id, client_id, account_type, opening_balance, open_date)

**Output BigQuery:** `bank_raw_daily_ingest_analytics.daily_sales`

| Coloana | Tip | Descriere |
|---|---|---|
| event_date | DATE | data procesata |
| client_id | INT | ID client |
| total_spent | DOUBLE | suma totala cheltuita in ziua respectiva |
| opening_balance | DOUBLE | suma balantelor de deschidere ale conturilor noi |

---

### `daily_loan_analytics`

Analiza complexa a imprumuturilor: calcul rate, scor de risc, profitabilitate si recomandare per imprumut.

```
GCS sensor (loans)       ──┐
GCS sensor (delay_fees)  ──┼──► SparkSubmit (daily_loan_analytics.py) ──► Email
GCS sensor (sales)       ──┘
```

**Input GCS:**
- `loans/loans_YYYY_MM_DD.csv` — imprumuturi (loan_id, client_id, principal_amount, interest_rate, start_date, term_months, loan_type)
- `delay_fees/delay_fees_YYYY_MM_DD.csv` — taxe intarziere (fee_id, client_id, loan_id, fee_amount, fee_date, days_delayed)
- `sales/sales_YYYY_MM_DD.csv` — tranzactii client (pentru contextul cheltuielilor)

**Output BigQuery:** `bank_raw_daily_ingest_analytics.daily_loan_analytics`

Coloane principale: `loan_id`, `client_id`, `loan_type`, `principal_amount`, `monthly_payment`, `loan_size`, `rate_category`, `total_interest`, `fees_collected`, `total_revenue`, `profit_ratio`, `times_late`, `max_days_late`, `payment_history`, `daily_spending`, `client_type`, `payment_burden_percent`, `risk_score`, `risk_level` (Pretty Safe / Kinda Risky / Risky), `profit_level`, `recommendation`

---

### `daily_spending_analytics`

Profil detaliat de cheltuieli per client: segmentare, preferinte de merchant, ore de cumparaturi, indicator de risc.

```
GCS sensor (accounts) ──┐
                        ├──► SparkSubmit (daily_spending_analytics.py)
GCS sensor (sales)    ──┘
```

**Input GCS:**
- `sales/sales_YYYY_MM_DD.csv`
- `accounts/accounts_YYYY_MM_DD.csv`

**Output BigQuery:** `bank_raw_daily_ingest_analytics.daily_spending_analytics`

Coloane principale: `client_id`, `total_spent`, `transaction_count`, `avg_transaction`, `weekend_spending`, `weekday_spending`, `groceries_spent`, `food_spent`, `entertainment_spent`, `shopping_spent`, `transport_spent`, `bills_spent`, `favorite_merchant`, `spending_tier`, `customer_segment` (VIP / Weekend Warrior / Routine Shopper / etc.), `shopping_preference`, `risk_indicator`, `time_preference`

---

### `daily_orders`

Job de streaming: agregate totaluri de comenzi (cantitate per tip de produs) din fisiere CSV care ajung continuu in GCS.

```
SparkSubmit (daily_orders_total.py) ──► BigQuery (append streaming)
```

**Input GCS (streaming):**
- `orders/date=YYYY-MM-DD/*.csv` — comenzi (order_id, quantity, type, date)
- Fisierele sunt adaugate continuu de simulatorul `streaming_orders_data_simulator.py`

**Output BigQuery:** `bank_raw_daily_ingest_analytics.daily_streaming_orders`

| Coloana | Tip | Descriere |
|---|---|---|
| event_date | DATE | data comenzii |
| type | STRING | tipul produsului (ex: apple, banana) |
| total_quantity | INT | cantitate totala din batch-ul curent |

**Mecanism:** Spark Structured Streaming cu `foreachBatch`, watermark de 1 zi, deduplicare dupa `order_id`, trigger la 15 secunde.

---

### `daily_transactions`

Job de streaming cu **stream-stream join**: imbina tranzactiile bancare cu metadatele clientilor in timp real.

```
SparkSubmit (daily_transactions_with_clients.py) ──► BigQuery (append streaming)
```

**Input GCS (streaming, doua surse paralele):**
- `transactions/date=YYYY-MM-DD/*.csv` — (transaction_id, client_id, amount, timestamp)
- `clients/date=YYYY-MM-DD/*.csv` — (client_id, client_name, account_type, updated_at)

**Output BigQuery:** `bank_raw_daily_ingest_analytics.daily_transaction_totals`

| Coloana | Tip | Descriere |
|---|---|---|
| event_date | DATE | data evenimentului |
| client_id | STRING | ID client |
| client_name | STRING | Numele clientului |
| account_type | STRING | Tipul de cont |
| total_amount | DOUBLE | Suma totala pe batch |

**Mecanism:** Spark stream-stream join cu watermark de 10 minute pe ambele parti, conditie de timp ±5 minute intre event_time-urile celor doua surse, trigger la 15 secunde.

---

## Conexiuni si integrari

### Airflow Connections (configurate in UI sau env)

| Connection ID | Tip | Utilizare |
|---|---|---|
| `google_cloud_default` | Google Cloud | GCSObjectExistenceSensor, acces GCS din DAG-uri |
| `spark_default` | Apache Spark | SparkSubmitOperator |
| `metadata_db` | PostgreSQL | audit log DAG-uri in Cloud SQL |

### Docker Secrets

| Secret | Continut |
|---|---|
| `google_service_account_key` | JSON key pentru Service Account GCP |
| `metadata_db_conn` | URI conexiune PostgreSQL Cloud SQL |
| `smtp_user` | adresa Gmail pentru trimitere emailuri |
| `smtp_password` | parola aplicatie Gmail |

### Variabile de mediu (`.env`)

| Variabila | Valoare exemplu | Utilizare |
|---|---|---|
| `GCP_PROJECT_ID` | `accesa-playground` | project BigQuery si Cloud SQL |
| `GCP_REGION` | `europe-west1` | region Cloud SQL |
| `CLOUDSQL_INSTANCE_ID` | `bank-raw-daily-ingest-sql` | instanta Cloud SQL Proxy |
| `GCS_BUCKET` | `bank-raw-daily-ingest` | bucket sursa date si temp pentru BQ connector |
| `GOOGLE_APPLICATION_CREDENTIALS` | `./secrets/google-service-account-key.json` | autentificare GCP |

---

## Parametri DAG-uri

Toate DAG-urile accepta parametrul optional `date` (format `YYYY-MM-DD`). Daca nu este specificat, se foloseste `ds` (data de executie Airflow).

```bash
# Exemplu trigger manual cu data specifica
airflow dags trigger daily_sales --conf '{"date": "2025-10-01"}'
```

---

## Audit Log (dag_metadata)

La fiecare rulare (success sau failure), callbackul din `dags/helpers/logging.py` scrie in tabelul `dag_metadata` din Cloud SQL:

| Coloana | Descriere |
|---|---|
| dag_id | numele DAG-ului |
| run_id | ID-ul rularii |
| state | `success` sau `failed` |
| executed_at | timestamp-ul executiei |

---

## Generatoare de date (simulatoare)

### `bank_data_simulator.py` (batch)

Genereaza fisiere CSV pentru o zi si le incarca in GCS. Se ruleaza o data pe zi inainte de DAG-uri.

- Produce: `sales_*.csv`, `accounts_*.csv`, `loans_*.csv`, `delay_fees_*.csv`
- Injecteaza erori aleatorii (~2-5%) pentru a testa curatarea datelor
- Configurabil prin `NUM_CLIENTS` (implicit 10.000)

### `streaming_orders_data_simulator.py` (streaming)

Genereaza continuu loturi de comenzi (3-10 inregistrari) la fiecare 10 secunde.

- Produce: `orders/date=YYYY-MM-DD/orders_*.csv`
- Ruleaza in bucla pana la oprire manuala

### `streaming_bank_data_simulator.py` (streaming)

Genereaza continuu perechi de fisiere tranzactii + clienti la fiecare 10 secunde.

- Produce: `transactions/date=YYYY-MM-DD/tx_*.csv` si `clients/date=YYYY-MM-DD/clients_*.csv`
- Folosit impreuna cu DAG-ul `daily_transactions`

---

## Pornire locala

### Preconditii

1. Docker si Docker Compose instalate
2. Fisierele secrete create in `secrets/`:
   - `google-service-account-key.json`
   - `metadata_db_conn` (ex: `postgresql+psycopg2://user:pass@host:5433/dbname`)
   - `smtp_user` (ex: `adresa@gmail.com`)
   - `smtp_password` (parola aplicatie Gmail)

### Comenzi

```bash
# Construieste si porneste stack-ul
docker compose up --build

# Airflow UI
open http://localhost:8080   # user: admin / pass: admin

# Flower (monitor Celery workers)
open http://localhost:5555

# Trigger manual un DAG
docker exec airflow_scheduler airflow dags trigger daily_sales

# Ruleaza simulatorul de date batch
GCS_BUCKET=bank-raw-daily-ingest python scripts/data_generation/bank_data_simulator.py

# Ruleaza simulatorul de streaming (intr-un terminal separat)
GCS_BUCKET=bank-raw-daily-ingest python scripts/data_generation/streaming_orders_data_simulator.py
```

---

## Flux de date complet (batch)

```
Simulator ──► GCS (CSV) ──► GCS Sensor (Airflow) ──► SparkSubmit
    ↓
PySpark citeste CSV din GCS
    ↓
Curatare date (filtrare null-uri, valori negative)
    ↓
Agregari / SQL Analytics (Spark SQL cu CTE-uri)
    ↓
write_to_bigquery() — direct write, WRITE_APPEND
    ↓
BigQuery tabla destinatie
    ↓
log_dag_status() ──► dag_metadata (Cloud SQL)
    ↓
EmailOperator ──► Gmail (notificare succes)
```

## Flux de date complet (streaming)

```
Simulator (loop la 10s) ──► GCS (CSV partitionat pe data)
    ↓
Spark Structured Streaming (readStream .csv)
    ↓
Watermark + deduplicare / stream-stream join
    ↓
foreachBatch: agregare increment ──► write_to_bigquery() (WRITE_APPEND)
    ↓
BigQuery tabla destinatie (append continuu)
```
