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
│   ├── parser.py            # parse argparse CLI pentru joburi Spark
│   ├── writer.py            # write_to_bigquery + write_single_csv
│   ├── data_categories.py   # constante (tipuri de cont, categorii, etc.)
│   └── accesa_readers.py    # scheme + readere CSV pentru cele 5 datasets Accesa
│
├── scripts/data_generation/
│   ├── bank_data_simulator.py          # genereaza CSV-uri batch (GCS)
│   ├── streaming_bank_data_simulator.py # genereaza date streaming tranzactii+clienti
│   ├── streaming_orders_data_simulator.py # genereaza date streaming comenzi
│   └── accesa_data_simulator.py        # genereaza an intreg de date retail Accesa
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

## DAG-uri (retail Accesa)

Pipeline separat pentru cerinta noua de la Accesa — 14 DAG-uri care acopera curatare, partitionare, analize pret/discount, demografie, comportament de cumparare, raportare financiara si recomandari.

Datele sunt generate intr-o singura rulare (an intreg, ~10k clienti) si scrise in 5 CSV-uri in GCS:

```
accesa_customers/accesa_customers.csv
accesa_products/accesa_products.csv
accesa_product_prices/accesa_product_prices.csv
accesa_discounts/accesa_discounts.csv
accesa_transactions/accesa_transactions.csv
```

Schemele si readerele sunt definite o singura data in `plugins/accesa_readers.py` si reutilizate de toate joburile Spark.

Toate DAG-urile au aceeasi forma ca cele de banca: `GCSObjectExistenceSensor` per input ──► `SparkSubmit` ──► `EmailOperator`, cu `log_dag_status` ca callback. Joburile scriu mai multe tabele in BigQuery — fiecare cu sufix peste `--table`-ul DAG-ului.

| DAG | Ce face | Sufixe BQ |
|---|---|---|
| `accesa_data_preparation` | parseaza `fidelity_card` JSON, sparge adresa (UDF + built-ins), normalizeaza telefonul, detecteaza si umple preturi lipsa (preturile se schimba doar miercurea) | `_customers_clean`, `_missing_prices`, `_prices_filled` |
| `accesa_storage_partitioning` | scrie produsele in Parquet (1 fisier/saptamana Wed-Tue) si CSV (7 fisiere/saptamana — unul/zi); top 100 clienti ca text | scrie direct in GCS: `accesa_products_parquet/`, `accesa_products_csv/`, `accesa_top_customers/` |
| `accesa_price_change_behavior` | top schimbari de pret, produse care n-au scazut, cresteri inainte de discount, >30% scumpire, top 20 ieftiniri, predictie inflatie pentru sfarsit de 2026 | `_frequent_changes`, `_never_decreased`, `_increase_before_discount`, `_over_30_increase`, `_top20_cheaper`, `_inflation_prediction` |
| `accesa_price_aggregations` | media pretului saptamanal pe categorie, pret net + TVA per produs | `_weekly_avg_per_category`, `_net_and_tva_per_product` |
| `accesa_category_inference` | inferenta categoriei reale pentru produsele marcate `ALTELE`, lista categoriilor cu restrictie de varsta | `_inferred_categories`, `_age_restricted_categories` |
| `accesa_discount_analysis` | media discount/saptamana, categoria cu cele mai multe oferte, produse cu 3+ discounturi, pret cu/fara card de fidelitate, cumparari in fereastra de discount, produse niciodata cumparate la oferta, economii ratate de non-fidelity | `_avg_discount_per_week`, `_discounts_per_category`, `_frequent_discount_products`, `_discounted_prices`, `_purchases_during_discount`, `_never_purchased_while_discounted`, `_missed_savings_non_fidelity` |
| `accesa_customer_demographics` | grupe de varsta pe oras, cel mai tanar/batran din fiecare oras, varsta medie pe oras + tip card, numar minori, vechime card, top 5 cei mai vechi posesori per oras, card primit inainte de 25 | `_age_group_by_city`, `_youngest_oldest_per_city`, `_avg_age_per_city`, `_avg_age_by_fidelity_type`, `_underage_count`, `_fidelity_tenure`, `_top5_longest_per_city`, `_card_before_25` |
| `accesa_customer_data_quality` | adrese lipsa pe tip de card, telefoane/emailuri lipsa, % cu prefix +40, % cu orice telefon, grupa de varsta cu cele mai multe telefoane lipsa, distributia providerilor de email, varsta medie + stddev pentru emailuri `@accesa.eu` | `_missing_address_by_fidelity`, `_missing_phone_email_counts`, `_phone_prefix_share`, `_phone_any_share`, `_age_group_most_missing_phone`, `_email_providers`, `_accesa_email_age_stats` |
| `accesa_customer_names_addresses` | strazi cele mai comune pe oras, nume de familie cele mai comune, clienti cu nume identic | `_common_streets_per_city`, `_common_family_names`, `_identical_full_names` |
| `accesa_customer_metrics` | total produse/client, marime medie cos pe metoda de plata, comparatie fidelity vs. non-fidelity, top 10 cheltuieli si top 10 produse cumparate in 2025, minori care au cumparat produse cu restrictie | `_total_products_per_customer`, `_avg_basket_by_payment`, `_fidelity_vs_non_fidelity`, `_top10_spenders_2025`, `_top10_buyers_by_items_2025`, `_underage_age_restricted` |
| `accesa_item_metrics` | produsul cel mai cumparat, perechi cumparate frecvent impreuna, top 10 in decembrie, categoria cu cele mai multe discounturi | `_most_purchased_products`, `_frequently_bought_together`, `_top10_december`, `_category_with_most_discounts` |
| `accesa_temporal_trends` | zilele in care magazinul a fost inchis, luna cu cele mai multe vanzari, top 10 zile, ora de varf per zi, statistici orare (min/max/avg), max tranzactii la deschidere/inchidere, distributie zilnica Lu-Du, evolutia cumparaturilor de vineri | `_closure_days`, `_busiest_month`, `_top10_days`, `_peak_hour_per_day`, `_hourly_item_stats`, `_max_tx_open_close`, `_daily_distribution`, `_friday_trends` |
| `accesa_financial_revenue` | total venit 2025 (overall, fara discount, doar discount, lunar), TVA total, TVA recuperabil pentru persoane juridice | `_totals_2025`, `_monthly_2025`, `_total_tva_2025`, `_refundable_tva_legal_entities` |
| `accesa_recommendations` | top 5 produse recomandate per client pe baza categoriilor preferate + popularitate, excluzand ce a cumparat deja | `_next_product_recommendations` |

### Cum se ruleaza

```bash
# 1) genereaza datele (o singura data, an intreg)
GCS_BUCKET=bank-raw-daily-ingest python scripts/data_generation/accesa_data_simulator.py

# 2) trigger orice DAG accesa din UI sau CLI
docker exec airflow_scheduler airflow dags trigger accesa_data_preparation
```

DAG-urile nu au parametru `date` — datele sunt statice (generate o data pentru tot anul) si DAG-urile sunt trigger-uite manual.

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

### `accesa_data_simulator.py` (batch, retail Accesa)

O singura rulare → un an intreg de date pentru toate cele 14 DAG-uri Accesa.

- Produce: `accesa_customers.csv`, `accesa_products.csv`, `accesa_product_prices.csv`, `accesa_discounts.csv`, `accesa_transactions.csv` (toate sub `accesa_<dataset>/` in GCS)
- Preturile se schimba doar miercurea, ~5% zile lipsa pentru detectie de inregistrari lipsa
- Variabile: `NUM_CUSTOMERS` (implicit 10.000), `START_DATE` (`2025-01-01`), `END_DATE` (`2025-12-31`)
- Injecteaza erori (~2-5%) ca si la simulatorul de banca

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
