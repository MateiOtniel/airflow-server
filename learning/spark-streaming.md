# Spark Structured Streaming – Ghid pentru proiectul tău (GCP + Airflow, fără pași de rulare)

> **Important:** Acesta este **doar ghid de înțelegere a conceptelor + exemple de cod** adaptate proiectului tău. **Nu** conține comenzi `spark-submit`, pași de deploy sau instrucțiuni de rulare. Pentru operare (deploy/rollout/monitorizare) facem un fișier separat.

Contextul tău:

* Airflow **2.10.5** (Python 3.11) pe imagine `apache/airflow:2.10.5-python3.11`.
* `JAVA_HOME` setat, JRE prezent.
* **JAR-uri BigQuery & GCS** puse în `pyspark/.../jars` în Dockerfile (ex.: `spark-bigquery-with-dependencies_2.12-<ver>.jar`, `gcs-connector-<ver>-shaded.jar`).
* Stack local cu CeleryExecutor; GCS bucket: `bank-raw-daily-ingest`; servicii Gmail SMTP setate în `docker-compose.yml`.

---

## Cuprins

1. [Când merită Streaming vs. când rămâi pe Batch](#când-merită-streaming-vs-când-rămâi-pe-batch)
2. [Pro & Contra Streaming](#pro--contra-streaming)
3. [Cum se potrivește Streaming-ul în proiectul tău](#cum-se-potrivește-streaming-ul-în-proiectul-tău)
4. [Concepte cheie (pe cod)](#concepte-cheie-pe-cod)

   * 4.1 Surse & Sinks
   * 4.2 Micro-batch & **Triggers**
   * 4.3 **Event time** vs **Processing time**
   * 4.4 **Watermark** (late data & curățare stare)
   * 4.5 Ferestre: **tumbling / sliding / session**
   * 4.6 **Output modes**: append / update / complete
   * 4.7 **foreachBatch** & scriere idempotentă (exactly-once la sink)
   * 4.8 **Deduplicare** (cu watermark)
   * 4.9 **Join-uri**: stream–static & stream–stream
   * 4.10 **Checkpointing** (fault tolerance)
   * 4.11 Latență & backpressure
   * 4.12 **State store** (inclusiv RocksDB)
   * 4.13 Evoluție schemă
   * 4.14 Testare (MemoryStream)
   * 4.15 Monitorizare (StreamingQueryListener)
5. [Exemple adaptate proiectului tău](#exemple-adaptate-proiectului-tău)

   * 5.1 **Batch → Streaming** pentru jobul tău `daily_sales_job`
   * 5.2 Agregări pe ferestre (sliding/tumbling) pentru vânzări
   * 5.3 Deducerea dublurilor (dropDuplicates + watermark)
   * 5.4 Stream–Static Join cu dimensiuni din BigQuery
6. [Capcane frecvente & „pe înțelesul tuturor”](#capcane-frecvente--pe-înțelesul-tuturor)
7. [Anexă: Observații legate de Dockerfile-ul tău](#anexă-observații-legate-de-dockerfile-ul-tău)

---

## Când merită Streaming vs. când rămâi pe Batch

**Folosește Streaming când:**

* vrei **latență mică** (secunde–minute) pentru dashboard/alerte;
* apar **fișiere noi constant** în GCS (evenimente zilnice/orare care curg pe parcursul zilei);
* ai **agregări rulante** (ex. sumă/medie pe ferestre de timp) și nevoie de toleranță la căderi.

**Rămâi pe Batch când:**

* refresh la 15–60 min e suficient (rapoarte zilnice/orare);
* datele vin **în valuri** (dumpuri) și nu ai nevoie de live;
* vrei simplitate operațională și cost mai mic.

> **Pe scurt (analogie):** Streaming = fabrica lucrează non-stop, prelucrezi pe măsură ce sosesc cutiile. Batch = aduni cutiile într-o zonă și le procesezi o dată pe oră/zi.

---

## Pro & Contra Streaming

**Pro:** latență mică; API unificat cu batch; stateful processing (ferestre, joins); checkpointing (reziliență).

**Contra:** operare mai grea (job long‑running); cost de resurse stabil; conectori/JAR-uri potrivite; nevoie de idempotency la sink (evitarea dublurilor).

---

## Cum se potrivește Streaming-ul în proiectul tău

* **Sursa principală:** foldere GCS (`gs://bank-raw-daily-ingest/...`) în care apar incremental CSV‑uri.
* **Procesare:** curățări/validări + agregări per fereastră sau per zi; eventual `stream–static join` cu dimensiuni (conturi, clienți) din BigQuery sau dintr-un extract periodic.
* **Sink:** BigQuery (direct din `foreachBatch`), cu pattern **staging + MERGE** pentru exactitate (evităm dublurile).
* **Airflow:** orchestrează *serviciul* de streaming (conceptual) și joburile batch/backfill (cu `availableNow`). *În acest ghid nu includem pașii de rulare*.

> **„Pe înțelesul tuturor”**: GCS e „bandă rulantă”, Spark Streaming e „muncitorul” care stă de veghe, BigQuery e „raftul final” unde pui cutiile etichetate corect.

---

## Glossar (definiții simple)

> Scop: explicații scurte, „pe înțelesul tuturor”, pentru termenii pe care îi vezi în cod și în comentarii.

* **Event time** – timpul din date (timestamp-ul evenimentului). Exemplu: când s-a făcut plata. *Asta e „timpul adevărat”.*
* **Processing time** – timpul când Spark a **văzut**/procesat evenimentul. Poate fi mai târziu decât event time.
* **Late data** – evenimente care ajung **după** ce te-ai aștepta (ex.: rețele instabile, retry). Îți pot „strica” agregările dacă nu le gestionezi.
* **Watermark** – „răbdarea ta” pentru late data. Spui cât poți aștepta (ex. `30 minutes`). După acest prag, Spark **închide** ferestrele și **curăță starea**.
* **Window (fereastră)** – intervale pe care agregi:

  * **Tumbling**: bucăți fixe fără suprapunere (ex. 10 min).
  * **Sliding**: bucăți cu suprapunere (ex. fereastră 10 min, pas 2 min).
  * **Session**: grupare după pauze de inactivitate (ex. sesiuni de utilizator).
* **Trigger** – ritmul micro-batch-urilor:

  * `processingTime="30 seconds"` → rulează la fiecare 30s.
  * `availableNow=True` → procesează tot ce e disponibil **acum** și se oprește (bun pentru backfill).
  * `once=True` → un singur lot și stop.
* **Micro-batch** – modul implicit al Structured Streaming: datele sunt procesate în loturi mici. (Modul „continuous” există, dar e rar folosit.)
* **Checkpoint** – folder special (ex. pe GCS) unde Spark ține **progresul** (offseturi) și **starea** (pentru ferestre/join-uri). *Nu îl șterge decât dacă știi exact consecințele.*
* **State store** – spațiul intern (memorie + disc) cu „ce știe” Spark între batch-uri (ferestre deschise, chei, etc.). Se curăță cu watermark. Pentru state mare poți folosi **RocksDB state store**.
* **Output mode** – cum emite rezultatele:

  * **append** – doar rezultate **finale** (ex. ferestre închise).
  * **update** – doar rândurile schimbate față de ultimul batch.
  * **complete** – re-emite **tot** tabelul (costisitor, mai mult pt. demo/debug).
* **`foreachBatch`** – scriere per **batch** (nu per rând). Îți dă control pentru tranzacții, `MERGE`, retry, dedup. (Preferat în producție.)
* **`foreach`** – scriere per **rând**. Simpla, dar grea pentru idempotency și lentă la volum.
* **Idempotent** – dacă rulezi aceeași scriere de mai multe ori, rezultatul final rămâne **același**. (Ex.: `MERGE` în BigQuery după o cheie unică.)
* **Exactly-once (în practică)** – la sursă Spark asigură procesare deterministă; la **sink** obții „exactly-once” **practic** prin **idempotency + dedup** (ex.: `MERGE` în BQ). Fără asta, e „at-least-once”.
* **Backpressure** – controlul ritmului ca să nu inunzi sistemele aval (ex. la Kafka: `maxOffsetsPerTrigger`).
* **Stream–static join** – îmbogățești stream-ul cu o masă **statică** (ex. dimensiuni din BigQuery). Recomandat cu `broadcast`.
* **Stream–stream join** – unești două stream-uri; necesită **watermark pe ambele** și o **condiție temporală**.
* **`dropDuplicates` cu watermark** – dedup sigur pe un orizont de timp (altfel starea poate crește necontrolat).
* **`repartition(n)`** – setezi paralelismul (câte partiții/ task-uri rulează). Prea mic → lent; prea mare → overhead.
* **`checkpointLocation` vs `tempGcsBucket`** – primul e pentru **progresul/starea** Spark; al doilea e **bufferul** conectorului BigQuery pentru încărcări.
* **Kafka `startingOffsets`** – de unde începi (`earliest`, `latest` sau per-topic).
* **Kafka `maxOffsetsPerTrigger`** – limitează câte mesaje iei per micro-batch.

---

## Concepte cheie (pe cod)

### 4.1 Surse & Sinks

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("demo-stream")
         .config("spark.sql.shuffle.partitions", "200")
         .getOrCreate())

# SOURCE: fișiere CSV noi care apar în GCS
src = (spark.readStream
       .schema("id STRING, ts TIMESTAMP, amount DOUBLE")  # schema explicită la CSV/JSON
       .option("header", True)
       .csv("gs://bank-raw-daily-ingest/incoming/*.csv"))

# SINK: pentru exemplu – consolă; în proiect: BigQuery (în foreachBatch)
q = (src.writeStream
     .format("console")
     .outputMode("append")
     .option("checkpointLocation", "gs://bank-raw-daily-ingest/chk/demo/")
     .start())
```

**Observație:** sursa „file” vede **doar fișiere noi**. Pentru foldere existente → vezi conceptul `availableNow` (mai jos), tot în cod, nu ca pas de rulare aici.

### 4.2 Micro-batch & Triggers

```python
(src.writeStream
  .trigger(processingTime="30 seconds")   # la fiecare 30s rulează un micro-batch
  # .trigger(availableNow=True)            # procesează tot ce există acum, apoi se oprește (bun pt backfill)
  # .trigger(once=True)                    # un singur micro-batch și stop
)
```

### 4.3 Event time vs Processing time

```python
from pyspark.sql.functions import col, window

events = src.select("id", "amount", col("ts").alias("event_time"))
agg = (events
       .withWatermark("event_time", "30 minutes")
       .groupBy(window(col("event_time"), "10 minutes"))
       .sum("amount"))
```

**Idee:** *event time* = când s-a întâmplat evenimentul; *processing time* = când a sosit la Spark. Pentru calcule corecte (late data), folosește **event time** + **watermark**.

### 4.4 Watermark (late data & curățare stare)

```python
with_wm = events.withWatermark("event_time", "30 minutes")
```

Watermark = până când accepți evenimente întârziate **și** când Spark poate curăța starea.

### 4.5 Ferestre: tumbling / sliding / session

```python
from pyspark.sql.functions import window, session_window

# Tumbling: ferestre fixe
(tumbling := events
  .withWatermark("event_time", "30 minutes")
  .groupBy(window("event_time", "10 minutes"))
  .count())

# Sliding: ferestre suprapuse
(sliding := events
  .withWatermark("event_time", "30 minutes")
  .groupBy(window("event_time", "10 minutes", "2 minutes"))
  .count())

# Session: ferestre pe perioade de inactivitate
(sessions := events
  .withWatermark("event_time", "1 hour")
  .groupBy(session_window("event_time", "15 minutes"))
  .sum("amount"))
```

Recap rapid (în cuvinte simple)

Tumbling = „cutii” de timp fixe, fără suprapunere.

Sliding = cutii care se și suprapun; același eveniment poate „pica” în mai multe.

Session = grupezi evenimentele unui client atâta timp cât nu există pauze mai mari decât gap-ul.

Watermark = „cât aștepți” întârziații în funcție de event time și când eliberezi memoria.

Boundary = start inclusiv, end exclusiv.

Late data = mai vechi decât watermark → DROP.

Append emite doar ferestre închise (după watermark).

### 4.6 Output modes

* `append`: rânduri finalizate (ex. ferestre închise)
* `update`: doar rânduri modificate
* `complete`: rescrie tot rezultatul (costisitor)

### 4.7 foreachBatch & scriere idempotentă

```python
def upsert_to_bq(batch_df, batch_id: int):
    # 1) scrii într-o masă "staging" (append)
    (batch_df.write
        .format("bigquery")
        .option("table", "<proj>.staging.sales_10m")
        .mode("append").save())
    # 2) apoi faci MERGE în masa finală (pas logic separat) – conceptul rămâne același

(tumbling.writeStream
  .foreachBatch(upsert_to_bq)
  .outputMode("update")
  .option("checkpointLocation", "gs://bank-raw-daily-ingest/chk/upsert/")
  .start())
```

**Pe înțelesul tuturor:** pui noile rezultate într-un „coș temporar”, apoi sincronizezi în „raftul final” fără dubluri.

### 4.8 Deduplicare (cu watermark)

```python
dedup = (events
         .withWatermark("event_time", "1 hour")
         .dropDuplicates(["id"]))
```

### 4.9 Join-uri

```python
# Stream–Static: lookup pe dimensiuni (ex. clienți) – static din BigQuery
static_dim = (spark.read
              .format("bigquery")
              .option("table", "<proj>.<dataset>.dim_clients")
              .load())

enriched = events.join(static_dim.hint("broadcast"), on="id", how="left")

# Stream–Stream: necesită watermark pe ambele + condiție temporală
left  = events.withWatermark("event_time", "30 minutes")
right = other_stream.withWatermark("event_time", "30 minutes")
from pyspark.sql.functions import expr
joined = left.join(
  right,
  expr("""
    left.id = right.id AND
    right.event_time BETWEEN left.event_time - interval 5 minutes
                         AND left.event_time + interval 5 minutes
  """),
  "inner")
```

### 4.10 Checkpointing

```python
(df.writeStream
  .option("checkpointLocation", "gs://bank-raw-daily-ingest/chk/my_stream/")
  .start())
```

**Important:** checkpoint-ul păstrează progresul & starea. Ștergerea lui poate conduce la re-procesare.

### 4.11 Latență & backpressure

* `trigger(processingTime="N seconds")` – cât de des rulează micro-batch-ul.
* Kafka (dacă folosești): `maxOffsetsPerTrigger`, `startingOffsets`.
* Paralelism: `df = df.repartition(200)` – ajustezi în funcție de volum.

### 4.12 State store (inclusiv RocksDB)

* Ferestre/join-uri mențin **stare**; watermark + setările `spark.sql.streaming.stateStore.*` controlează curățarea.
* RocksDB (Spark 3.4+) – util pentru stare mare.

### 4.13 Evoluție schemă

* La CSV/JSON definește schemă explicită; tratează coloane noi prin selectări explicite.
* Pentru tabele stabile, ia în calcul formate tabelare (Delta/Parquet) pentru control mai bun.

### 4.14 Testare (MemoryStream)

* Pentru teste unitare poți folosi MemoryStream ca să injectezi evenimente și să verifici rezultatele.

### 4.15 Monitorizare (StreamingQueryListener)

```python
from pyspark.sql.streaming import StreamingQueryListener
class MyListener(StreamingQueryListener):
    def onQueryProgress(self, event):
        print("Batch duration:", event.progress["batchDuration"])

spark.streams.addListener(MyListener())
```

---

## Exemple adaptate proiectului tău

### 5.1 Batch → Streaming pentru `daily_sales_job`

**Context:** ai deja cod batch care citește din GCS, agregă și scrie în BigQuery. Mai jos e o variantă **streaming** pe același model conceptual, folosind `foreachBatch` pentru scriere.

```python
# streaming/app_daily_sales_stream.py
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, coalesce, to_date, lit, sum as sum_, to_timestamp

PROJECT = "<proj>"
DATASET = "<dataset>"
TABLE   = "daily_sales_agg"
BUCKET  = "bank-raw-daily-ingest"
CHK     = f"gs://{BUCKET}/checkpoints/daily_sales_stream/"

spark = (SparkSession.builder
         .appName("daily_sales_stream")
         .config("spark.sql.shuffle.partitions", "200")
         .config("spark.bigquery.projectId", PROJECT)
         .getOrCreate())

sales_schema = StructType([
    StructField("transaction_id",   StringType(), True),
    StructField("client_id",        IntegerType(), True),
    StructField("transaction_ts",   StringType(), True),  # ca text → îl convertim la timestamp
    StructField("amount",           DoubleType(),  True),
    StructField("merchant",         StringType(), True),
    StructField("category",         StringType(), True),
])

# 1) Citește STREAM de fișiere noi (CSV) din GCS
sales_raw = (spark.readStream
    .schema(sales_schema)
    .option("header", True)
    .csv(f"gs://{BUCKET}/sales/*.csv"))

# 2) Normalizează & filtrează pe eveniment (ziua curentă derivată din timestamp)
sales_clean = (sales_raw
    .withColumn("ts", to_timestamp(col("transaction_ts")))
    .where((col("client_id").isNotNull()) & (col("amount").isNotNull()) & (col("amount") >= lit(0)))
)

# 3) Agregă pe zi/client (event-time). Exemplu simplu: folosim data din timestamp
sales_day = (sales_clean
    .withColumn("event_date", to_date(col("ts")))
    .groupBy("event_date", "client_id")
    .agg(sum_("amount").alias("total_spent")))

# 4) Scriere în BigQuery prin foreachBatch (pattern staging → MERGE final, dacă e nevoie)

def write_to_bq(batch_df: DataFrame, batch_id: int):
    (batch_df.write
        .format("bigquery")
        .option("table", f"{PROJECT}.{DATASET}.{TABLE}")
        .mode("append")
        .save())

query = (sales_day.writeStream
    .outputMode("update")
    .foreachBatch(write_to_bq)
    .option("checkpointLocation", CHK)
    .start())

query.awaitTermination()
```

**Observații:**

* Sursa de tip „file” ia **doar fișiere noi** din `gs://.../sales/`. Pentru a prelucra istoricul existent, folosește trigger-ul `availableNow` (concept explicat mai sus).
* `event_date` derivat din `ts` este *event time*, nu „momentul procesării”.
* Dacă ai nevoie de *exactly-once* în masa finală, aplici **staging + MERGE** (conceptual), tot via `foreachBatch`.

### 5.2 Agregări pe ferestre

```python
from pyspark.sql.functions import window

windowed = (sales_clean
  .withWatermark("ts", "30 minutes")
  .groupBy(window(col("ts"), "10 minutes"), col("client_id"))
  .agg(sum_("amount").alias("total_amount"))
  .selectExpr("window.start as window_start", "window.end as window_end", "client_id", "total_amount"))
```

### 5.3 Deduplicare evenimente

```python
# Presupunem un ID unic de eveniment pe tranzacție
sales_dedup = (sales_clean
  .withWatermark("ts", "1 hour")
  .dropDuplicates(["transaction_id"]))
```

### 5.4 Stream–Static Join cu dimensiuni din BigQuery

```python
# Dimensiuni statice (ex. tipuri de cont, merchandising rules) din BigQuery
static_dims = (spark.read
  .format("bigquery")
  .option("table", f"{PROJECT}.{DATASET}.dim_clients")
  .load())

enriched = sales_day.join(static_dims.hint("broadcast"), on="client_id", how="left")
```

**Observații:** dimensiunile nu se schimbă des în runtime; le poți reîncărca periodic ca batch separat dacă ai nevoie de refresh.

---

## Capcane frecvente & „pe înțelesul tuturor”

* **„Nu îmi ia fișierele deja existente”** → sursa file „vede” doar fișiere noi; pentru catch‑up folosești conceptul `availableNow`.
* **„Memoria crește”** → ai uitat `withWatermark` la agregări/joins; Spark nu eliberează starea veche.
* **„Am dubluri în BigQuery”** → scrie prin `foreachBatch` și aplică **staging + MERGE** (idempotent).
* **„Jobul nu se termină”** → streaming este **long‑running** prin definiție; îl tratezi ca serviciu.

---