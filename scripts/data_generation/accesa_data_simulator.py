#!/usr/bin/env python3
import os
import json
import random
import uuid
from datetime import datetime, date, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType, DateType,
)

from plugins.writer import write_single_csv


# ---------------------- Static categorical data ----------------------

CITIES = [
    "Cluj-Napoca", "București", "Iași", "Timișoara",
    "Constanța", "Brașov", "Sibiu", "Oradea", "Craiova", "Galați",
]

FIRST_NAMES = [
    "Andrei", "Maria", "Ion", "Elena", "Ioana", "Mihai", "Ana", "Alexandru",
    "Cristina", "Vlad", "Ștefan", "Diana", "Cătălin", "Raluca", "Bogdan",
    "Andreea", "Răzvan", "Monica", "Tudor", "Iulia", "George", "Roxana",
]

LAST_NAMES = [
    "Popescu", "Ionescu", "Pop", "Popa", "Stan", "Munteanu", "Stoica",
    "Dumitru", "Diaconu", "Marin", "Constantinescu", "Georgescu", "Radu",
    "Gheorghe", "Toma", "Florea", "Barbu", "Stoian", "Voicu", "Lupu",
]

STREET_TYPES = ["Str.", "Strada", "Bd.", "Bulevardul", "Calea", "Aleea", "Splaiul"]

STREET_NAMES = [
    "Mihai Viteazu", "Eroilor", "Unirii", "Republicii", "Libertății",
    "Avram Iancu", "Mihai Eminescu", "Ștefan cel Mare", "Decebal", "Traian",
    "Horea", "Independenței", "Victoriei", "Florilor", "Primăverii",
    "Trandafirilor", "Castanilor", "Crizantemelor",
]

EMAIL_PROVIDERS = [
    "gmail.com", "yahoo.com", "yahoo.ro", "hotmail.com",
    "outlook.com", "accesa.eu", "icloud.com",
]

FIDELITY_TYPES = ["standard", "silver", "gold", "platinum"]

PAYMENT_METHODS = ["cash", "card", "contactless", "voucher"]

# (prefix, operator) — Romanian mobile prefixes
MOBILE_PREFIXES = [
    ("0744", "Orange"), ("0745", "Orange"), ("0746", "Orange"),
    ("0722", "Vodafone"), ("0726", "Vodafone"), ("0727", "Vodafone"),
    ("0772", "Digi"), ("0773", "Digi"),
    ("0735", "Telekom"), ("0736", "Telekom"),
]

AGE_RESTRICTED_CATEGORIES = {"Băuturi alcoolice", "Băuturi energizante", "Țigări"}

# (name, category, base_price, tva_rate). "ALTELE" entries are intentional —
# the analysis pipeline must infer their real category.
PRODUCT_CATALOG = [
    ("Lapte 1.5% 1L",            "Lactate",             7.5,   0.09),
    ("Iaurt natural 400g",       "Lactate",             6.2,   0.09),
    ("Brânză telemea 250g",      "Lactate",            12.0,   0.09),
    ("Smântână 12% 200g",        "Lactate",             5.5,   0.09),
    ("Cașcaval 200g",            "Lactate",            18.5,   0.09),
    ("Pâine albă 500g",          "Panificație",         4.0,   0.09),
    ("Cornuri cu unt 4buc",      "Panificație",         9.0,   0.09),
    ("Baghetă",                  "Panificație",         5.5,   0.09),
    ("Mere roșii 1kg",           "Fructe",              6.0,   0.09),
    ("Banane 1kg",               "Fructe",              8.5,   0.09),
    ("Portocale 1kg",            "Fructe",              7.0,   0.09),
    ("Struguri 1kg",             "Fructe",             10.5,   0.09),
    ("Cartofi 1kg",              "Legume",              4.0,   0.09),
    ("Roșii 1kg",                "Legume",              9.0,   0.09),
    ("Castraveți 1kg",           "Legume",              7.5,   0.09),
    ("Ardei capia 1kg",          "Legume",             11.0,   0.09),
    ("Piept de pui 1kg",         "Carne",              28.0,   0.09),
    ("Carne tocată vită 500g",   "Carne",              32.5,   0.09),
    ("Cârnați afumați 500g",     "Carne",              24.0,   0.09),
    ("Somon proaspăt 200g",      "Pește",              35.0,   0.09),
    ("File de cod 400g",         "Pește",              22.0,   0.09),
    ("Pizza congelată 350g",     "Congelate",          15.0,   0.09),
    ("Legume mexicane 750g",     "Congelate",          13.5,   0.09),
    ("Ciocolată cu lapte 100g",  "Dulciuri",            6.5,   0.19),
    ("Biscuiți cu cremă 200g",   "Dulciuri",            5.0,   0.19),
    ("Bomboane mentolate 100g",  "Dulciuri",            4.5,   0.19),
    ("Chipsuri sare 140g",       "Snacks",              8.0,   0.19),
    ("Alune sărate 200g",        "Snacks",             12.0,   0.19),
    ("Pateu de ficat 100g",      "Conserve",            4.0,   0.09),
    ("Mazăre conservată 400g",   "Conserve",            6.0,   0.09),
    ("Sare iodată 1kg",          "Condimente",          3.5,   0.09),
    ("Piper măcinat 50g",        "Condimente",          7.5,   0.09),
    ("Cafea măcinată 250g",      "Cafea",              22.0,   0.09),
    ("Cafea boabe 1kg",          "Cafea",              75.0,   0.09),
    ("Ceai negru 25 plicuri",    "Ceai",                8.5,   0.09),
    ("Ceai verde 25 plicuri",    "Ceai",                9.5,   0.09),
    ("Suc portocale 1L",         "Sucuri",              9.0,   0.09),
    ("Suc mere 1L",              "Sucuri",              8.5,   0.09),
    ("Apă plată 2L",             "Apă",                 3.5,   0.09),
    ("Apă minerală 1.5L",        "Apă",                 3.0,   0.09),
    ("Bere blondă 0.5L",         "Băuturi alcoolice",   5.5,   0.19),
    ("Vin roșu 750ml",           "Băuturi alcoolice",  28.0,   0.19),
    ("Vodka 700ml",              "Băuturi alcoolice",  65.0,   0.19),
    ("Whisky 700ml",             "Băuturi alcoolice", 120.0,   0.19),
    ("Energizant 250ml",         "Băuturi energizante", 7.5,   0.19),
    ("Energizant 500ml",         "Băuturi energizante",11.0,   0.19),
    ("Țigări mentol pachet",     "Țigări",             22.0,   0.19),
    ("Țigări clasice pachet",    "Țigări",             21.0,   0.19),
    ("Detergent rufe 2kg",       "ALTELE",             35.0,   0.19),
    ("Hârtie igienică 8 role",   "ALTELE",             18.0,   0.19),
    ("Șampon 400ml",             "ALTELE",             16.0,   0.19),
    ("Pastă de dinți 100ml",     "ALTELE",             11.0,   0.19),
    ("Detergent vase 750ml",     "ALTELE",             14.5,   0.19),
]


# --------------------------- helpers ---------------------------

def _inject_errors_spark(rows: list, schema: StructType, error_rate: float, spark: SparkSession) -> DataFrame:
    """
    Inject missing or simple type-mismatch errors:
     - For string fields: randomly inject a numeric value.
     - For all other types: inject None.
    """
    total = len(rows)
    if total == 0:
        return spark.createDataFrame(rows, schema)
    n_err = int(total * error_rate)
    for _ in range(n_err):
        idx = random.randrange(total)
        field = random.choice(schema.names)
        dtype = next(f.dataType for f in schema.fields if f.name == field)
        if isinstance(dtype, StringType) and random.random() < 0.5:
            rows[idx][field] = random.uniform(100, 500)
        else:
            rows[idx][field] = None
    return spark.createDataFrame(rows, schema)


def _wednesday_of_week(d: date) -> date:
    """Wednesday of the Wed–Tue week containing d (Wed=0, Tue=6)."""
    return d - timedelta(days=(d.weekday() - 2) % 7)


def _random_birth_date(today: date) -> date:
    age = random.choices(
        [random.randint(10, 17), random.randint(18, 24), random.randint(25, 34),
         random.randint(35, 49), random.randint(50, 64), random.randint(65, 90)],
        weights=[3, 12, 22, 28, 22, 13],
    )[0]
    return today - timedelta(days=age * 365 + random.randint(0, 364))


def _build_address(city: str) -> str:
    street_type = random.choice(STREET_TYPES)
    street_name = random.choice(STREET_NAMES)
    nr = random.randint(1, 250)
    extras = random.choice([
        "",
        f", bl. {random.choice('ABCDE')}{random.randint(1, 30)}",
        f", ap. {random.randint(1, 80)}",
        f", et. {random.randint(0, 8)}, ap. {random.randint(1, 80)}",
    ])
    return random.choice([
        f"{street_type} {street_name}, nr. {nr}{extras}, {city}",
        f"{street_type} {street_name} nr. {nr}{extras}, {city}",
        f"{street_type} {street_name} {nr}{extras}, {city}",
        f"{city}, {street_type} {street_name}, nr. {nr}{extras}",
    ])


def _build_phone() -> str:
    prefix, _operator = random.choice(MOBILE_PREFIXES)
    rest = "".join(str(random.randint(0, 9)) for _ in range(7))
    return random.choice([
        f"+40{prefix[1:]}{rest}",
        f"+40 {prefix[1:]} {rest[:3]} {rest[3:]}",
        f"{prefix}{rest}",
        f"{prefix} {rest[:3]} {rest[3:]}",
        f"0{prefix[1:]}-{rest[:3]}-{rest[3:]}",
    ])


def _build_email(first: str, last: str) -> str:
    base = f"{first.lower()}.{last.lower()}{random.randint(0, 99)}"
    base = (base.replace('ș', 's').replace('ț', 't').replace('ă', 'a')
                .replace('â', 'a').replace('î', 'i'))
    return f"{base}@{random.choice(EMAIL_PROVIDERS)}"


def _maybe_fidelity_card(birth_date: date, today: date):
    """JSON fidelity-card string, or None for ~25% of customers."""
    if random.random() < 0.25:
        return None
    earliest_issue = max(birth_date + timedelta(days=18 * 365),
                         today - timedelta(days=10 * 365))
    days_range = max(1, (today - earliest_issue).days)
    issue = earliest_issue + timedelta(days=random.randint(0, days_range))
    return json.dumps({
        "card_id": str(uuid.uuid4()),
        "issue_date": issue.strftime("%Y-%m-%d"),
        "type": random.choice(FIDELITY_TYPES),
    })


# --------------------------- generators ---------------------------

def _build_products():
    return [{
        "product_id":     str(uuid.uuid4()),
        "product_name":   name,
        "category":       category,
        "tva_rate":       tva,
        "base_price":     round(base_price, 2),
        "age_restricted": "true" if category in AGE_RESTRICTED_CATEGORIES else "false",
    } for name, category, base_price, tva in PRODUCT_CATALOG]


def _build_customers(num_customers: int, today: date):
    customers = []
    for _ in range(num_customers):
        first = random.choice(FIRST_NAMES)
        last  = random.choice(LAST_NAMES)
        city  = random.choice(CITIES)
        birth = _random_birth_date(today)
        underage = (today - birth).days < 18 * 365
        fidelity = None if underage else _maybe_fidelity_card(birth, today)

        customers.append({
            "customer_id":   str(uuid.uuid4()),
            "first_name":    first,
            "last_name":     last,
            "birth_date":    birth,
            "email":         _build_email(first, last) if random.random() > 0.07 else None,
            "phone_number":  _build_phone() if random.random() > 0.10 else None,
            "address":       _build_address(city) if random.random() > 0.05 else None,
            "city":          city,
            "fidelity_card": fidelity,
            "customer_type": random.choices(
                ["persoana_fizica", "persoana_juridica"], weights=[92, 8]
            )[0],
        })
    return customers


def _build_product_prices(products, start_date: date, end_date: date):
    """Daily price records. Prices change only on Wednesdays. ~5% of daily records are missing."""
    rows = []
    current_prices = {p["product_id"]: p["base_price"] for p in products}
    cur = start_date
    while cur <= end_date:
        if cur.weekday() == 2:  # Wednesday
            for pid in current_prices:
                if random.random() < 0.30:
                    drift = random.uniform(-0.05, 0.12)  # mostly upward (inflation)
                    current_prices[pid] = round(
                        max(0.5, current_prices[pid] * (1 + drift)), 2
                    )
                else:
                    current_prices[pid] = round(
                        current_prices[pid] * (1 + random.uniform(0, 0.004)), 2
                    )
        for pid, price in current_prices.items():
            if random.random() < 0.05:
                continue
            rows.append({
                "price_id":   str(uuid.uuid4()),
                "product_id": pid,
                "valid_date": cur,
                "price":      price,
            })
        cur += timedelta(days=1)
    return rows


def _build_discounts(products, start_date: date, end_date: date):
    """One row per (week, discounted product). Weeks run Wed–Tue."""
    rows = []
    week_start = _wednesday_of_week(start_date)
    if week_start < start_date:
        week_start += timedelta(days=7)
    while week_start <= end_date:
        week_end = min(week_start + timedelta(days=6), end_date)
        discounted = random.sample(products, k=random.randint(8, 18))
        for p in discounted:
            rows.append({
                "discount_id":         str(uuid.uuid4()),
                "product_id":          p["product_id"],
                "discount_percentage": random.choice([0.10, 0.15, 0.20, 0.25, 0.30, 0.50]),
                "valid_from":          week_start,
                "valid_to":            week_end,
            })
        week_start += timedelta(days=7)
    return rows


def _build_transactions(customers, products, start_date: date, end_date: date):
    """~12 transactions/customer/year, 1–8 line items each. Some days are store closures."""
    rows = []
    products_by_id = {p["product_id"]: p for p in products}
    product_ids = list(products_by_id.keys())

    total_days = (end_date - start_date).days + 1
    closure_days = set(random.sample(
        [start_date + timedelta(days=i) for i in range(total_days)],
        k=max(1, total_days // 20),
    ))

    store_ids = [f"S{n:03d}" for n in range(1, 6)]
    hour_weights = [2, 3, 5, 7, 8, 9, 10, 12, 11, 9, 8, 7, 6, 5, 3]  # 7..21

    for c in customers:
        n_tx = max(1, int(random.gauss(12, 4)))
        for _ in range(n_tx):
            for _ in range(20):
                offset = random.randint(0, total_days - 1)
                tx_date = start_date + timedelta(days=offset)
                if tx_date not in closure_days:
                    break
            hour = random.choices(list(range(7, 22)), weights=hour_weights)[0]
            tx_ts = datetime.combine(tx_date, datetime.min.time()).replace(
                hour=hour, minute=random.randrange(60), second=random.randrange(60)
            )
            tx_id = str(uuid.uuid4())
            payment = random.choice(PAYMENT_METHODS)
            store_id = random.choice(store_ids)

            line_count = random.randint(1, 8)
            for pid in random.sample(product_ids, k=line_count):
                product = products_by_id[pid]
                rows.append({
                    "transaction_id":         tx_id,
                    "customer_id":            c["customer_id"],
                    "product_id":             pid,
                    "quantity":               random.randint(1, 4),
                    "transaction_date":       tx_ts,
                    "payment_method":         payment,
                    "store_id":               store_id,
                    "unit_price_at_purchase": product["base_price"],
                })
    return rows


# --------------------------- orchestration ---------------------------

def generate_and_upload(
    spark: SparkSession,
    start_date: date,
    end_date: date,
    bucket: str,
    num_customers: int,
):
    today = end_date  # treat end_date as "today" for age & card-issue logic

    # PRODUCTS
    products = _build_products()
    products_schema = StructType([
        StructField("product_id",     StringType(), True),
        StructField("product_name",   StringType(), True),
        StructField("category",       StringType(), True),
        StructField("tva_rate",       DoubleType(), True),
        StructField("base_price",     DoubleType(), True),
        StructField("age_restricted", StringType(), True),
    ])
    products_df = _inject_errors_spark(products, products_schema, 0.02, spark)
    write_single_csv(
        products_df, "/tmp/accesa_products",
        f"gs://{bucket}/accesa_products/accesa_products.csv",
    )

    # CUSTOMERS
    customers = _build_customers(num_customers, today)
    customers_schema = StructType([
        StructField("customer_id",   StringType(), True),
        StructField("first_name",    StringType(), True),
        StructField("last_name",     StringType(), True),
        StructField("birth_date",    DateType(),   True),
        StructField("email",         StringType(), True),
        StructField("phone_number",  StringType(), True),
        StructField("address",       StringType(), True),
        StructField("city",          StringType(), True),
        StructField("fidelity_card", StringType(), True),
        StructField("customer_type", StringType(), True),
    ])
    customers_for_df = [dict(c) for c in customers]  # error-injection mutates rows
    customers_df = _inject_errors_spark(customers_for_df, customers_schema, 0.04, spark)
    write_single_csv(
        customers_df, "/tmp/accesa_customers",
        f"gs://{bucket}/accesa_customers/accesa_customers.csv",
    )

    # PRODUCT PRICES
    price_rows = _build_product_prices(products, start_date, end_date)
    prices_schema = StructType([
        StructField("price_id",   StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("valid_date", DateType(),   True),
        StructField("price",      DoubleType(), True),
    ])
    prices_df = _inject_errors_spark(price_rows, prices_schema, 0.01, spark)
    write_single_csv(
        prices_df, "/tmp/accesa_product_prices",
        f"gs://{bucket}/accesa_product_prices/accesa_product_prices.csv",
    )

    # DISCOUNTS
    discount_rows = _build_discounts(products, start_date, end_date)
    discounts_schema = StructType([
        StructField("discount_id",         StringType(), True),
        StructField("product_id",          StringType(), True),
        StructField("discount_percentage", DoubleType(), True),
        StructField("valid_from",          DateType(),   True),
        StructField("valid_to",            DateType(),   True),
    ])
    discounts_df = _inject_errors_spark(discount_rows, discounts_schema, 0.01, spark)
    write_single_csv(
        discounts_df, "/tmp/accesa_discounts",
        f"gs://{bucket}/accesa_discounts/accesa_discounts.csv",
    )

    # TRANSACTIONS
    tx_rows = _build_transactions(customers, products, start_date, end_date)
    tx_schema = StructType([
        StructField("transaction_id",         StringType(),    True),
        StructField("customer_id",            StringType(),    True),
        StructField("product_id",             StringType(),    True),
        StructField("quantity",               IntegerType(),   True),
        StructField("transaction_date",       TimestampType(), True),
        StructField("payment_method",         StringType(),    True),
        StructField("store_id",               StringType(),    True),
        StructField("unit_price_at_purchase", DoubleType(),    True),
    ])
    tx_df = _inject_errors_spark(tx_rows, tx_schema, 0.03, spark)
    write_single_csv(
        tx_df, "/tmp/accesa_transactions",
        f"gs://{bucket}/accesa_transactions/accesa_transactions.csv",
    )


def main():
    bucket = os.getenv("GCS_BUCKET")
    if not bucket:
        raise RuntimeError("Environment variable GCS_BUCKET must be set")

    num_customers = int(os.getenv("NUM_CUSTOMERS", 10_000))
    start_date = datetime.strptime(
        os.getenv("START_DATE", "2025-01-01"), "%Y-%m-%d"
    ).date()
    end_date = datetime.strptime(
        os.getenv("END_DATE", "2025-12-31"), "%Y-%m-%d"
    ).date()

    spark = (
        SparkSession.builder
        .appName("generate_accesa_data")
        .master("local[*]")
        .getOrCreate()
    )

    print(
        f"Writing accesa datasets for {start_date}..{end_date} "
        f"({num_customers} customers) to gs://{bucket}/…"
    )
    generate_and_upload(spark, start_date, end_date, bucket, num_customers)
    spark.stop()
    print("Done.")


if __name__ == "__main__":
    main()
