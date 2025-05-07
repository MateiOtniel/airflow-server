import random
from datetime import datetime, timedelta
from pyspark.sql import Row, DataFrame, SparkSession

DATASETS = ["sales", "inventory", "billing", "analytics"]
DATAPRODUCTS_MAP = {
    "sales": ["north_america_q1", "europe_q2", "asia_q3", "south_america_q4"],
    "inventory": ["warehouse_east", "warehouse_west", "central_stock"],
    "billing": ["invoices_2025", "receipts_2025"],
    "analytics": ["user_engagement", "system_metrics", "revenue_forecast"]
}
MANDANTS = [1001, 1002, 1003]

def generate_events_sample_data(spark: SparkSession) -> DataFrame:
    rows = []
    now = datetime.now()
    for _ in range(500):
        dataset = random.choice(DATASETS)
        dataproduct = random.choice(DATAPRODUCTS_MAP[dataset])
        mandant = random.choice(MANDANTS)
        refdate = (now.date() - timedelta(days=random.randint(0, 29)))
        ts_offset = random.randint(80000,  200000)
        timestamp = datetime.combine(refdate, datetime.min.time()) + timedelta(seconds=ts_offset)

        rows.append(Row(
            dataset=dataset,
            dataproduct=dataproduct,
            mandant=mandant,
            refdate=refdate.strftime("%Y-%m-%d"),
            timestamp=timestamp.strftime("%Y-%m-%d %H:%M:%S")
        ))

    mock_df = spark.createDataFrame(rows)
    return mock_df