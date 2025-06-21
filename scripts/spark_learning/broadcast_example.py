from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# 1. Inițializare Spark
spark = SparkSession.builder \
    .appName("BroadcastExample") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 2. DataFrame mare: tranzacții (simulat)
transactions = spark.createDataFrame([
    (1, "2025-06-20", 100.0),
    (2, "2025-06-20",  50.0),
    (3, "2025-06-21", 200.0),
    # ... (să zicem milioane de rânduri)
], ["user_id", "date", "amount"])

# 3. DataFrame mic: informații despre utilizatori
users = spark.createDataFrame([
    (1, "Alice"),
    (2, "Bob"),
    (3, "Carol"),
], ["user_id", "name"])

# 4. Join „normal” (fără broadcast) -> shuffle pe ambele părți
normal_join = transactions.join(users, on="user_id", how="inner")
print("Plan fără broadcast:")
normal_join.explain()

# 5. Join cu broadcast -> shuffle doar pe 'transactions'
broadcast_join = transactions.join(broadcast(users), on="user_id", how="inner")
print("\nPlan cu broadcast:")
broadcast_join.explain()

# 6. Executăm și afișăm rezultatul
print("\nRezultatul join-ului broadcast:")
broadcast_join.show()

# 7. Oprire Spark
spark.stop()
