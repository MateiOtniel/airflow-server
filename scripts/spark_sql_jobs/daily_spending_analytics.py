import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)

from plugins.parser import parse_args
from plugins.writer import write_to_bigquery


def _read_sales(spark: SparkSession, bucket: str, date_str: str) -> DataFrame:
    """Read raw sales CSV for the given date from GCS."""
    path = f"gs://{bucket}/sales/sales_{date_str.replace('-', '_')}.csv"
    sales_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("client_id", IntegerType(), True),
        StructField("transaction_date", TimestampType(), True),
        StructField("amount", DoubleType(), True),
        StructField("merchant", StringType(), True),
        StructField("category", StringType(), True),
    ])
    return (
        spark.read
        .option("header", "true")
        .schema(sales_schema)
        .csv(path)
    )


def _read_accounts(spark: SparkSession, bucket: str, date_str: str) -> DataFrame:
    """Read raw accounts CSV for the given date from GCS."""
    path = f"gs://{bucket}/accounts/accounts_{date_str.replace('-', '_')}.csv"
    acc_schema = StructType([
        StructField("account_id", StringType(), True),
        StructField("client_id", IntegerType(), True),
        StructField("account_type", StringType(), True),
        StructField("opening_balance", DoubleType(), True),
        StructField("open_date", TimestampType(), True),
    ])
    return (
        spark.read
        .option("header", "true")
        .schema(acc_schema)
        .csv(path)
    )


def _create_spending_analytics_with_sql(spark: SparkSession, event_date: str) -> DataFrame:

    analytics_sql = f"""
    with transaction_details as (
        select
            transaction_id,
            client_id,
            amount,
            merchant,
            category,
            hour(transaction_date) as transaction_hour,
            dayofweek(transaction_date) as transaction_day_of_week,
            case
                when hour(transaction_date) between 6 and 11 then 'Morning'
                when hour(transaction_date) between 12 and 17 then 'Afternoon'
                when hour(transaction_date) between 18 and 22 then 'Evening'
                else 'Night'
            end as time_of_day,
            case
                when dayofweek(transaction_date) in (1, 7) then True
                else False
            when as is_weekend
        from transactions
        where event_date = '{event_date}'
        and client_id is not null
        and amount is not null
    ),
    client_spending_summary as (
        select
            client_id,
            sum(amount) as total_spent,
            count(*) as transaction_count,
            round(avg(amount), 2) as avg_transaction,
            max(amount) as max_transaction,
            min(amount) as min_transaction,
            count(distinct merchant) as unique_merchants,
            count(distinct category) as unique_category,
            sum(case when is_weekend = true then amount else 0 end) as weekend_spending,
            sum(case when is_weekend = false then amount else 0 end) as weekday_spending,
            count(case when time_of_day = 'Morning' then 1 end) as morning_transactions,
            count(case when time_of_day = 'Evening' then 1 end) as evening_transactions,
        from transaction_details
        group by client_id
    ),
    category_breakdown as (
        select
            client_id,
            sum(case when category = 'groceries' then amount else 0 end) as groceries_spent,
            sum(case when category = 'food' then amount else 0 end) as food_spent,
            sum(case when category = 'entertainment' then amount else 0 end) as entertainment_spent,
            sum(case when category = 'shopping' then amount else 0 end) as shopping_spent,
            sum(case when category = 'transport' then amount else 0 end) as transport_spent,
            sum(case when category = 'bills' then amount else 0 end) as bills_spent,
            sum(
                case
                    when category not in ('groceries', 'food', 'entertainment', 'shopping', 'transport', 'bills') then amount
                    else 0
                end
            ) as other_spent,
        from transaction_details
        group by client_id
    ),
    merchant_rankings as (
        select
            client_id,
            merchant,
            sum(amount) as spending,
            count(*) as visits,
            row_number() over (
                partition by client_id
                order by sum(amount) desc
            ) as row_num
        from transaction_details
        group by client_id, merchant
    ),
    merchant_preferences as (
        select
            client_id,
            merchant as favorite_merchant,
            spending as favorite_merchant_spending,
            visits as favorite_merchant_visits
        from merchant_rankings
        where row_num = 1
    ),
    spending_patterns as (
        select
            avg(total_spent) as avg_spending_all_clients,
            stddev(total_spent) as stddev_spending,
            percentile_cont(0.25) within group (order by total_spent) as percentile_25,
            percentile_cont(0.75) withing group (order by total_spent) as percentile_75
        from client_spending_summary
    ),
    account_rankings as (
        select
            client_id,
            account_type,
            opening_balance,
            row_number() over (partition by client_id order by opening_balance desc) as row_num
        from accounts_clean
        where date(open_date) <= '{event_date}'
    ),
    client_accounts as (
        select
            client_id,
            sum(opening_balance) as total_opening_balance,
            count(ac.account_id) as account_count,
            ar.account_type as primary_account_type
        from account_rankings ar
        join accounts_clean ac on ar.client_id = ac.client_id
            and date(ac.open_date) <= '{event_date}'
        where ar.row_num = 1
        group by ar.client_id, ar.account_type
    )
    """

    return spark.sql(analytics_sql)


def main():
    args = parse_args(["date", "project", "dataset", "table", "temp-bucket"])

    spark = (
        SparkSession.builder
        .appName("spending_analytics_job")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    logging.info("Started spending_analytics_job")

    event_date = args.date
    bucket = args.temp_bucket

    # 1) Read raw inputs
    sales_raw = _read_sales(spark, bucket, event_date)
    accounts_raw = _read_accounts(spark, bucket, event_date)

    # 2) Clean data and create temporary views for SQL
    sales_clean = (
        sales_raw
        .where(
            (sales_raw.client_id.isNotNull()) &
            (sales_raw.amount.isNotNull()) &
            (sales_raw.amount > 0)
        )
    )
    sales_clean.createOrReplaceTempView("sales_clean")

    accounts_clean = (
        accounts_raw
        .where(
            (accounts_raw.client_id.isNotNull()) &
            (accounts_raw.opening_balance.isNotNull())
        )
    )
    accounts_clean.createOrReplaceTempView("accounts_clean")

    # 3) Create comprehensive analytics using SQL
    analytics_result = _create_spending_analytics_with_sql(spark, event_date)

    # 4) Write to BigQuery
    table_fqn = f"{args.project}.{args.dataset}.{args.table}"
    write_to_bigquery(analytics_result, table_fqn)

    # 5) Log some summary statistics
    total_clients = analytics_result.count()
    vip_clients = analytics_result.filter(
        analytics_result.customer_segment == 'VIP - High Value').count()
    high_risk_clients = analytics_result.filter(
        analytics_result.risk_indicator == 'High Risk').count()
    avg_spending = analytics_result.selectExpr("avg(total_spent)").collect()[0][0]

    logging.info(f"Processed {total_clients} clients for {event_date}")
    logging.info(f"VIP clients: {vip_clients} ({vip_clients / total_clients * 100:.1f}%)")
    logging.info(f"High risk clients: {high_risk_clients} ({high_risk_clients / total_clients * 100:.1f}%)")
    logging.info(f"Average spending: ${avg_spending:.2f}")

    spark.stop()
    logging.info("Finished spending_analytics_job")


if __name__ == '__main__':
    main()
