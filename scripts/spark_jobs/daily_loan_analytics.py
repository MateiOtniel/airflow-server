import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)

from plugins.parser import parse_args
from plugins.writer import write_to_bigquery


def _read_loans(spark: SparkSession, bucket: str, date_str: str) -> DataFrame:
    """Read raw loans CSV for the given date from GCS."""
    path = f"gs://{bucket}/loans/loans_{date_str.replace('-', '_')}.csv"
    loan_schema = StructType([
        StructField("loan_id", StringType(), True),
        StructField("client_id", IntegerType(), True),
        StructField("principal_amount", DoubleType(), True),
        StructField("interest_rate", DoubleType(), True),
        StructField("start_date", TimestampType(), True),
        StructField("term_months", IntegerType(), True),
        StructField("loan_type", StringType(), True),
    ])
    return (
        spark.read
        .option("header", "true")
        .schema(loan_schema)
        .csv(path)
    )


def _read_delay_fees(spark: SparkSession, bucket: str, date_str: str) -> DataFrame:
    """Read raw delay fees CSV for the given date from GCS."""
    path = f"gs://{bucket}/delay_fees/delay_fees_{date_str.replace('-', '_')}.csv"
    fee_schema = StructType([
        StructField("fee_id", StringType(), True),
        StructField("client_id", IntegerType(), True),
        StructField("loan_id", StringType(), True),
        StructField("fee_amount", DoubleType(), True),
        StructField("fee_date", TimestampType(), True),
        StructField("days_delayed", IntegerType(), True),
    ])
    return (
        spark.read
        .option("header", "true")
        .schema(fee_schema)
        .csv(path)
    )


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


def _create_loan_analytics_with_sql(spark: SparkSession, event_date: str) -> DataFrame:
    """
    TASK DESCRIPTION:
    ================
    This function creates a loan analytics report that combines loan data,
    fees, and client spending to provide insights about loan performance.

    The analytics pipeline performs:
    1. Basic loan calculations (monthly payment, total interest)
    2. Fee analysis (how much extra money we collect from late payments)
    3. Client spending patterns (how much clients spend per day)
    4. Simple risk scoring (combining payment delays and loan size)

    Output: A table showing each loan with financial metrics and basic risk assessment.
    """

    analytics_sql = f"""
    with loan_basics as (
        select
            loan_id,
            client_id,
            principal_amount,
            interest_rate,
            term_months,
            loan_type,
            date(start_date) as loan_date,
            round(principal_amount * (interest_rate / 100) * (term_months / 12.0), 2) as total_interest,
            round(principal_amount * ((interest_rate / 100 /12) * power((1 + interest_rate / 100 / 12), term_months))
                / (power(1 + (interest_rate / 100 / 12), term_months) - 1), 2) as monthly_payment,
            case
                when principal_amount < 10000 then 'Small Loan'
                when principal_amount < 30000 then 'Medium Loan'
                else 'Large Loan'
            end as loan_size,
            case
                when interest_rate <= 5 then 'Low Rate'
                when interest_rate <= 8 then 'Normal Rate'
                else 'High Rate'
            end as rate_category
        from loans_clean
        where date(start_date) = '{event_date}'
    ),
    fee_summary as (
        select
            loan_id,
            client_id,
            count(*) as late_payment_count,
            sum(fee_amount) as total_fees,
            round(avg(fee_amount), 2) as avg_fee,
            max(days_delayed) as worst_delay_days,
            round(avg(days_delayed), 1) as avg_delay_days,
            case
                when max(days_delayed) > 30 then 'Bad at Paying'
                when max(days_delayed) > 10 then 'Sometimes Forgets'
                else 'Pretty Good'
            end as payment_behavior
        from fees_clean
        where date(fee_date) = '{event_date}'
        group by loan_id, client_id
    ),
    client_activity as (
        select
            client_id,
            count(*) as transactions_today,
            sum(amount) as total_spending,
            round(avg(amount), 2) as avg_per_transaction,
            round(sum(case when category in ('groceries', 'food') then amount else 0 end), 2) as food_spending,
            round(sum(case when category in ('entertainment', 'shopping') then amount else 0 end), 2) as fun_spending,
            case
                when sum(amount) > 300 then 'Spends Lots'
                when sum(amount) > 100 then 'Spends Normal'
                else 'Saves Money'
            end as spending_level
        from sales_clean
        where date(transaction_date) = '{event_date}'
        group by client_id
    ),
    loan_analysis as (
        select
            lb.*,
            coalesce(fs.late_payment_count, 0) as times_late,
            coalesce(fs.total_fees, 0.0) as fees_collected,
            coalesce(fs.avg_fee, 0.0) as avg_fee_amount,
            coalesce(fs.worst_delay_days, 0) as max_days_late,
            coalesce(fs.payment_behavior, 'Always On Time') as payment_history,
            lb.total_interest + coalesce(fs.total_fees, 0.0) as total_revenue,
            round((lb.total_interest + coalesce(fs.total_fees, 0.0)) / lb.principal_amount, 3) as profit_ratio,
            coalesce(ca.total_spending, 0.0) as daily_spending,
            coalesce(ca.transactions_today, 0) as daily_transactions,
            coalesce(ca.spending_level, 'Unknown') as client_type,
            coalesce(ca.food_spending, 0.0) as essential_spending,
            coalesce(ca.fun_spending, 0.0) as luxury_spending,
            case
                when coalesce(ca.total_spending, 0) = 0 then null
                else round(lb.monthly_payment / (coalesce(ca.total_spending, 0) * 30) * 100, 1)
            end as payment_burden_percent
        from loan_basics lb
        left join fee_summary fs on lb.loan_id = fs.loan_id
        left join client_activity ca on lb.client_id = ca.client_id
    ),
    final_results as (
        select
            '{event_date}' as report_date,
            loan_id,
            client_id,
            loan_type,
            principal_amount,
            interest_rate,
            monthly_payment,
            loan_size,
            rate_category,
            total_interest,
            fees_collected,
            total_revenue,
            profit_ratio,
            times_late,
            max_days_late,
            payment_history,
            daily_spending,
            client_type,
            payment_burden_percent,
            case
                when payment_history = 'Bad at Paying' then 3
                when payment_history = 'Sometimes Forgets' then 1
                else 0
            end +
            case
                when rate_category = 'High Rate' then 3
                when rate_category = 'Normal Rate' then 1
                else 0
            end +
            case
                when payment_burden_percent > 40 then 2
                when payment_burden_percent > 25 then 1
                else 0
            end +
            case
                when loan_size = 'Large Loan' then 2
                else 0
            end as risk_score,
            case
                when (case
                    when payment_history = 'Bad at Paying' then 3
                    when payment_history = 'Sometimes Forgets' then 1
                    else 0
                end +
                case
                    when rate_category = 'High Rate' then 3
                    when rate_category = 'Normal Rate' then 1
                    else 0
                end +
                case
                    when payment_burden_percent > 40 then 2
                    when payment_burden_percent > 25 then 1
                    else 0
                end +
                case
                    when loan_size = 'Large Loan' then 2
                    else 0
                end) >= 6 then 'Risky'
                when (case
                    when payment_history = 'Bad at Paying' then 3
                    when payment_history = 'Sometimes Forgets' then 1
                    else 0
                end +
                case
                    when rate_category = 'High Rate' then 3
                    when rate_category = 'Normal Rate' then 1
                    else 0
                end +
                case
                    when payment_burden_percent > 40 then 2
                    when payment_burden_percent > 25 then 1
                    else 0
                end +
                case
                    when loan_size = 'Large Loan' then 2
                    else 0
                end) >= 3 then 'Kinda Risky'
                else 'Pretty Safe'
            end as risk_level,
            case
                when profit_ratio >= 0.20 then 'Makes Lots of Money'
                when profit_ratio >= 0.15 then 'Makes Good Money'
                when profit_ratio >= 0.10 then 'Makes Some Money'
                else 'Makes Little Money'
            end as profit_level,
            case
                when profit_ratio >= 0.20 and payment_history in ('Always On Time', 'Pretty Good') then 'Great Customer - Give Them More!'
                when payment_history = 'Bad at Paying' and profit_ratio < 0.15 then 'Watch Out - Might Be Trouble'
                when profit_ratio >= 0.15 then 'Good Loan - Keep It'
                else 'Normal Loan'
            end as recommendation
        from loan_analysis
    )
    select * from final_results
    order by risk_score desc, profit_ratio desc
    """

    return spark.sql(analytics_sql)


def main():
    args = parse_args(["date", "project", "dataset", "table", "temp-bucket"])

    spark = (
        SparkSession.builder
        .appName("loan_analytics_job")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    logging.info("Started loan_analytics_job")

    event_date = args.date
    bucket = args.temp_bucket

    # 1) Read raw inputs
    loans_raw = _read_loans(spark, bucket, event_date)
    fees_raw = _read_delay_fees(spark, bucket, event_date)
    sales_raw = _read_sales(spark, bucket, event_date)

    # 2) Clean data and create temporary views for SQL
    loans_clean = (
        loans_raw
        .where(
            (loans_raw.client_id.isNotNull()) &
            (loans_raw.principal_amount.isNotNull()) &
            (loans_raw.principal_amount > 0) &
            (loans_raw.interest_rate.isNotNull()) &
            (loans_raw.interest_rate > 0)
        )
    )
    loans_clean.createOrReplaceTempView("loans_clean")

    fees_clean = (
        fees_raw
        .where(
            (fees_raw.client_id.isNotNull()) &
            (fees_raw.loan_id.isNotNull()) &
            (fees_raw.fee_amount.isNotNull()) &
            (fees_raw.fee_amount > 0)
        )
    )
    fees_clean.createOrReplaceTempView("fees_clean")

    sales_clean = (
        sales_raw
        .where(
            (sales_raw.client_id.isNotNull()) &
            (sales_raw.amount.isNotNull()) &
            (sales_raw.amount > 0)
        )
    )
    sales_clean.createOrReplaceTempView("sales_clean")

    # 3) Create comprehensive analytics using SQL
    analytics_result = _create_loan_analytics_with_sql(spark, event_date)

    # 4) Write to BigQuery
    table_fqn = f"{args.project}.{args.dataset}.{args.table}"
    write_to_bigquery(analytics_result, table_fqn)

    # 5) Log some summary statistics
    total_loans = analytics_result.count()
    high_risk_loans = analytics_result.filter(
        analytics_result.risk_level.isin(['Risky'])).count()
    avg_risk_score = analytics_result.selectExpr("avg(risk_score)").collect()[0][0]

    logging.info(f"Processed {total_loans} loans for {event_date}")
    logging.info(f"High risk loans: {high_risk_loans} ({high_risk_loans / total_loans * 100:.1f}%)")
    logging.info(f"Average risk score: {avg_risk_score:.2f}")

    spark.stop()
    logging.info("Finished loan_analytics_job")


if __name__ == '__main__':
    main()
