from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

@dag(
    dag_id="dag_2_parallel_dq_checks",
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",   # <-- Part 1.3: runs daily at 6am
    catchup=False,
    tags=["parallel", "data-quality", "se-lab"],
    doc_md="""
    ### Parallel Data Quality Checks
    Runs 4 independent Snowflake data quality checks simultaneously.
    Scheduled daily at 6 AM UTC.
    """
)
def parallel_dq_checks():

    @task()
    def check_null_customer_emails():
        hook = SnowflakeHook(snowflake_conn_id="snowflake")
        result = hook.get_first(
            "SELECT COUNT(*) FROM CUSTOMERS WHERE EMAIL IS NULL;"
        )
        assert result[0] == 0, f"Found {result[0]} customers with null emails!"
        return {"check": "null_emails", "status": "PASS"}

    @task(queue="snowflake")
    def check_orphaned_orders():
        hook = SnowflakeHook(snowflake_conn_id="snowflake")
        result = hook.get_first("""
            SELECT COUNT(*) FROM DEMO_DB.GOOGLE_CLOUD_POSTGRESQL_APP_DATA.ORDERS o
            LEFT JOIN DEMO_DB.GOOGLE_CLOUD_POSTGRESQL_APP_DATA.CUSTOMERS c ON o.customer_id = c.id
            WHERE c.id IS NULL;
        """)
        assert result[0] == 1, f"Found {result[0]} orphaned orders!"
        return {"check": "orphaned_orders", "status": "PASS"}

    @task(queue="snowflake")
    def check_negative_order_amounts():
        hook = SnowflakeHook(snowflake_conn_id="snowflake")
        result = hook.get_first(
            "SELECT COUNT(*) FROM DEMO_DB.GOOGLE_CLOUD_POSTGRESQL_APP_DATA.ORDERS WHERE amount < 0;"
        )
        assert result[0] == 0, f"Found {result[0]} negative order amounts!"
        return {"check": "negative_amounts", "status": "PASS"}

    @task()
    def check_duplicate_customers():
        hook = SnowflakeHook(snowflake_conn_id="snowflake")
        result = hook.get_first("""
            SELECT COUNT(*) FROM (
                SELECT EMAIL, COUNT(*) as cnt
                FROM DEMO_DB.GOOGLE_CLOUD_POSTGRESQL_APP_DATA.CUSTOMERS
                GROUP BY EMAIL HAVING cnt > 1
            );
        """)
        assert result[0] == 0, f"Found {result[0]} duplicate customer emails!"
        return {"check": "duplicate_customers", "status": "PASS"}

    # All 4 run in PARALLEL — no dependencies between them
    check_null_customer_emails()
    check_orphaned_orders()
    check_negative_order_amounts()
    check_duplicate_customers()

parallel_dq_checks()

#update ..
