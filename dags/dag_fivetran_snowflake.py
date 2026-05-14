from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.empty import EmptyOperator
import requests, logging
from datetime import datetime

FIVETRAN_API_KEY    = "{{ var.value.fivetran_api_key }}"
FIVETRAN_API_SECRET = "{{ var.value.fivetran_api_secret }}"
FIVETRAN_CONNECTOR  = "{{ var.value.fivetran_connector_id }}"

@dag(
    dag_id="dag_4_fivetran_to_snowflake",
    start_date=datetime(2024, 1, 1),
    schedule="0 5 * * *",   # Daily at 5am — runs before DQ checks
    catchup=False,
    tags=["fivetran", "snowflake", "realistic", "se-lab"],
    doc_md="""
    ### Fivetran → Snowflake Orchestration Pipeline
    
    **Business Use Case:** Nightly sync of customer/orders data from 
    GCP PostgreSQL via Fivetran into Snowflake, followed by 
    transformation and reporting mart refresh.
    
    **Flow:**
    1. Trigger Fivetran connector sync (Postgres → Snowflake)
    2. Wait for sync completion  
    3. Run dbt-style transformations in Snowflake
    4. Refresh reporting views
    5. Update data freshness audit log
    """
)
def fivetran_to_snowflake():

    @task(queue="snowflake")   # <-- Part 3.2: different worker queue!
    def trigger_fivetran_sync():
        """Trigger the Fivetran PostgreSQL → Snowflake connector"""
        resp = requests.post(
            f"https://api.fivetran.com/v1/connectors/{FIVETRAN_CONNECTOR}/sync",
            auth=(FIVETRAN_API_KEY, FIVETRAN_API_SECRET)
        )
        resp.raise_for_status()
        logging.info(f"Fivetran sync triggered: {resp.json()}")
        return resp.json().get("data", {}).get("id")

    @task(queue="snowflake")
    def wait_for_fivetran_sync(sync_id: str):
        """Poll Fivetran until sync completes"""
        import time
        for _ in range(30):  # max 30 polls
            resp = requests.get(
                f"https://api.fivetran.com/v1/connectors/{FIVETRAN_CONNECTOR}",
                auth=(FIVETRAN_API_KEY, FIVETRAN_API_SECRET)
            )
            status = resp.json()["data"]["status"]["sync_state"]
            logging.info(f"Fivetran sync state: {status}")
            if status == "ready":
                return "COMPLETE"
            elif status == "error":
                raise Exception("Fivetran sync failed!")
            time.sleep(30)
        raise TimeoutError("Fivetran sync timed out after 15 minutes")

    @task()
    def transform_customer_metrics():
        """Build customer summary metrics in Snowflake"""
        hook = SnowflakeHook(snowflake_conn_id="snowflake")
        hook.run("""
            CREATE OR REPLACE TABLE ANALYTICS.CUSTOMER_METRICS AS
            SELECT 
                c.id,
                c.name,
                c.email,
                c.created_at,
                COUNT(o.id)        AS total_orders,
                SUM(o.amount)      AS lifetime_value,
                MAX(o.created_at)  AS last_order_date,
                AVG(o.amount)      AS avg_order_value
            FROM CUSTOMERS c
            LEFT JOIN ORDERS o ON c.id = o.customer_id
            GROUP BY 1, 2, 3, 4;
        """)
        return "customer_metrics_done"

    @task()
    def transform_order_metrics():
        """Build order summary metrics in Snowflake"""
        hook = SnowflakeHook(snowflake_conn_id="snowflake")
        hook.run("""
            CREATE OR REPLACE TABLE ANALYTICS.ORDER_METRICS AS
            SELECT
                DATE_TRUNC('day', created_at) AS order_date,
                COUNT(*)                       AS total_orders,
                SUM(amount)                    AS daily_revenue,
                AVG(amount)                    AS avg_order_value,
                COUNT(DISTINCT customer_id)    AS unique_customers
            FROM ORDERS
            GROUP BY 1
            ORDER BY 1 DESC;
        """)
        return "order_metrics_done"

    @task()
    def refresh_reporting_views(cust_done: str, ord_done: str):
        """Refresh the executive reporting mart"""
        hook = SnowflakeHook(snowflake_conn_id="snowflake")
        hook.run("""
            CREATE OR REPLACE VIEW REPORTING.EXECUTIVE_SUMMARY AS
            SELECT
                om.order_date,
                om.total_orders,
                om.daily_revenue,
                om.unique_customers,
                SUM(om.daily_revenue) OVER (ORDER BY om.order_date) AS cumulative_revenue
            FROM ANALYTICS.ORDER_METRICS om
            ORDER BY om.order_date DESC;
        """)
        return "views_refreshed"

    @task()
    def update_audit_log(sync_status: str, views_status: str):
        """Record pipeline run in audit log"""
        hook = SnowflakeHook(snowflake_conn_id="snowflake")
        hook.run(f"""
            INSERT INTO AUDIT.PIPELINE_LOG 
                (pipeline_name, fivetran_status, views_status, loaded_at)
            VALUES 
                ('fivetran_to_snowflake', '{sync_status}', '{views_status}', CURRENT_TIMESTAMP());
        """)

    # DAG flow
    sync_id     = trigger_fivetran_sync()
    sync_status = wait_for_fivetran_sync(sync_id)
    cust        = transform_customer_metrics()
    ord_m       = transform_order_metrics()
    sync_status >> [cust, ord_m]    # transforms only after sync completes
    views       = refresh_reporting_views(cust, ord_m)
    update_audit_log(sync_status, views)

fivetran_to_snowflake()
