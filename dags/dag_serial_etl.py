from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import logging

@dag(
    dag_id="dag_serial_etl",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["serial", "se-lab"],
    doc_md="""
    ### Serial ETL Pipeline
    Validates, transforms, and loads customer data from GCP Postgres → Snowflake.
    Tasks run one after another in strict sequence.
    """
)
def serial_etl_pipeline():

    @task()
    def extract_from_postgres():
        """Pull customer count from GCP PostgreSQL"""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id="gcp_postgres")
        result = hook.get_records("SELECT COUNT(*) FROM app_data.customers;")
        logging.info(f"Customer count: {result[0][0]}")
        return {"customer_count": result[0][0]}

    @task()
    def validate_data(stats: dict):
        """Ensure data quality thresholds are met"""
        if stats["customer_count"] == 0:
            raise ValueError("No customers found — aborting pipeline!")
        logging.info(f"Validation passed. {stats['customer_count']} customers found.")
        return stats

    @task()
    def transform_data(stats: dict):
        """Simulate a transformation step"""
        transformed = {
            **stats,
            "transformed_at": str(datetime.utcnow()),
            "pipeline": "serial_etl"
        }
        logging.info(f"Transformation complete: {transformed}")
        return transformed

    @task()
    def load_to_snowflake(payload: dict):
        """Log the load confirmation to Snowflake audit table"""
        hook = SnowflakeHook(snowflake_conn_id="snowflake")
        hook.run(f"""
            INSERT INTO AUDIT.PIPELINE_LOG (pipeline_name, record_count, loaded_at)
            VALUES ('{payload["pipeline"]}', {payload["customer_count"]}, CURRENT_TIMESTAMP());
        """)
        logging.info("Load to Snowflake complete.")

    # Serial dependency chain
    raw = extract_from_postgres()
    validated = validate_data(raw)
    transformed = transform_data(validated)
    load_to_snowflake(transformed)

serial_etl_pipeline()

