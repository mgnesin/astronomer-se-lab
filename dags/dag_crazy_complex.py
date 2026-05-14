from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
from datetime import datetime

@dag(
    dag_id="dag_3_crazy_complex",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["complex", "se-lab"],
    doc_md="### The Crazy DAG — Maximum graph complexity for demo purposes!"
)
def crazy_complex_dag():

    # Layer 0: Entry
    start = EmptyOperator(task_id="start")

    # Layer 1: Fan out to 3 extractors
    @task()
    def extract_customers(): return "customers"
    
    @task()
    def extract_orders(): return "orders"
    
    @task()
    def extract_products(): return "products"

    # Layer 2: Each feeds 2 validators (6 tasks)
    @task()
    def validate_cust_schema(d): return d
    
    @task()
    def validate_cust_volume(d): return d
    
    @task()
    def validate_ord_schema(d): return d
    
    @task()
    def validate_ord_dates(d): return d
    
    @task()
    def validate_prod_sku(d): return d
    
    @task()
    def validate_prod_price(d): return d

    # Layer 3: Cross-stream joins (creates cross-edges)
    @task()
    def join_cust_orders(c, o): return "cust_ord"
    
    @task()
    def join_orders_products(o, p): return "ord_prod"
    
    @task()
    def join_all(c, o): return "all_joined"

    # Layer 4: Parallel transforms
    @task()
    def transform_revenue(d): return d
    
    @task()
    def transform_cohorts(d): return d
    
    @task()
    def transform_inventory(d): return d
    
    @task()
    def transform_clv(d): return d

    # Layer 5: Quality gate
    @task()
    def quality_gate(r, c, i, clv):
        return all([r, c, i, clv])

    # Layer 6: Parallel loads
    @task()
    def load_snowflake(d): return "sf_done"

    @task()
    def load_gcs_archive(d): return "gcs_done"
    
    @task()
    def load_reporting_mart(d): return "mart_done"

    # Layer 7: Notifications (fan-out from 3 loads)
    @task()
    def notify_data_team(a, b, c): return "notified"
    
    @task()
    def notify_bi_team(d): return "notified"
    
    @task()
    def update_data_catalog(a, b, c): return "cataloged"

    end = EmptyOperator(task_id="end")

    # Wire it all up
    c = extract_customers()
    o = extract_orders()
    p = extract_products()

    vc1 = validate_cust_schema(c); vc2 = validate_cust_volume(c)
    vo1 = validate_ord_schema(o);  vo2 = validate_ord_dates(o)
    vp1 = validate_prod_sku(p);    vp2 = validate_prod_price(p)

    co  = join_cust_orders(vc1, vo1)
    op  = join_orders_products(vo2, vp1)
    joined = join_all(co, op)

    r   = transform_revenue(joined)
    ch  = transform_cohorts(co)
    inv = transform_inventory(op)
    clv = transform_clv(joined)

    qg  = quality_gate(r, ch, inv, clv)

    sf  = load_snowflake(qg)
    gcs = load_gcs_archive(qg)
    mart= load_reporting_mart(qg)

    nt  = notify_data_team(sf, gcs, mart)
    nb  = notify_bi_team(mart)
    cat = update_data_catalog(sf, gcs, mart)

    start >> [c, o, p]
    [nt, nb, cat] >> end

crazy_complex_dag()