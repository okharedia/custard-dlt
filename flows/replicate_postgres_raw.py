import dlt
from dlt.sources.sql_database import sql_database
from prefect import flow, task, get_run_logger
from prefect.cache_policies import INPUTS

from flows.utils.pkey import get_pkey_from_string
from flows.utils.ssh_tunnel import create_ssh_tunnel


@task(cache_policy=INPUTS, retries=1, retry_delay_seconds=1)
def run_dlt_pipeline(connection_string):
    logger = get_run_logger()

    source = sql_database(connection_string, backend="pandas")

    pipeline = dlt.pipeline(
        pipeline_name="custard",
        destination="duckdb",
        dataset_name="main"
    )
    
    # Apply table naming to all resources in the source
    for resource_name in source.resources.keys():
        source.resources[resource_name].apply_hints(table_name=f"raw_{resource_name}")
    
    load_info = pipeline.run(source, write_disposition="merge")
    logger.info("All tables loaded.")
    logger.info(load_info)
    logger.info(pipeline.last_trace.last_normalize_info)
    logger.info(f"Pipeline finished in {pipeline.last_trace.finished_at - pipeline.last_trace.started_at}")
    return load_info


@flow(name="replicate-postgres-raw", log_prints=True, version="1.0.0")
def replicate_postgres_raw(
    remote_db_host: str = "127.0.0.1",
    remote_db_port: int = 5433,
    ssh_port: int = 22,
    db_name: str = "postgres"
):
    logger = get_run_logger()
    
    db_username = dlt.secrets["sources.sql_database.credentials.username"]
    db_password = dlt.secrets["sources.sql_database.credentials.password"]
    ssh_username = dlt.secrets["ssh.username"]
    ssh_host = dlt.secrets["ssh.host"]
    private_key = dlt.secrets["ssh.private_key"]
    
    pkey = get_pkey_from_string(private_key)

    tunnel = None
    try:
        tunnel = create_ssh_tunnel(
            ssh_host, ssh_port, ssh_username, pkey, remote_db_host, remote_db_port
        )
        
        connection_string = f"postgresql://{db_username}:{db_password}@{tunnel.local_bind_host}:{tunnel.local_bind_port}/{db_name}"
        
        run_dlt_pipeline(connection_string)
        logger.info("DLT pipeline finished")
    finally:
        if tunnel:
            tunnel.stop()
            logger.info("SSH tunnel stopped.")

    logger.info("ETL finished successfully!")


if __name__ == "__main__":
    replicate_postgres_raw()