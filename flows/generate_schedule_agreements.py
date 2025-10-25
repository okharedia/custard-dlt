import dlt

from datetime import datetime, timedelta, timezone

from dlt.sources.sql_database import sql_database
from prefect.cache_policies import INPUTS
from flows.utils.pkey import get_pkey_from_string
from flows.utils.ssh_tunnel import create_ssh_tunnel
from prefect import flow, task, get_run_logger

from get_events_between import get_events_between


@dlt.resource(
    name="raw_schedule_agreements", 
    write_disposition="merge",
    primary_key=["schedule_id", "start_at", "end_at"]
)
def schedule_agreements(schedules_items):
    """
    For each calendar in the schedules table, this resource generates schedule agreements for the next 10 years.
    """
    for schedule_row in schedules_items:
        calendar_str = schedule_row.get("calendar")
        if not calendar_str:
            continue

        schedule_id = schedule_row.get("parent_id")
        if not schedule_id:
            continue

        range_start = datetime.now(timezone.utc) - timedelta(days=365 * 10)
        range_end = datetime.now(timezone.utc) + timedelta(days=365 * 10)

        try:
            events = get_events_between(calendar_str, range_start, range_end)
        except Exception as e:
            get_run_logger().error(f"Could not parse calendar for schedule {schedule_id}: {e}")
            continue

        for event in events:
            yield {
                "start_at": event.start,
                "end_at": event.end,
                "schedule_id": schedule_id,
            }
@task(cache_policy=INPUTS, retries=1, retry_delay_seconds=1)
def run_dlt_pipeline(connection_string):
    logger = get_run_logger()

    source = sql_database(connection_string).with_resources("schedules")
    agreements_resource = schedule_agreements(source)

    pipeline = dlt.pipeline(
        pipeline_name="custard",
        destination="bigquery",
        dataset_name="main"
    )
    load_info = pipeline.run([agreements_resource])
    logger.info("Schedule agreements loaded.")
    logger.info(load_info)
    logger.info(pipeline.last_trace.last_normalize_info)
    logger.info(f"Pipeline finished in {pipeline.last_trace.finished_at - pipeline.last_trace.started_at}")
    return load_info


@flow(name="generate-schedule-agreements", log_prints=True, version="1.0.0")
def generate_schedule_agreements(
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
    generate_schedule_agreements()