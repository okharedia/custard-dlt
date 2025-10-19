import dlt
import os
from sshtunnel import SSHTunnelForwarder
from dlt.sources.sql_database import sql_database
from sqlalchemy import create_engine
import sqlalchemy as sa
import uuid
from datetime import datetime, timedelta, timezone
from get_events_between import get_events_between
from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE
from prefect.server.schemas.schedules import CronSchedule
import io
from paramiko import RSAKey, Ed25519Key, ECDSAKey, DSSKey


@dlt.resource(name="schedule_agreements", write_disposition="replace")
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


@task
def build_dlt_resources(connection_string):
    logger = get_run_logger()
    engine = create_engine(connection_string)
    inspector = sa.inspect(engine)
    all_table_names = inspector.get_table_names()
    source = sql_database(connection_string)
    all_resources = []

    if "schedules" in all_table_names:
        all_table_names.remove("schedules")
        schedules_resource = source.resources["schedules"]
        schedules_resource.apply_hints(write_disposition="merge")
        agreements = schedule_agreements(schedules_resource)
        agreements.apply_hints(write_disposition="replace")
        all_resources.extend([schedules_resource, agreements])

    for table_name in all_table_names:
        columns = inspector.get_columns(table_name)
        pk_constraint = inspector.get_pk_constraint(table_name)
        pk_columns = pk_constraint['constrained_columns'] if pk_constraint else []
        
        range_columns = []
        for col in columns:
            if 'TSTZRANGE' in str(col['type']).upper() or 'TZRANGE' in str(col['type']).upper():
                range_columns.append(col['name'])

        if not range_columns:
            resource = source.resources[table_name]
            resource.apply_hints(
                write_disposition="merge",
                primary_key=pk_columns if pk_columns else None
            )
            all_resources.append(resource)
        else:
            logger.info(f"Creating custom query resource for table '{table_name}'")
            select_parts = []
            for col in columns:
                if col['name'] in range_columns:
                    select_parts.append(f"lower({col['name']}) as {col['name']}_start_at")
                    select_parts.append(f"upper({col['name']}) as {col['name']}_end_at")
                else:
                    select_parts.append(col['name'])
            
            query = f"SELECT {', '.join(select_parts)} FROM {table_name}"

            def _execute_query(q=query, conn_str=connection_string):
                engine = create_engine(conn_str)
                with engine.connect() as connection:
                    result = connection.execute(sa.text(q))
                    for row in result.mappings():
                        yield dict(row)

            custom_resource = dlt.resource(
                _execute_query,
                name=table_name,
                write_disposition="merge",
                primary_key=pk_columns if pk_columns else None
            )
            all_resources.append(custom_resource)
    
    return all_resources


@task(cache_policy=NO_CACHE)
def run_dlt_pipeline(resources):
    logger = get_run_logger()
    pipeline = dlt.pipeline(
        pipeline_name="custard",
        destination="duckdb",
        dataset_name="raw"
    )
    load_info = pipeline.run(resources)
    logger.info("All tables loaded.")
    logger.info(load_info)
    logger.info(pipeline.last_trace.last_normalize_info)
    logger.info(f"Pipeline finished in {pipeline.last_trace.finished_at - pipeline.last_trace.started_at}")
    return load_info


@flow(name="replicate-postgres-to-duckdb", log_prints=True)
def replicate_postgres_to_duckdb():
    logger = get_run_logger()
    
    remote_db_host = "127.0.0.1"
    remote_db_port = 5433
    ssh_port = 22
    db_username = dlt.secrets["sources.sql_database.credentials.username"]
    db_password = dlt.secrets["sources.sql_database.credentials.password"]
    ssh_username = dlt.secrets["ssh.username"]
    ssh_host = dlt.secrets["ssh.host"]
    private_key = dlt.secrets["ssh.private_key"]
    pkey = None
    if private_key:
        key_classes = [RSAKey, Ed25519Key, ECDSAKey, DSSKey]
        for key_class in key_classes:
            try:
                pkey = key_class.from_private_key(io.StringIO(private_key))
                break
            except Exception:
                continue

    with SSHTunnelForwarder(
        (ssh_host, int(ssh_port)),
        ssh_username=ssh_username,
        ssh_pkey=pkey,
        remote_bind_address=(remote_db_host, remote_db_port),
    ) as tunnel:
        logger.info("Tunnel started")
        connection_string = f"postgresql://{db_username}:{db_password}@{tunnel.local_bind_host}:{tunnel.local_bind_port}/postgres"
        
        resources = build_dlt_resources(connection_string)
        run_dlt_pipeline(resources)
        logger.info("DLT pipeline finished")

    logger.info("ETL finished successfully!")