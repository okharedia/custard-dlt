import dlt

from datetime import datetime, timedelta, timezone

from dlt.sources.sql_database import sql_database
from prefect import flow, get_run_logger

from get_events_between import get_events_between


@dlt.resource(
    name="schedule_agreements", 
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


@flow(name="generate-schedule-agreements", log_prints=True, version="1.0.0")
def generate_schedule_agreements(
    
):
    logger = get_run_logger()
    
    source = sql_database().with_resources("schedules")
    agreements_resource = schedule_agreements(source)

    pipeline = dlt.pipeline(
        pipeline_name="custard",
        destination="bigquery"
    )
    load_info = pipeline.run([agreements_resource])

    logger.info("Schedule agreements loaded.")
    logger.info(load_info)
    logger.info(pipeline.last_trace.last_normalize_info)
    logger.info(f"Pipeline finished in {pipeline.last_trace.finished_at - pipeline.last_trace.started_at}")
    
    logger.info("DLT pipeline finished")
    logger.info("ETL finished successfully!")


if __name__ == "__main__":
    generate_schedule_agreements()