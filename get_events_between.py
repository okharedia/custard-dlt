"""Simple and robust iCal processing using ical-library."""

from datetime import datetime, timedelta, timezone
from typing import List
from icalendar import Calendar
from recurring_ical_events import of as recurring_ical_events_of
from pydantic import BaseModel

class Event(BaseModel):
    start: datetime
    end: datetime

def get_events_between(
    ical_string: str, range_start: datetime = None, range_end: datetime = None
) -> List[Event]:
    calendar = Calendar.from_ical(ical_string)

    if range_start is None:
        earliest_start = None
        for component in calendar.walk("VEVENT"):
            dtstart = component.get("dtstart").dt

            # dtstart can be a date, so we convert it to a datetime
            if not isinstance(dtstart, datetime):
                dtstart = datetime.combine(dtstart, datetime.min.time())

            if earliest_start is None or dtstart < earliest_start:
                earliest_start = dtstart
        range_start = earliest_start

    if range_end is None:
        latest_end = None
        for component in calendar.walk("VEVENT"):
            end_date = None
            if "RRULE" in component:
                if "UNTIL" in component["RRULE"]:
                    end_date = component["RRULE"]["UNTIL"][0]
            else:
                if "DTEND" in component:
                    end_date = component.get("dtend").dt
                elif "DURATION" in component:
                    dtstart = component.get("dtstart").dt
                    duration = component.get("duration").dt
                    end_date = dtstart + duration

            if end_date:
                if not isinstance(end_date, datetime):
                    end_date = datetime.combine(end_date, datetime.max.time())
                
                # Ensure timezone awareness for comparison
                if end_date.tzinfo is None:
                    end_date = end_date.replace(tzinfo=timezone.utc)

                if latest_end is None or end_date > latest_end:
                    latest_end = end_date
        range_end = latest_end


    events = recurring_ical_events_of(calendar).between(range_start, range_end)
    date_ranges = []

    for event in events:
        date_ranges.append(Event(start=event.start, end=event.end))

    return date_ranges
