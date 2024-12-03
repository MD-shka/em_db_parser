from datetime import datetime, timedelta

ONE_DAY = 1

def get_all_days(date_start: datetime, date_end: datetime) -> list[str]:
    return [(date_start + timedelta(days=i)).strftime('%Y%m%d') for i in
            range((date_end - date_start).days + ONE_DAY)]
