from datetime import datetime


def parse_yyyy_mm_dd(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return None


def is_valid_date_range(start_date, end_date):
    if start_date and end_date and start_date > end_date:
        return False
    return True


def is_allowed(value, allowed):
    return value in allowed
