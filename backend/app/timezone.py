from datetime import date, datetime
from zoneinfo import ZoneInfo

CST = ZoneInfo("America/Chicago")


def now_cst() -> datetime:
    return datetime.now(CST).replace(tzinfo=None)


def today_cst() -> date:
    return now_cst().date()
