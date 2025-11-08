from langchain_core.tools import tool

@tool("fetch_time", return_direct=False)
def fetch_time(timezone: str | None = None) -> str:
    """Return current time (ISO). timezone = IANA name."""
    import datetime, zoneinfo
    try:
        now = datetime.datetime.now(zoneinfo.ZoneInfo(timezone)) if timezone else datetime.datetime.now(datetime.timezone.utc)
        return now.isoformat()
    except Exception as e:
        return f"error: {e}"