from datetime import datetime, timedelta
from typing import Tuple


def get_date_range(days_back: int = 30) -> Tuple[str, str]:
    """Generate date range in BCR format (YYYY-M-D)"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    return (
        start_date.strftime("%Y-%-m-%-d"),
        end_date.strftime("%Y-%-m-%-d")
    )


def get_partition_path(date: datetime) -> str:
    """Generate partition path YYYY/MM/DD"""
    return date.strftime("%Y/%m/%d")
