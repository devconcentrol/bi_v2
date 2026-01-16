from datetime import datetime
import pandas as pd

def parse_date(date_val):
    """
    Parses a date value and returns it in YYYYMMDD format.
    Handles strings (YYYYMMDD, DD/MM/YYYY), datetime objects, and SAP '00000000'/null values.
    """
    if pd.isna(date_val) or date_val in ("00000000", "", 0, "0"):
        return None
        
    if isinstance(date_val, (datetime, pd.Timestamp)):
        return date_val.strftime("%Y%m%d")

    date_str = str(date_val).strip()
    
    # Fast paths for common formats
    if len(date_str) == 8 and date_str.isdigit():
        try:
            return datetime.strptime(date_str, "%Y%m%d").strftime("%Y%m%d")
        except ValueError:
            pass
            
    if "/" in date_str:
        try:
            return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y%m%d")
        except ValueError:
            pass
            
    # Fallback to pandas
    dt = pd.to_datetime(date_str, dayfirst=True, errors='coerce')
    if pd.notna(dt):
        return dt.strftime("%Y%m%d")
        
    return None