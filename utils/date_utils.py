from datetime import datetime
import pandas as pd

def parse_date(date_val):
    """
    Parses a date value and returns it in dd/mm/yyyy format.
    Handles strings (YYYYMMDD, DD/MM/YYYY), datetime objects, and SAP '00000000'/null values.
    """
    if pd.isna(date_val) or date_val == "00000000" or date_val == "" or date_val == 0:
        return None
        
    dt = None
    if isinstance(date_val, (datetime, pd.Timestamp)):
        dt = date_val
    else:
        date_str = str(date_val).strip()
        # Try YYYYMMDD (SAP)
        try:
            dt = datetime.strptime(date_str, "%Y%m%d")
        except (ValueError, TypeError):
            # Try DD/MM/YYYY
            try:
                dt = datetime.strptime(date_str, "%d/%m/%Y")
            except (ValueError, TypeError):
                # Try letting pandas handle other formats
                try:
                    dt = pd.to_datetime(date_str, dayfirst=True)
                    if pd.isna(dt):
                        dt = None
                except (ValueError, TypeError):
                    dt = None
    
    if dt:
        return dt.strftime("%Y%m%d")
    return None