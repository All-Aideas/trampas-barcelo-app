from datetime import datetime
import pytz


time_zone = pytz.timezone('America/Argentina/Salta')
utc = pytz.utc


def get_datetime_from_str(fecha_str, format="%Y-%m-%d"):
    date_value = datetime.strptime(fecha_str, format)
    datetime_value = datetime(year=date_value.year, month=date_value.month, day=date_value.day)
    datetime_tz_value = time_zone.localize(datetime_value, is_dst=True)
    return datetime_tz_value

def get_str_format_from_date_str(date_str, format_old="%Y-%m-%d", format_new="%d/%m/%Y"):
    datetime_value = get_datetime_from_str(date_str, format_old)
    return datetime_value.strftime(format_new)

def get_timestamp_from_datetime(date_value):
    datetime_value = datetime(year=date_value.year, month=date_value.month, day=date_value.day)
    return int(datetime_value.timestamp())


def get_str_date_tz_from_timestamp(int_timestamp_value, format="%Y-%m-%d"):
    """
    Output:
    - Date: string. Format: %Y-%m-%d
    """
    tz_argentina = pytz.timezone('America/Argentina/Buenos_Aires')
    utc_dt = datetime.utcfromtimestamp(int_timestamp_value)
    argentina_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(tz_argentina)
    formatted_datetime = argentina_dt.strftime(format)
    return formatted_datetime

def get_datetime(format='%Y-%m-%d %H:%M:%S'):
    return datetime.now(pytz.timezone('America/Argentina/Buenos_Aires')).strftime(format)
