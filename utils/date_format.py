from datetime import datetime


def get_date_from_str(fecha_str, format="%Y-%m-%d"):
    return datetime.strptime(fecha_str, format)


def get_timestamp_from_datetime(date_value):
    datetime_value = datetime(year=date_value.year, month=date_value.month, day=date_value.day)
    return int(datetime_value.timestamp())


def get_timestamp_from_date_format(str_date_value, format="%Y-%m-%d"):
    return get_timestamp_from_datetime(get_date_from_str(str_date_value, format))

