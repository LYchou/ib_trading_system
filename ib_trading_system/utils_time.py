import pytz
from datetime import datetime


def now(timezone:str=''):
    """
    Get the current time in the specified timezone. 
    If no timezone is provided, it returns the system's local time.

    Parameters:
    - timezone (str): The name of the desired timezone (e.g., "Asia/Taipei", "UTC"). 
                      If left empty, it defaults to the system's local time.

    Returns:
    - datetime: The current datetime object in the specified or system's timezone.

    Example:
    >>> now("Asia/Taipei")
    2023-09-05 20:00:00+08:00

    >>> now()
    2023-09-05 12:00:00 (assuming the system's timezone is UTC)
    """
    tz = pytz.timezone(timezone) if timezone else None
    now = datetime.now(tz=tz)
    return now


def timezone_convert(time_str, time_format, current_tz, target_tz):
    """
    Convert a datetime string from one timezone to another.

    Parameters:
    - time_str (str): Datetime string.
    - time_format (str): Format of the datetime string.
    - current_tz (str): Current timezone of the datetime string.
    - target_tz (str): Target timezone to convert to.

    Returns:
    - datetime: Converted datetime object without timezone information.
    """
    
    # Parsing the time_str using the given time_format
    naive_dt = datetime.strptime(time_str, time_format)
    
    # Assigning the current timezone to the datetime object
    current_timezone = pytz.timezone(current_tz)
    aware_dt = current_timezone.localize(naive_dt)
    
    # Converting to the target timezone
    target_timezone = pytz.timezone(target_tz)
    target_dt = aware_dt.astimezone(target_timezone)
    
    # Returning the datetime object without timezone information
    return target_dt.replace(tzinfo=None)


def naive_timezone_convert(naive_dt, current_tz, target_tz):
    """
    Convert a naive datetime object from one timezone to another.

    Parameters:
    - naive_dt (datetime): Naive datetime object.
    - current_tz (str): Current timezone of the datetime object.
    - target_tz (str): Target timezone to convert to.

    Returns:
    - datetime: Converted datetime object without timezone information.
    """
    
    # Assigning the current timezone to the datetime object
    current_timezone = pytz.timezone(current_tz)
    aware_dt = current_timezone.localize(naive_dt)
    
    # Converting to the target timezone
    target_timezone = pytz.timezone(target_tz)
    target_dt = aware_dt.astimezone(target_timezone)
    
    # Returning the datetime object without timezone information
    return target_dt.replace(tzinfo=None)


if __name__ == '__main__':
    print(now())
    print(naive_timezone_convert(now(), "Asia/Taipei", "US/Eastern"))