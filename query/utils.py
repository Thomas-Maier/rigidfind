
from __future__ import print_function


def date_to_unix(date_string, date_format = '%Y-%m-%dT%H:%M:%S'):
  import datetime, calendar
  ## Make datetime object from time_string
  date = datetime.datetime.strptime(date_string, date_format)
  time_tuple = date.timetuple()
  ## Get unix time (this assumes that time_tuple was created with a UTC time)
  time_unix = calendar.timegm(time_tuple)

  return time_unix

def unix_to_date(timestamp, date_format = '%Y-%m-%dT%H:%M:%S'):
  import datetime

  return datetime.datetime.utcfromtimestamp(timestamp).strftime(date_format)

def get_date_params(shift = None, date1 = None, date2 = None, date_format = '%Y-%m-%d'):
  import datetime
  if shift is not None:
    date = datetime.datetime.utcnow()
    this_month = date.month
    date += datetime.timedelta(days = shift)
    return date.year, date.month, date.day, this_month
  elif date1 is not None and date2 is not None:
    start_date = datetime.datetime.strptime(date1, date_format)
    end_date = datetime.datetime.strptime(date2, date_format)
    return start_date.year, start_date.month, start_date.day, end_date.year, end_date.month, end_date.day
  else:
    print('Please specify either a shift or start and end date')
    raise Exception

def hr_time(time_seconds):
  minutes, seconds = divmod(time_seconds, 60)
  hours, minutes = divmod(minutes, 60)
  ## Weird case in which hours get a minus prepended
  if hours == 0 and minutes == 0:
    hours = abs(hours)
  if hours < 25:
    return '{:.0f}:{:02.0f}:{:02.0f}'.format(hours, minutes, seconds)
  days, hours = divmod(hours, 24)
  if days < 366:
    return '{:.0f}d {:.0f}:{:02.0f}:{:02.0f}'.format(days, hours, minutes, seconds)
  years, days = divmod(days, 365)

  return '{:.0f}y {:.0f}d {:.0f}:{:02.0f}:{:02.0f}'.format(years, days, hours, minutes, seconds)
