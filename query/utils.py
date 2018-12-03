
from __future__ import print_function


def convert_to_unix_time(date_string, date_format = '%Y-%m-%dT%H:%M:%S'):
  import datetime, calendar
  ## Make datetime object from time_string
  date = datetime.datetime.strptime(date_string, date_format)
  time_tuple = date.timetuple()
  ## Get unix time (this assumes that time_tuple was created with a UTC time)
  time_unix = calendar.timegm(time_tuple)

  return time_unix

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
