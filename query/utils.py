
def convert_to_unix_time(date_string, date_format = '%Y-%m-%dT%H:%M:%S'):
  import datetime, calendar
  ## Make datetime object from time_string
  date = datetime.datetime.strptime(date_string, date_format)
  time_tuple = date.timetuple()
  ## Get unix time (this assumes that time_tuple was created with a UTC time)
  time_unix = calendar.timegm(time_tuple)
  
  return time_unix
