# load libraries for analysis and manipulating dates
import sd_card_analysis as sda
import datetime as dt

# --------------------------------------------------------------------------
# begin user configuration

# specify start and end dates for imported data (yyyy, mm, dd, hh, mm)
date_start = dt.datetime(2011, 12, 1)
date_end   = dt.datetime(2012, 7, 1)

# specify time interval in seconds that data will be sampled
interval_seconds = 5 * 60

# location of all data directories 
base_directory = '/Users/dsoto/repos/sd_card_analysis/uganda_data/'

# specify meter_name that will be included in the data and csv file
meter_name = 'ug08'

# specify data directory as a string ('..' means the directory one level up)
data_directory = base_directory + meter_name

# specify output file name can save as either .xls, .xlsx, or .csv
output_file = meter_name + '.csv'


# end user configuration
# --------------------------------------------------------------------------

start = dt.datetime.now()

df = sda.export_sd_to_csv(data_directory,
                          date_start,
                          date_end,
                          meter_name=meter_name,
                          interval_seconds=interval_seconds,
                          output_file=output_file)

print 'time elapsed'
print dt.datetime.now() - start

import smtplib
fromaddr = 'drsautomate@gmail.com'
toaddr = 'drdrsoto@gmail.com'
msg = meter_name + ' conversion complete'
username = 'drsautomate'
password = username[::-1]

server = smtplib.SMTP('smtp.gmail.com:587')
server.starttls()
server.login(username, password)
server.sendmail(fromaddr, toaddr, msg)
server.quit()

