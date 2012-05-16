# load libraries for analysis and manipulating dates
import sd_card_analysis as sda
import datetime as dt

# --------------------------------------------------------------------------
# begin user configuration

# specify start and end dates for imported data (yyyy, mm, dd, hh, mm)
date_start = dt.datetime(2012, 1, 1)
date_end   = dt.datetime(2012, 1, 2)

# specify data directory as a string ('..' means the directory one level up)
data_directory = '../data/ml08'

# specify meter_name that will be included in the data and csv file
meter_name = 'ml08'

# specify time interval in seconds that data will be sampled
interval_seconds = 10 * 60

# specify output file name
output_file = 'ml08.csv'

# end user configuration
# --------------------------------------------------------------------------

df = sda.export_sd_to_csv(data_directory,
                          date_start,
                          date_end,
                          meter_name=meter_name,
                          interval_seconds=interval_seconds,
                          output_file=output_file)

