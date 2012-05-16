sd_card_analysis

This repo contains a collection of tools for parsing,
sampling, and storing the SD card usage data generated
by the SharedSolar meters.

To assemble the data distributed across the SD card
directory structure into a CSV with a unified timebase,
use the export_sd_to_csv() function in sd_card_analysis.py.

Rather than try to specify date objects on the command
line, export_sd_to_csv() is only available to be called
from within a python session.

A convenience script, export_sd_to_csv.py is available
for use.
Edit the script using a text editor and specify the
data directory, dates, sampling interval, etc. and then
run using
> python export_sd_to_csv.py

You may encounter corrupted log file that will cause the
script to fail.
When this happens, make a note of the last file read
and see if you can determine the cause and correct it
either by deleting the offending line or file.

