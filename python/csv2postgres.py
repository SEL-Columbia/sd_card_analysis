'''
must run from directory that contains data directory
> python ../python/csv2postgres.py ml01

'''


import csv, os, os.path, sys
from datetime import datetime

import psycopg2
conn = psycopg2.connect('dbname=sdcard')
cursor = conn.cursor()


def load_dir(data_dir):

    insert_string = '''insert into logs (meter_name, ip_address, meter_timestamp, watts, volts, amps, watthours_sc20, watthours_today, powerfactor, frequency, voltamps, status, machineid, credit) values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');'''

    for root, dirs, files in os.walk(data_dir):
        for f in files:
            if not f.endswith(".log"):
                continue

            circuit_id = int(f[-6:-4])
            print os.path.join(root, f), circuit_id, data_dir

            log = csv.reader(open(os.path.join(root, f), 'rb'), delimiter=',')
            header = log.next()

            line_number = 0
            for row in log:
                timestamp = datetime.strptime(row[0], "%Y%m%d%H%M%S").isoformat(' ')
                watts = float(row[1])
                volts = float(row[2])
                amps = float(row[3])
                wh_sc20 = float(row[4])
                wh_today = float(row[5])
                power_factor = int(row[12])
                frequency = float(row[14])
                volt_amps = float(row[15])
                status = 1 if int(row[16]) == 0 else 1
                machine_id = row[18]
                try:
                    credit = 0.0 if circuit_id == 0 else float(row[20])
                except ValueError:
                    print "value error on line ", line_number, "of csv file"
                    break
                line_number += 1
                str=    insert_string % (data_dir,
                                       circuit_id,
                                       timestamp,
                                       watts,
                                       volts,
                                       amps,
                                       wh_sc20,
                                       wh_today,
                                       power_factor,
                                       frequency,
                                       volt_amps,
                                       status,
                                       machine_id,
                                       credit)
                # if there is a primary key error, rollback
                try:
                    cursor.execute(str)
                except psycopg2.IntegrityError:
                    conn.rollback()
                else:
                    conn.commit()



if __name__ == "__main__":
    args = sys.argv[1:]
    print args
    load_dir(args[0])
