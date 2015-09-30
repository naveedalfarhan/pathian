import csv
import datetime
import json
import threading
import os
import pytz
from flask import current_app
from pdb import set_trace as trace


class PostHandlerOld:
    handle_johnson_post_lock = threading.Lock()
    handle_fieldserver_post_lock = threading.Lock()
    handle_invensys_post_lock = threading.Lock()

    def __init__(self):
        self.johnson_raw_folder = ""
        self.fieldserver_raw_folder = ""
        self.invensys_raw_folder = ""
        self.date_time_str = pytz.utc.localize(datetime.datetime.utcnow()).strftime("%Y-%m-%d %H:%M:%S")
        self.date_str = pytz.utc.localize(datetime.datetime.utcnow()).strftime("%Y-%m-%d")

    def handle_johnson_post(self, f):
        with self.handle_johnson_post_lock:
            file_path = os.path.join(self.johnson_raw_folder, self.date_str)

            with open(file_path, "a") as writing_file:
                self.write_johnson_post_data(f, writing_file)

    def write_johnson_post_data(self, f, writing_file):
        reader = csv.reader(f)
        for row in reader:
            if reader.line_num == 1:
                continue

            f_site_id = str(row[0])
            f_fqr = str(row[1])
            f_timestamp = row[2]
            f_value = row[3]
            f_reliability = row[4]

            data = json.dumps({
                "site_id": f_site_id,
                "fqr": f_fqr,
                "timestamp": f_timestamp,
                "value": f_value,
                "reliability": f_reliability,
                "date_added": self.date_time_str
            })
            writing_file.write(data + u"\n")

    def handle_fieldserver_post(self, data):
        with self.handle_fieldserver_post_lock:
            file_path = os.path.join(self.fieldserver_raw_folder, self.date_str)

            with open(file_path, "a") as writing_file:
                self.write_fieldserver_post_data(data, writing_file)

    def write_fieldserver_post_data(self, data, writing_file):
        timestamp = round_time(pytz.utc.localize(datetime.datetime.utcnow()))

        for (key, values) in data.iteritems():
            key_split = key.split("_")
            if key_split[1] == "Offsets":
                site_id = key_split[0]
                values_array = values.split(",")
                starting_offset = int(key_split[2].split("-")[0])

                for offset, value in enumerate(values_array):
                    data = json.dumps({
                        "site_id": site_id,
                        "offset": starting_offset + offset,
                        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                        "value": value.strip(),
                        "date_added": self.date_time_str
                    })

                    writing_file.write(data + u"\n")

    def handle_invensys_post(self, f):
        with self.handle_invensys_post_lock:
            file_path = os.path.join(self.invensys_raw_folder, self.date_str)

            with open(file_path, "a") as writing_file:
                self.write_invensys_post_data(f, writing_file)

    def write_invensys_post_data(self, f, writing_file):
        reader = csv.reader(f)
        for row in reader:

            f_timestamp = str(row[0])
            f_site_name = str(row[1])
            f_equipment_name = str(row[2])
            f_point_name = str(row[3])
            f_value = str(row[4])

            data = json.dumps({
                "timestamp": f_timestamp,
                "invensys_site_name": f_site_name,
                "invensys_equipment_name": f_equipment_name,
                "invensys_point_name": f_point_name,
                "value": f_value,
                "date_added": self.date_time_str
            })

            writing_file.write(data + u"\n")

    def handle_invensys_form_post(self, data):
        with self.handle_invensys_post_lock:
            file_path = os.path.join(self.invensys_raw_folder, self.date_str)
            with open(file_path, "a") as writing_file:
                self.write_invensys_form_post_data(data, writing_file)

    def write_invensys_form_post_data(self, d, writing_file):
        for (key, value) in d.iteritems():
            for row in key.splitlines():
                rowData = row.split(",")
                f_timestamp = str(rowData[0])
                f_site_name = str(rowData[1])
                f_equipment_name = str(rowData[2])
                f_point_name = str(rowData[3])
                f_value = str(rowData[4])

                data = json.dumps({
                    "timestamp": f_timestamp,
                    "invensys_site_name": f_site_name,
                    "invensys_equipment_name": f_equipment_name,
                    "invensys_point_name": f_point_name,
                    "value": f_value,
                    "date_added": self.date_time_str
                })
                writing_file.write(data + u"\n")

def round_time(time):
    minute = time.minute
    minute_remainder = minute % 15
    new_time = pytz.utc.localize(datetime.datetime(time.year, time.month, time.day, time.hour, 0, 0))

    if minute_remainder < 7 or (minute_remainder == 7 and time.second < 30):
        minute -= minute_remainder
    else:
        minute = minute - minute_remainder + 15

    new_time += datetime.timedelta(seconds=minute*60)

    return new_time