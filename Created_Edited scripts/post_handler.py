import csv
import datetime
import json
import threading
import os
import pytz
from db.uow import UoW
#from pdb import set_trace as trace

class PostHandler:
    handle_johnson_post_lock = threading.Lock()
    handle_fieldserver_post_lock = threading.Lock()
    handle_invensys_post_lock = threading.Lock()
    handle_siemens_post_lock = threading.Lock()
    def __init__(self):
        self.johnson_raw_folder = ""
        self.fieldserver_raw_folder = ""
        self.invensys_raw_folder = ""
        self.date_time_str = pytz.utc.localize(datetime.datetime.utcnow()).strftime("%Y-%m-%d %H:%M:%S")
        self.date_str = pytz.utc.localize(datetime.datetime.utcnow()).strftime("%Y-%m-%d")
        self.uow = UoW(None)
        self.ws_id=None

    def handle_johnson_post(self, f):
        with self.handle_johnson_post_lock:
            file_path = os.path.join(self.johnson_raw_folder, self.date_str)

            with open(file_path, "a") as writing_file:
                self.write_johnson_post_data(f, writing_file)

    def write_johnson_post_data(self, f, writing_file):
        reader = csv.reader(f)
        data=[]
        del data[:]
        for row in reader:
            if reader.line_num == 1:
                continue
            f_site_id = str(row[0])
            f_fqr = str(row[1])
            f_timestamp = datetime.datetime.strptime(row[2], '%m/%d/%Y %I:%M %p')
            f_value = row[3]
            groupIds = self.uow.intermediate_records.getGroupId({"johnson_site_id":f_site_id,"johnson_fqr":f_fqr})
            for group in groupIds:
                wsIds = self.uow.intermediate_records.getWeatherStationId(group.split("-")[0])
                for ws in wsIds:
                    self.ws_id = ws

            data.append({
                "sensor_id": f_site_id+"-"+f_fqr,
                "hours_in_record":"0.25",
                "local_year":f_timestamp.year,
                "local_month":f_timestamp.month,
                "local_hour": f_timestamp.hour,
                "local_day_of_month": f_timestamp.day,
                "timestamp": f_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "value": f_value,
                "weatherstation_id":self.ws_id,
                "date_added": self.date_time_str
            })
        #trace()
        self.uow.intermediate_records.insert_intermediate_raw_records(data)


    def handle_fieldserver_post(self, data):
        with self.handle_fieldserver_post_lock:
            file_path = os.path.join(self.fieldserver_raw_folder, self.date_str)

            with open(file_path, "a") as writing_file:
                self.write_fieldserver_post_data(data, writing_file)

    def write_fieldserver_post_data(self, data, writing_file):
        timestamp = round_time(pytz.utc.localize(datetime.datetime.utcnow()))
        records=[]
        del records[:]
        for (key, values) in data.iteritems():
            key_split = key.split("_")
            if key_split[1] == "Offsets":
                site_id = key_split[0]
                values_array = values.split(",")
                starting_offset = int(key_split[2].split("-")[0])
                for offset, value in enumerate(values_array):
                    groupIds = self.uow.intermediate_records.getGroupId({"fieldserver_site_id":site_id,"fieldserver_offset":starting_offset + offset})
                    for group in groupIds:
                        wsIds = self.uow.intermediate_records.getWeatherStationId(group.split("-")[0])
                        for ws in wsIds:
                            self.ws_id = ws
                    records.append({
                        "sensor_id": site_id+"-"+ str(starting_offset + offset),
                        "hours_in_record":"0.25",
                        "local_year":timestamp.year,
                        "local_month":timestamp.month,
                        "local_hour": timestamp.hour,
                        "local_day_of_month": timestamp.day,
                        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                        "value":  value.strip(),
                        "weatherstation_id":self.ws_id,
                        "date_added": self.date_time_str
                    })
        self.uow.intermediate_records.insert_intermediate_raw_records(records)

    def handle_invensys_post(self, f):
        with self.handle_invensys_post_lock:
            file_path = os.path.join(self.invensys_raw_folder, self.date_str)

            with open(file_path, "a") as writing_file:
                self.write_invensys_post_data(f, writing_file)

    def write_invensys_post_data(self, f, writing_file):
        reader = csv.reader(f)
        data=[]
        del data[:]
        for row in reader:
            f_timestamp = datetime.datetime.strptime(str(row[0]).split(".")[0],"%Y-%m-%dT%H:%M:%S")
            f_site_name = str(row[1])
            f_equipment_name = str(row[2])
            f_point_name = str(row[3])
            f_value = str(row[4])
            groupIds = self.uow.intermediate_records.getGroupId({"invensys_site_name":f_site_name,"invensys_equipment_name":f_equipment_name,"invensys_point_name":f_point_name})
            for group in groupIds:
                wsIds = self.uow.intermediate_records.getWeatherStationId(group.split("-")[0])
                for ws in wsIds:
                    self.ws_id = ws

            data.append({
                "sensor_id": f_site_name+"-"+f_equipment_name+"-"+f_point_name,
                "hours_in_record":"0.25",
                "local_year":f_timestamp.year,
                "local_month":f_timestamp.month,
                "local_hour": f_timestamp.hour,
                "local_day_of_month": f_timestamp.day,
                "timestamp": f_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "value": f_value,
                "weatherstation_id":self.ws_id,
                "date_added": self.date_time_str
            })
        #trace()
        #self.uow.intermediate_records.insert_intermediate_raw_records(data)

    def handle_invensys_form_post(self, data):
        with self.handle_invensys_post_lock:
            file_path = os.path.join(self.invensys_raw_folder, self.date_str)
            with open(file_path, "a") as writing_file:
                self.write_invensys_form_post_data(data, writing_file)

    def write_invensys_form_post_data(self, d, writing_file):
        timestamp = round_time(pytz.utc.localize(datetime.datetime.utcnow()))
        data=[]
        del data[:]
        for (key, value) in d.iteritems():
            for row in key.splitlines():
                rowData = row.split(",")
                f_timestamp = datetime.datetime.strptime(str(rowData[0]).split(".")[0],"%Y-%m-%dT%H:%M:%S")
                f_site_name = str(rowData[1])
                f_equipment_name = str(rowData[2])
                f_point_name = str(rowData[3])
                f_value = str(rowData[4])
                groupIds = self.uow.intermediate_records.getGroupId({"invensys_site_name":f_site_name,"invensys_equipment_name":f_equipment_name,"invensys_point_name":f_point_name})
                for group in groupIds:
                    wsIds = self.uow.intermediate_records.getWeatherStationId(group.split("-")[0])
                    for ws in wsIds:
                        self.ws_id = ws

                data.append({
                    "sensor_id": f_site_name+"-"+f_equipment_name+"-"+f_point_name,
                    "hours_in_record":"0.25",
                    "local_year":f_timestamp.year,
                    "local_month":f_timestamp.month,
                    "local_hour": f_timestamp.hour,
                    "local_day_of_month": f_timestamp.day,
                    "timestamp": f_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    "value": f_value,
                    "weatherstation_id":self.ws_id,
                    "date_added": self.date_time_str
                })
        self.uow.intermediate_records.insert_intermediate_raw_records(data)

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