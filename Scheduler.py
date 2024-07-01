import schedule
import time
import arcpy
import json
import os

print("Importing Scripts")
import scripts.MapRequest_Launcher as map_request
import scripts.Update_Permits as update_permits
import scripts.Update_Jetter_Status as update_jetter_status
import scripts.Update_Valve_Status as update_valve_status
import scripts.Update_Hydrant_Status as update_hydrant_status
import scripts.Update_WatermainBreak_Status as update_watermainbreak_status
print("Scripts imported")

def job():
    print("I'm working...")
    time.sleep(10)
    print("I'm working...")
    time.sleep(10)
    print("I'm working...")
    time.sleep(10)
    print("I'm working...")

with open(os.path.join(os.path.dirname(__file__), "Scripts.json")) as json_file:
    data = json.load(json_file)
index = 0
for d in data:
    s_script = str(d)
    s_name = data[d]["Name"]
    s_type = data[d]["Schedule Type"]
    s_freq = int(data[d]["Scheduled Frequency"])
    s_date = data[d]["Scheduled Date"]
    s_time = data[d]["Scheduled Time"]
    print(s_time)
    if s_type == "Day":
        if type(s_time) is list:
            for t in s_time:
                schedule.every().day.at(t).do(locals()[s_script].main).tag(s_name)
        else:
            schedule.every().day.at(str(s_time)).do(locals()[s_script].main).tag(s_name)
    if s_type == "Minute":
        print(s_freq)
        schedule.every(s_freq).minutes.do(locals()[s_script].main).tag(s_name)
    print("Name: {}".format(s_name))

print("Script Scheduler Running")
while True:
    schedule.run_pending()
    time.sleep(1)