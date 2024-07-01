import schedule
import time
import arcpy
print("Importing Scripts")
from scripts.MapRequest_Launcher import launcher # type: ignore
from scripts.Update_Permits import excecute_update_permit# type: ignore
from scripts.Update_Jetter_Status import jetter_status_update # type: ignore
from scripts.Update_Valve_Status import update_valve_status_execution
from scripts.Update_Hydrant_Status import update_hydrant_status_execution
from scripts.Update_WatermainBreak_Status import update_watermainbreak_status_Excecution
print("Scripts imported")

def job():
    print("I'm working...")
    time.sleep(10)
    print("I'm working...")
    time.sleep(10)
    print("I'm working...")
    time.sleep(10)
    print("I'm working...")

schedule.every(5).minutes.do(launcher)
schedule.every(1).minutes.do(jetter_status_update)
schedule.every(update_valve_status_execution).day.at("20:00")
schedule.every(update_hydrant_status_execution).day.at("21:00")
schedule.every(update_watermainbreak_status_Excecution).day.at("22:00")

print("Script Scheduler Running")
while True:
    schedule.run_pending()
    time.sleep(1)