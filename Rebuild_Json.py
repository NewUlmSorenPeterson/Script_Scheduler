import json

scripts_dict = {
    "map_request" : {
        "Name" : "Map Request",
        "Status" : "NA",
        "Scheduled Run" : "",
        "Previous Attempt" : "",
        "Last Successful Attempt" : "",
        "Errors" : "",
        "Error String" : "",
    },
    "update_jetter_status" : {
        "Name" : "Update Jetter Status",
        "Status" : "NA",
        "Scheduled Run" : "",
        "Previous Attempt" : "",
        "Last Successful Attempt" : "",
        "Errors" : "",
        "Error String" : "",
    },
    "update_valve_status" : {
        "Name" : "Update Valve Status",
        "Status" : "NA",
        "Scheduled Run" : "",
        "Previous Attempt" : "",
        "Last Successful Attempt" : "",
        "Errors" : "",
        "Error String" : "",
    },
    "update_hydrant_status" : {
        "Name" : "Update Hydrant Status",
        "Status" : "NA",
        "Scheduled Run" : "",
        "Previous Attempt" : "",
        "Last Successful Attempt" : "",
        "Errors" : "",
        "Error String" : "",
    },
    "Update_WatermainBreak_Status" : {
        "Name" : "Update WatermainBreak Status",
        "Status" : "NA",
        "Scheduled Run" : "",
        "Previous Attempt" : "",
        "Last Successful Attempt" : "",
        "Errors" : "",
        "Error String" : "",
    },
}

with open("W:\GIS\Python\Scheduler\Scripts.json","w") as f:
    json.dump(scripts_dict,f, indent=4, separators=(',', ': '))
