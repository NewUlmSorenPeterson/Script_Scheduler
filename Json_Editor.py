import json
import os
import datetime

with open(os.path.join(os.path.dirname(__file__), "Scripts.json")) as json_file:
    data = json.load(json_file)
index = 0
for d in data:
    id = data[d]["Name"]
    print("Name: {}".format(id))
    data[d]["Previous Attempt"] = str(datetime.datetime.now())
with open(os.path.join(os.path.dirname(__file__), "Scripts.json"), "w") as json_file:
    json.dump(data,json_file, indent=4, separators=(',', ': '))