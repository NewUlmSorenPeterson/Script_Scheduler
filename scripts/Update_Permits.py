import pyodbc
import csv
import pandas as pd
import os
from scripts.Get_Year import year
import arcpy
from arcpy import env
from scripts.Environments import ag_user, ag_pass
from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection

arcpy.env.workspace = r"\\nu-sto-03\Users\soren.peterson\GIS\Python\AGOL\Permit_Update\Data\Data.gdb"

arcpy.SignInToPortal("https://nupuc.maps.arcgis.com/", 
                     ag_user, ag_pass)

portalURL = "https://nupuc.maps.arcgis.com/"
username = ag_user
password = ag_pass
gis = GIS(portalURL, username, password)
store_csv_w_attachments = False

service_id = "0a01083872ae4d7089f4fb2748341ce9"
d_layer = "https://services1.arcgis.com/V0aadQ5fktNa6V0X/arcgis/rest/services/Permit_Locations/FeatureServer/0"
columns = []
current_year = "2024"
csvpath = r"\\nu-sto-03\Users\soren.peterson\GIS\Python\AGOL\Permit_Update\data\Perms_Review.csv"
accesspath = r"\\nu-sto-03\Departments\Engineering_CAD\Engineering-City\Permits and Patching List\Excavation Permit Spreadsheets\2024 Permits\2024 Excavation Permits.mdb"

## future update: needs compatibility for future years. Use re.search and datetime to pull folder year and append into same csv. Loop through db connection and cursor to write all data to single csv.
## future update: filter to years greater than number to exlude older years. ie: if years > 2022:
def access_to_csv(access_location, csv_path):
    pyodbc.lowercase = False
    conn = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};' + r"Dbq={path};".format(path = access_location))
    cursor = conn.cursor()
    cursor.execute('select * FROM Locate_Table');
    columns = [column[0] for column in cursor.description]
    print(columns)

    cursor.execute('select * FROM Locate_Table');
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        for row in cursor.fetchall():
            writer.writerow(row)
    cursor.close()
    conn.close()

def update_service(feature_service_id, year, csv_path):
    field_id = -1
    dffield_id = -2
    hfl = gis.content.get(feature_service_id)
    hflayer = hfl.layers[0]
    ## future update: grab year from "work done" field to calculate year in ID for each years entries.
    df = pd.read_csv(csv_path, on_bad_lines='skip', low_memory=False, dtype=str, header=None)
    df[0] = df[0].astype(str) + "-" + str(year)
    df[27] = df[27].str.split('\\').str[-1].str.rstrip('.pdf')
    df[27] = df[27].str.replace('#', '')
    df[27] = os.path.join("Z:\Permits and Patching List\Scanned Excavation Permits", str(year) + " Scanned Excavation Permits", "") + df[27]
    df[28] = df[28].str.split('\\').str[-1].str.rstrip('.pdf')
    df[28] = df[28].str.replace('#', '')
    df[28] = os.path.join("Z:\Permits and Patching List\Scanned Excavation Photos", str(year) + " Photos", "") + df[28]
    fset = hflayer.query()
    for i in range(len(df)):
        print(df.loc[i, 0])
        current_id = df.loc[i, 0]
        for f in (fset.features):
            field_id = 0
            dffield_id = -2
            if f.attributes["Permit_Loc_GIS_ID"] == current_id:
                for k in f.attributes:
                    print(k)
                    field_id = field_id + 1
                    dffield_id = dffield_id + 1 
                    print(dffield_id)
                    attribute_value = ""
                    if dffield_id > 0 and dffield_id < 29:
                        attribute_value = df.loc[i, dffield_id]
                        print(attribute_value)
                        if df.loc[i, dffield_id] == None or df.loc[i, dffield_id] == "" or df.loc[i, dffield_id] is None or df.loc[i, dffield_id] == "nan" or df.isnull().loc[i, dffield_id] == True:
                            f.attributes[k] = ""
                            print("Is empty")
                        else:
                            f.attributes[k] = df.loc[i, dffield_id]
                            print("Not Empty")
                    hflayer.edit_features(updates = fset.features)

def excecute_update_permit(): 
    access_to_csv(accesspath, csvpath)
    update_service(service_id, current_year, csvpath)

if __name__ == '__main__':
    excecute_update_permit()