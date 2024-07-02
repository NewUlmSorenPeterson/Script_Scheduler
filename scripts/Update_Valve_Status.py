import arcpy
from arcpy import da
import os
from datetime import date
from datetime import datetime
import json


def excecution_status(script, successful, error_count, error_string):
    with open(r"W:\GIS\Python\Scheduler\Scripts.json") as json_file:
        data = json.load(json_file)
    data[script]["Previous Attempt"] = str(datetime.now())
    if successful == True:
        data[script]["Status"] = "Running"
        data[script]["Last Successful Attempt"] = str(datetime.now())
        data[script]["Errors"] = 0
        data[script]["Error String"] = ""
    else:
        data[script]["Status"] = "Error"
        data[script]["Errors"] = error_count
        data[script]["Error String"] = error_string
    with open(r"W:\GIS\Python\Scheduler\Scripts.json", "w") as json_file:
        json.dump(data,json_file, indent=4, separators=(',', ': '))


def update_valve_status():
    try:
        error_string = ""
        error_count = 0
        script_success = True
        print("Clearing Workspace")
        arcpy.env.workspace = r"\\nu-sto-03\Departments\GIS\_Python\A_AutomatedScripts\WaterValveInspectionStatus\Data\Data.gdb"
        project_fgdb = r"\\nu-sto-03\Departments\GIS\_Python\A_AutomatedScripts\WaterValveInspectionStatus\Data\Data.gdb"
        fc_list = arcpy.ListFeatureClasses()
        tables = arcpy.ListTables()
        ds_list = arcpy.ListDatasets()
        if 'SewerLine' in fc_list:
            arcpy.Delete_management('WaterDevice')
        if 'WaterValve_Join' in fc_list:
            arcpy.Delete_management('WaterValve_Join')
        if 'WaterValve_Table' in tables:
            arcpy.Delete_management('WaterValve_Table')
        print("Workspace Cleared")

        print("Copy Files From SDE...")
        with arcpy.EnvManager(preserveGlobalIds=True):
            arcpy.conversion.ExportFeatures(
                in_features=r"\\nu-sto-03\Departments\GIS\_Python\A_AutomatedScripts\WaterValveInspectionStatus\Data\Water_UN.sde\UN_OWNER.WaterSystem\UN_OWNER.WaterDevice",
                out_features=r"\\nu-sto-03\Departments\GIS\_Python\A_AutomatedScripts\WaterValveInspectionStatus\Data\Data.gdb\WaterDevice",
            )
        WaterValve_fc = r"\\nu-sto-03\Departments\GIS\_Python\A_AutomatedScripts\WaterValveInspectionStatus\Data\Data.gdb\WaterDevice"

        arcpy.conversion.ExportTable(
            in_table=r"\\nu-sto-03\Departments\GIS\_Python\A_AutomatedScripts\WaterValveInspectionStatus\Data\Water_UN.sde\UN_OWNER.ValveInspection",
            out_table=r"\\nu-sto-03\Departments\GIS\_Python\A_AutomatedScripts\WaterValveInspectionStatus\Data\Data.gdb\WaterValve_Table",
        )
        WaterValve_table = r"\\nu-sto-03\Departments\GIS\_Python\A_AutomatedScripts\WaterValveInspectionStatus\Data\Data.gdb\WaterValve_Table"
        print("Complete")

        print("Exporting Join")
        join_fc = arcpy.AddJoin_management (WaterValve_fc, "GLOBALID", WaterValve_table, "valveid")
        join_out_fc = os.path.join(project_fgdb, "WaterValve_Join")
        arcpy.management.CopyFeatures(join_fc, join_out_fc)
        print("Complete")

        print("Deleting Extra Fields")
        WaterValve_fields = arcpy.ListFields(join_out_fc)
        h_preserve_list = ["WaterValve_Table_valveid", "WaterValve_Table_insdate", "WaterValve_Table_creator"]
        arcpy.management.DeleteField(join_out_fc, h_preserve_list, "KEEP_FIELDS")
        print("Complete")

        workspace = r"\\nu-sto-03\Departments\GIS\_Python\A_AutomatedScripts\WaterValveInspectionStatus\Data\Data.gdb"
        edit = arcpy.da.Editor(workspace)
        edit.startEditing(with_undo=False, multiuser_mode=True)
        edit.startOperation()
        print('Removing Old Features')
        keep_list = list() 
        sortfield = 'WaterValve_Table_insdate'
        with arcpy.da.UpdateCursor(join_out_fc, ['WaterValve_Table_valveid','WaterValve_Table_insdate'], sql_clause = (None, "ORDER BY WaterValve_Table_insdate DESC")) as cursor:
            for row in cursor:
                row_val = row[0]
                if row[1] == None:
                    cursor.deleteRow()
                if row_val not in keep_list: 
                    keep_list.append(row_val)
                elif row_val in keep_list:         
                    cursor.deleteRow() 
                else:         
                    pass
        del row
        del cursor
        edit.stopOperation()
        edit.stopEditing(save_changes=True)
        del edit
        print("Complete")

        print("Calculating Feature Ages")
        arcpy.management.AddField(join_out_fc, "INSPECT_AGE", "FLOAT")
        edit = arcpy.da.Editor(workspace)
        edit.startEditing(with_undo=False, multiuser_mode=True)
        edit.startOperation()
        with da.UpdateCursor(join_out_fc, ['WaterValve_Table_insdate', 'INSPECT_AGE']) as cursor:
            for row in cursor:
                inspection_date = row[0]
                print(row[0])
                current_date = date.today()
                inspect_ymd = inspection_date.strftime('%y')
                print(inspect_ymd)
                current_ymd = current_date.strftime('%y')
                print(current_ymd)
                dt_inspection = datetime.strptime(inspect_ymd, '%y')
                dt_current = datetime.strptime(current_ymd, '%y')
                year_current = int(dt_current.year)
                year_inspection = int(dt_inspection.year)
                year_diff = year_current - year_inspection
                row[1] = year_diff
                print(row[1])
                cursor.updateRow(row)
        edit.stopOperation()
        edit.stopEditing(save_changes=True)
        print("Complete")

        print("Loading Data to SDE")
        arcpy.env.workspace = r"\\nu-sto-03\Departments\GIS\_Python\A_AutomatedScripts\WaterValveInspectionStatus\Data\Utility_View.sde"
        sde_layer = r"\\nu-sto-03\Departments\GIS\_Python\A_AutomatedScripts\WaterValveInspectionStatus\Data\Utility_View.sde\DBO.Water_Data\DBO.WaterValve_Status"
        arcpy.management.DeleteRows(sde_layer)
        arcpy.management.Append(
            inputs = join_out_fc,
            target = sde_layer,
            schema_type = "TEST",
            field_mapping = None,
            subtype = "",
            expression = "",
            match_fields = None,
            update_geometry = "NOT_UPDATE_GEOMETRY"
        )
        print("Data Loaded")

    except:
        error_string = "Script Failure"
        error_count = error_count + 1
        script_success = False
        pass
    
    excecution_status("update_valve_status", script_success, error_count, error_string)

def main():
    update_valve_status()


if __name__ == '__main__':
    main()