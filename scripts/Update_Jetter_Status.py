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

def main():

    error_string = ""
    error_count = 0
    script_success = True
    
    try:
        print("Clearing Workspace")
        arcpy.env.workspace = r"\\nu-sto-03\Departments\GIS\_Python\A_AutomatedScripts\JetterInspectionStatus\Data\Data.gdb"
        project_fgdb = r"\\nu-sto-03\Departments\GIS\_Python\A_AutomatedScripts\JetterInspectionStatus\Data\Data.gdb"
        fc_list = arcpy.ListFeatureClasses()
        tables = arcpy.ListTables()
        ds_list = arcpy.ListDatasets()
        if 'SewerLine' in fc_list:
            arcpy.Delete_management('SewerLine')
        if 'SewerLine_Join' in fc_list:
            arcpy.Delete_management('SewerLine_Join')
        if 'Jetter_Table' in tables:
            arcpy.Delete_management('Jetter_Table')
        print("Workspace Cleared")

        print("Copy Files From SDE...")
        with arcpy.EnvManager(preserveGlobalIds=True):
            arcpy.conversion.ExportFeatures(
                in_features=r"\\nu-sto-03\Departments\GIS\_Python\A_AutomatedScripts\JetterInspectionStatus\Data\Sanitary_UN.sde\SanitaryUN6.UN_OWNER.SanitarySystem\SanitaryUN6.UN_OWNER.SewerLine",
                out_features=r"\\nu-sto-03\Departments\GIS\_Python\A_AutomatedScripts\JetterInspectionStatus\Data\Data.gdb\SewerLine",
            )
        sewerline_fc = r"\\nu-sto-03\Departments\GIS\_Python\A_AutomatedScripts\JetterInspectionStatus\Data\Data.gdb\SewerLine"

        arcpy.conversion.ExportTable(
            in_table=r"\\nu-sto-03\Departments\GIS\_Python\A_AutomatedScripts\JetterInspectionStatus\Data\Sanitary_UN.sde\SanitaryUN6.UN_OWNER.Sanitary_Jetter_Insp",
            out_table=r"\\nu-sto-03\Departments\GIS\_Python\A_AutomatedScripts\JetterInspectionStatus\Data\Data.gdb\Jetter_Table",
        )
        jettertable_table = r"\\nu-sto-03\Departments\GIS\_Python\A_AutomatedScripts\JetterInspectionStatus\Data\Data.gdb\Jetter_Table"
        print("Complete")

        print("Exporting Join")
        join_fc = arcpy.AddJoin_management (sewerline_fc, "GLOBALID", jettertable_table, "SewerLineGlobalID")
        join_out_fc = os.path.join(project_fgdb, "SewerLine_Join")
        arcpy.management.CopyFeatures(join_fc, join_out_fc)
        print("Complete")

        print("Deleting Extra Fields")
        jetter_fields = arcpy.ListFields(join_out_fc)
        j_preserve_list = ["OBJECTID", "Shape", "Shape_Length", "SewerLine_GLOBALID", "Jetter_Table_Maintenance_Date", "Jetter_Table_Maintenance_Type", "Jetter_Table_Deficeiency_Type", "Jetter_Table_Comments", "Jetter_Table_SewerLineGlobalID"]
        for field in jetter_fields:
            if field.name not in j_preserve_list:
                print(field.name)
                arcpy.management.DeleteField(join_out_fc, field.name)
        print("Complete")

        print('Removing Old Features')
        keep_list = list() 
        sortfield = 'Jetter_Table_Maintenance_Date'
        with da.UpdateCursor(join_out_fc, ['Jetter_Table_SewerLineGlobalID','Jetter_Table_Maintenance_Date'], sql_clause = (None, "ORDER BY Jetter_Table_Maintenance_Date DESC")) as cursor:
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
        print("Complete")

        print("Calculating Feature Ages")
        arcpy.management.AddField(join_out_fc, "INSPECT_AGE", "FLOAT")
        with da.UpdateCursor(join_out_fc, ['Jetter_Table_Maintenance_Date', 'INSPECT_AGE']) as cursor:
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
        print("Complete")

        print("Loading Data to SDE")
        arcpy.env.workspace = r"\\nu-sto-03\Departments\GIS\_Python\A_AutomatedScripts\JetterInspectionStatus\Data\Utility_View.sde"
        sde_jetter = r"\\nu-sto-03\Departments\GIS\_Python\A_AutomatedScripts\JetterInspectionStatus\Data\Utility_View.sde\DBO.Jetter_Data\DBO.Sanitary_Jetter_Status"
        arcpy.management.DeleteRows(sde_jetter)
        arcpy.management.Append(
            inputs = join_out_fc,
            target = sde_jetter,
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

    excecution_status("update_jetter_status", script_success, error_count, error_string)

if __name__ == '__main__':
    main()