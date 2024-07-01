import os
def launcher ():
    print("running map request")
    os.system('python' + ' ' + r'\\nu-sto-03\Users\soren.peterson\GIS\Python\AGOL\Map_Request\Query.py')

    os.system('python' + ' ' + r'\\nu-sto-03\Users\soren.peterson\GIS\Python\AGOL\Map_Request\Cleanup.py')
    print("map request completed")
