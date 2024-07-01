import os
def main ():
    print("running map request")
    os.system('python' + ' ' + r'\\nu-sto-03\Users\soren.peterson\GIS\Python\AGOL\Map_Request\Query.py')

    os.system('python' + ' ' + r'\\nu-sto-03\Users\soren.peterson\GIS\Python\AGOL\Map_Request\Cleanup.py')
    print("map request completed")
if __name__ == '__main__':
    main()