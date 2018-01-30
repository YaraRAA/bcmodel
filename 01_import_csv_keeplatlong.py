# name: import_csv.py -- 12/07/2017

# convert a CVS file to a shapefile
# the script works with a predifine schema and projection
# if the CVS file has a different structure please make 
# sure you change the schema as well as the prj

# python requirement: shapely and fiona
# conda install -c scitools shapely
# conda install -c conda-forge fiona
# conda install -c conda-forge gdal


import csv, os, subprocess
from shapely.geometry import Point, mapping
from fiona import collection
from osgeo import ogr, osr

"""
convert CSV file to shapefile
"""
def importCSVFile(csvPath, shpPath):
    # set up a schema for the shapefile
    schema = { 'geometry': 'Point', 'properties': { 'id': 'str', 'lat': 'float', 'lng' : 'float' } }
    # set up a prj file 
    prj = {'init': u'epsg:4326'}
    # change the name of the output shapefile and the CVS file
    with collection(shpPath, "w", "ESRI Shapefile", schema, prj) as output:
        with open(csvPath, 'rb') as f:
            reader = csv.DictReader(f)
            for row in reader:
                point = Point(float(row['Longitude']), float(row['Latitude']))
                output.write({
                    'properties': {
                        'id': row['id'],
                        'lat': row['Latitude'],
                        'lng': row['Longitude']
                      
                    },
                    'geometry': mapping(point)
                })

"""
change the shapefile projection from 4326 to 5070 using ogr2ogr
"""
def changeProj(inSHP, outSHP):    
    subprocess.call('ogr2ogr -f "ESRI Shapefile" ' + outSHP + '  ' + inSHP + ' -s_srs EPSG:4326 -t_srs EPSG:5070', shell=True)


if __name__ == '__main__':
    # OS Mac or Linux
    #importCSVFile('/Users/cecilia/Desktop/gis/pm/newengland/data/exampledata.csv','/Users/cecilia/Desktop/gis/pm/newengland/data/address.shp')
    #changeProj('/Users/cecilia/Desktop/gis/pm/newengland/data/address.shp', '/Users/cecilia/Desktop/gis/pm/newengland/data/_address.shp')
    # OS Windows
    importCSVFile(r'C:\Users\yaa291\Desktop\Autorun\mydata.csv',r'C:\Users\yaa291\Desktop\Autorun\addresses.shp')
    changeProj(r"C:\Users\yaa291\Desktop\Autorun\addresses.shp", r"C:\Users\yaa291\Desktop\Autorun\_addresses.shp")


