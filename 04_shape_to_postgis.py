# -*- coding: utf-8 -*-
"""
Created on Wed Dec 13 19:03:28 2017

@author: yaa291
"""

# name: shape_to_postgis.py -- 12/07/2017

# This script load the shapefile into postgres, create a spatial index, and force 2D

# Before to run this script make sure that all the shapefile you would like to import are 
# within the same folder.
# Make sure to run it using conda
# To install conda visit this website (https://conda.io/docs/index.html)
# Once conda is install successfully you can create a conda enviroment
# and then install psycopg2. Below is the conda comands:

# 1) conda create -n pmne python=3.6
# 2) source activate pmne (this can be different if you use windows OS)
# 3) conda install -c conda-forge psycopg2
# 4) conda list
# 5) python /path/shape_to_postgis.py (to run the script)

# Also make sure that the name of each shapefile is the name of the table you would like to use
# later on. On a linux or Mac OS the path to shp2pgsql, psql might be different

import os, subprocess, psycopg2, ogr, osr

# change the name of your database
db = 'bc'
# Choose your PostgreSQL version here
# OS for Windows
os.environ['PATH'] += r';C:\\Program Files\\PostgreSQL\\10\\bin'
# OS for Mac or Linux
#os.environ['PATH'] += '/Library/PostgreSQL/9.5/bin/'
# http://www.postgresql.org/docs/current/static/libpq-envars.html
os.environ['PGHOST'] = 'localhost'
os.environ['PGPORT'] = '5432'
os.environ['PGUSER'] = 'postgres'
os.environ['PGPASSWORD'] = '4surSecur2!!'
os.environ['PGDATABASE'] = db

conn = psycopg2.connect("dbname="+ db + " user=postgres password=4surSecur2!!")

def loadTable(base_dir):
	shapefile_list = []
	for root,dirs,files in os.walk(base_dir):
		if root[len(base_dir)+1:].count(os.sep)<2:
			for file_ in files:				
				# load only shapefile (.shp) that start with _
				if file_[-3:] == 'shp' and file_[0] == '_' :
					shapefile_path = os.path.join(base_dir, file_)
					#inSHP = r"" + shapefile_path + ""
					#outSHP = r"" + base_dir + "\\" + shapefile_path.split("\\")[-1].split('.')[0] + ".shp"
					inSHP = file_.split('.')[0]
					outSHP = base_dir + "/" + file_.split('.')[0] + ".shp"
					# update the array shapefile_list
					shapefile_list.append(file_.split('.')[0])
					#print outSHP
					#print inSHP
					#print shapefile_path.split("\\")[-1].split('.')[0]
					# OS for Mac or Linux
					#subprocess.call('/Library/PostgreSQL/9.5/bin/shp2pgsql -c -D -I -s 5070 "' + outSHP + ' ' + inSHP + '" | /Library/PostgreSQL/9.5/bin/psql -d ' + db + ' -h localhost -U postgres ', shell=True)
					# OS Windows
					subprocess.call('shp2pgsql -c -D -I -s 5070 "' + outSHP + ' ' + inSHP + '" | psql -d ' + db + ' -h localhost -U postgres ', shell=True)
	for shapename in shapefile_list:
		#print shapename
		changeSRID(shapename)

# function to get the geometry type need for force2D, and craeteGeoIndex
def changeSRID(table):
	    cur = conn.cursor()	    
	    sql = 'select ST_GeometryType(geom) as result FROM ' + table + ' limit 1;'
	    cur.execute(sql)
	    results = cur.fetchall()
	    tablegeom = results[0][0].split("_")[1]

	    force2D(table, tablegeom)
	    createGeoIndex(table)

	    conn.commit()
	    cur.close
# function to force table to 2D
def force2D(table, tablegeom):
	cur = conn.cursor()
	sql = 'alter table ' + table + ' ALTER COLUMN geom TYPE geometry (' + tablegeom  + ') USING ST_Force2D(geom);'
	cur.execute(sql)
	cur.close
# function to create spatial index
def createGeoIndex(table):
	cur = conn.cursor()
	sql = 'create index ' + table + '_gix on ' + table + ' USING GIST (geom);'	
	cur.execute(sql)		
	cur.close
	
if __name__ == '__main__':    
	# change the name of the data path
	# OS Windows
    loadTable(r'C:\Users\yaa291\Desktop\Autorun')
    # OS Mac or Linux
    #loadTable('/Users/cecilia/Desktop/gis/pm/newengland/data')

