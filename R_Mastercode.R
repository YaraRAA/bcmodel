#load libraries


library(dplyr)
library(dbplyr)
library(RPostgreSQL)
library(rpostgis)
library(DBI)
library(rgdal)
library(sp)
library(raster)



#### Change below path on your machine
mypath = 'C:/Users/yaa291/Desktop/BC_NewCode/'

################
#     STEP 1   #
################

#1- importing geocoded addresses in 'mydata.csv' file and projecting to EPSG:5070. Python script:01_import_csv_keeplatlong.py

addresses = read.csv(paste0(mypath, 'mydata.csv'))
addresses$lat = addresses$Latitude
addresses$lng = addresses$Longitude
coordinates(addresses)<-~Longitude+Latitude
proj4string(addresses) = CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs") 

addresses = spTransform(addresses, CRS("+init=epsg:5070"))



################
#     STEP 2   #
################

#2 - extracting values at raster files

elev = raster(paste0(mypath, 'rasters/elev'))
addresses$elev_m = extract(elev, addresses)
rm(elev); gc();

dvhi1km = raster(paste0(mypath, 'rasters/dvhi1km'))
addresses$dvhi_1km = extract(dvhi1km, addresses)
rm(dvhi1km); gc();

dvlo1km = raster(paste0(mypath, 'rasters/dvlo1km'))
addresses$dvlo_1km = extract(dvlo1km, addresses)
rm(dvlo1km); gc();

imp1km= raster(paste0(mypath, 'rasters/imp1km'))
addresses$imp1km= extract(imp1km, addresses)
rm(imp1km); gc();

#save file
writeOGR(addresses, paste0(mypath,'shapefiles'), "_addresses", driver = "ESRI Shapefile")

################
#     STEP 3   #
################

#3 - Create a postgresql database, add postgis extension and EPSG:5070 projection  (https://epsg.io/5070)
# open 03_createdbpostgis.py, enter your postgresql password in lines 18 AND 29

system('C:\\Users\\yaa291\\AppData\\Local\\Continuum\\anaconda2\\python.exe C:\\Users\\yaa291\\Desktop\\BC_NewCode\\code\\03_createdbpostgis.py')

#Alternatives to running line of code above:


  #A: Manually: Open pgAdmin4,
                # expand: Servers then PostgreSQL 10 - enter password when prompted
                # expand PostgreSQL 10
                # right click Databases -> Create -> Database
                # enter database name: bc
                # right click bc -> CREATE script
                # paste: create extension postgis;
                # run (click on little lightining symbol at the top of the window)
                # paste:INSERT into spatial_ref_sys (srid, auth_name, auth_srid, proj4text, srtext) values ( 5070, 'EPSG', 5070, '+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs ', 'PROJCS["NAD83 / Conus Albers",GEOGCS["NAD83",DATUM["North_American_Datum_1983",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6269"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4269"]],PROJECTION["Albers_Conic_Equal_Area"],PARAMETER["standard_parallel_1",29.5],PARAMETER["standard_parallel_2",45.5],PARAMETER["latitude_of_center",23],PARAMETER["longitude_of_center",-96],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["X",EAST],AXIS["Y",NORTH],AUTHORITY["EPSG","5070"]]');
                # run (click on little lightining symbol at the top of the window)

  #B: Open the Command Prompt (in windows)
                # cd C:\Program Files\PostgreSQL\10\bin --- or the path to this folder on your computer
                # psql -h localhost -p 5432 -U postgres  --- after the -U enter your username, by default this is postgres
                # supply password
                # CREATE DATABASE bc; --- where bc is the database name
                #  \c bc;
                # create extension postgis;
                # INSERT into spatial_ref_sys (srid, auth_name, auth_srid, proj4text, srtext) values ( 5070, 'EPSG', 5070, '+proj=aea +lat_1=29.5 +lat_2=45.5 +lat_0=23 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs ', 'PROJCS["NAD83 / Conus Albers",GEOGCS["NAD83",DATUM["North_American_Datum_1983",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6269"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4269"]],PROJECTION["Albers_Conic_Equal_Area"],PARAMETER["standard_parallel_1",29.5],PARAMETER["standard_parallel_2",45.5],PARAMETER["latitude_of_center",23],PARAMETER["longitude_of_center",-96],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["X",EAST],AXIS["Y",NORTH],AUTHORITY["EPSG","5070"]]');

################
#     STEP 4   #
################

#4 Import shapefiles into PostgreSQL 

yourpassword = 'password'
conn <- dbConnect(drv = "PostgreSQL", host = "localhost", dbname = "bc", user = "postgres", password = 'postgres')

setwd(paste0(mypath, 'shapefiles/'))
allshapefiles = list.files(pattern = '.shp')
allshapefiles = allshapefiles[!allshapefiles %in% grep('.xml', allshapefiles, value = T)]


for (i in 14: length(allshapefiles)){

shapefile = readOGR(dsn = paste0(allshapefiles[i]), layer = paste0(sapply(strsplit(allshapefiles[i],".",fixed = TRUE),"[[",1)), stringsAsFactors = FALSE, pointDropZ = T)
pgInsert(conn, paste0(sapply(strsplit(allshapefiles[i],".",fixed = TRUE),"[[",1)), shapefile)

rm(shapefile); gc();

}

rm(allshapefiles, conn); gc();


################
#     STEP 5   #
################

#5 Run sql code to extract gis variables


bc_psql <- src_postgres(dbname = 'bc',
                        host = 'localhost',
                        port = 5432,
                        user = 'postgres',
                        password = yourpassword)


dbGetQuery(bc_psql$con,"update _addresses set id = trim(id, '\"')")


# -- spatial join STEP 01 -- verify that all points are within the modelextent1km
# -- only the points within the modelextent1km boundry will be included in step01 table
# -- requires specify _addresses.geom and all fields
dbGetQuery(bc_psql$con, "create table step01 as (SELECT id, _addresses.geom, lat, lng, elev_m, dvhi_1km, dvlo_1km, imp1km
FROM _addresses, _modelextent1km WHERE ST_Within(_addresses.geom, _modelextent1km.geom));")


#-- spatial join >> grab data from blockgroup dataset
dbGetQuery(bc_psql$con, 'create table step02 as (SELECT DISTINCT ON (a.id) a.id, a.geom, a.lat, a.lng, a.elev_m, a.dvhi_1km, a.dvlo_1km, a.imp1km, bg."FIPS", bg.pop_sqkm
	FROM step01 a
		LEFT JOIN _midatlanewengbg00 bg ON ST_DWithin(a.geom, bg.geom, 1000)
	ORDER BY a.id, ST_Distance(a.geom, bg.geom));')


#--------  Calculate the distance to the coast line
dbGetQuery(bc_psql$con, "alter table step02 add column coastdis double precision;")

dbGetQuery(bc_psql$con, 'update step02 set coastdis = sub.dist from (SELECT DISTINCT ON (step02.id) ST_Distance(step02.geom, "_Coast".geom)  as dist, step02.id as sm
FROM step02, "_Coast"
ORDER BY step02.id, ST_Distance(step02.geom, "_Coast".geom)) as sub where step02.id = sub.sm;')

#------ Calculate the distance to the countway library
dbGetQuery(bc_psql$con, "alter table step02 add column countway_m double precision;")

dbGetQuery(bc_psql$con, 'update step02 set countway_m = sub.dist from (SELECT DISTINCT ON (step02.id) ST_Distance(step02.geom, "_Countway".geom)  as dist, step02.id as sm
FROM step02, "_Countway"
ORDER BY step02.id, ST_Distance(step02.geom, "_Countway".geom)) as sub where step02.id = sub.sm;')

#-- spatial join >> grab a field (modelregio) from allregions dataset
dbGetQuery(bc_psql$con, "alter table step02 add column modelreg character varying;")

dbGetQuery(bc_psql$con, "update step02 set modelreg = sub.modelregio from (
SELECT DISTINCT ON (a.id) a.id, bg.modelregio
	FROM step02 a
		LEFT JOIN _allregions bg ON ST_DWithin(a.geom, bg.geom, 100)) as sub where step02.id = sub.id;")

#---------- calculating pct variables
dbGetQuery(bc_psql$con, "alter table step02 add column pctdvhif12 double precision, add column pctdvlof12 double precision;")

dbGetQuery(bc_psql$con, "update step02 set pctdvhif12 = (dvhi_1km/144)*100;")
dbGetQuery(bc_psql$con, "update step02 set pctdvlof12 = (dvlo_1km/144)*100;")

dbGetQuery(bc_psql$con, "ALTER TABLE step02 DROP COLUMN dvhi_1km;")
dbGetQuery(bc_psql$con, "ALTER TABLE step02 DROP COLUMN dvlo_1km;")


#------ Calculate the distance to nearest RTA, and grab RTA_flag values
dbGetQuery(bc_psql$con, "alter table step02 add column rta_flag int, add column disttorta_m double precision;")
#

dbGetQuery(bc_psql$con, 'update step02 set rta_flag = sub."RTA_flag", disttorta_m = sub.rta_dis from (
SELECT DISTINCT ON (a.id) a.id, bg."RTA_flag", ST_Distance(a.geom, bg.geom) as rta_dis
	FROM step02 a
		LEFT JOIN "_RTA" bg ON ST_DWithin(a.geom, bg.geom, 100000) ORDER BY a.id, ST_Distance(a.geom, bg.geom)) as sub where step02.id = sub.id ;')


#------ Calculate the distance to the nearest truck route
dbGetQuery(bc_psql$con, "alter table step02 add column dsttrkrt_m double precision;")

dbGetQuery(bc_psql$con, "update step02 set dsttrkrt_m = sub.dist from (SELECT DISTINCT ON (step02.id) ST_Distance(step02.geom, _truckrtes.geom)  as dist, step02.id as sm
FROM step02, _truckrtes
ORDER BY step02.id, ST_Distance(step02.geom, _truckrtes.geom)) as sub where step02.id = sub.sm;")

#-- spatial join with pbl2003 >> grab a field (pblid) from pbl2003 dataset
dbGetQuery(bc_psql$con, "alter table step02 add column pblid character varying;")

dbGetQuery(bc_psql$con, "update step02 set pblid = sub.pblid from (
SELECT DISTINCT ON (a.id) a.id, bg.pblid
	FROM step02 a
		LEFT JOIN _pblid bg ON ST_DWithin(a.geom, bg.geom, 35000) ORDER BY a.id, ST_Distance(a.geom, bg.geom)) as sub where step02.id = sub.id ;")

#--NEAR select the 20 nearest stations

dbGetQuery(bc_psql$con, "alter table _stations add column gid SERIAL;")

dbGetQuery(bc_psql$con, 'create table step18 as(
SELECT st.geom, st.id as id, st.lat as lat, st.lng as lng, stp."NEAR_FID" as near_fid, stp.usaf_wban as usaf_wban, ST_Distance(st.geom, stp.geom) AS distance, ST_Azimuth(st.geom, stp.geom)/(2*pi())*360 as degAZ FROM
step01 AS st CROSS JOIN LATERAL
(SELECT _stations.gid, _stations.geom, _stations.usaf_wban,  _stations."NEAR_FID" FROM _stations ORDER BY st.geom <-> _stations.geom LIMIT 20) AS stp  order by st.id);')


#------ Measure the disatance to ge10kadt, and grab AADT values
dbGetQuery(bc_psql$con, "alter table step02 add column aadt double precision, add column disttoge10k double precision;")

dbGetQuery(bc_psql$con, 'update step02 set aadt = sub."AADT", disttoge10k = sub.ge10k_dis from (
SELECT DISTINCT ON (a.id) a.id, bg."AADT", ST_Distance(a.geom, bg.geom) as ge10k_dis
	FROM step02 a
		LEFT JOIN _ge10kadt bg ON ST_DWithin(a.geom, bg.geom, 10000) ORDER BY a.id, ST_Distance(a.geom, bg.geom)) as sub where step02.id = sub.id ;')


#------ Calculate Distance to 3 different road types
dbGetQuery(bc_psql$con, "alter table step02 add column dist1rd double precision, add column dist2rd double precision, add column dist3rd double precision;")

dbGetQuery(bc_psql$con, "update step02 set  dist1rd = sub.dist from (
SELECT DISTINCT ON (step02.id) ST_Distance(step02.geom, _hpms1rd.geom)  as dist, step02.id as sm
FROM step02, _hpms1rd
ORDER BY step02.id, ST_Distance(step02.geom, _hpms1rd.geom)
) as sub where step02.id = sub.sm;")

dbGetQuery(bc_psql$con, "update step02 set  dist2rd = sub.dist from (
SELECT DISTINCT ON (step02.id) ST_Distance(step02.geom, _hpms2rd.geom)  as dist, step02.id as sm
FROM step02, _hpms2rd
ORDER BY step02.id, ST_Distance(step02.geom, _hpms2rd.geom)
) as sub where step02.id = sub.sm;")

dbGetQuery(bc_psql$con, "update step02 set  dist3rd = sub.dist from (
SELECT DISTINCT ON (step02.id) ST_Distance(step02.geom, _hpms3rd.geom)  as dist, step02.id as sm
FROM step02, _hpms3rd
ORDER BY step02.id, ST_Distance(step02.geom, _hpms3rd.geom)
) as sub where step02.id = sub.sm;")

#------ Calculate distance to rail and retrieve rail type
dbGetQuery(bc_psql$con, "alter table step02 add column disttorail double precision, add column fullname character varying;")

dbGetQuery(bc_psql$con, 'update step02 set fullname = sub."FULLNAME", disttorail = sub.rail_dis from (
SELECT DISTINCT ON (a.id) a.id, bg."FULLNAME", ST_Distance(a.geom, bg.geom) as rail_dis
	FROM step02 a
		LEFT JOIN "_Rail" bg ON ST_DWithin(a.geom, bg.geom, 10000) ORDER BY a.id, ST_Distance(a.geom, bg.geom)) as sub where step02.id = sub.id ;')

#------ calculate distance to major road and retrieve LRSKEY
dbGetQuery(bc_psql$con, "alter table step02 add column disttomjrrd double precision, add column lrskey_mjrrd character varying;")

dbGetQuery(bc_psql$con, 'update step02 set lrskey_mjrrd = sub."LRSKEY", disttomjrrd = sub.mjrrd_dis from (
SELECT DISTINCT ON (a.id) a.id, bg."LRSKEY", ST_Distance(a.geom, bg.geom) as mjrrd_dis
	FROM step02 a
		LEFT JOIN "_Mjrrd" bg ON ST_DWithin(a.geom, bg.geom, 100000) ORDER BY a.id, ST_Distance(a.geom, bg.geom)) as sub where step02.id = sub.id ;')

#------ Calculate distance to mbta
dbGetQuery(bc_psql$con, "alter table step02 add column disttombtabus double precision;")

dbGetQuery(bc_psql$con, "update step02 set disttombtabus = sub.mbtabus from (
SELECT DISTINCT ON (a.id) a.id, ST_Distance(a.geom, bg.geom) as mbtabus
	FROM step02 a
		LEFT JOIN _mbta bg ON ST_DWithin(a.geom, bg.geom, 10000) ORDER BY a.id, ST_Distance(a.geom, bg.geom)) as sub where step02.id = sub.id ;")

#------ retrieve surface water area near address within 2km and 10km buffers
dbGetQuery(bc_psql$con, "alter table step02 add column w_area2k double precision, add column w_area10k double precision;")

#---- buffer 2 KM
dbGetQuery(bc_psql$con, 'update step02 set w_area2k = sub.sum from(
SELECT DISTINCT ON (buff.id) buff.id,  coalesce(sum(ST_Area(ST_Intersection(st_buffer(buff.geom, 2000), "_WaterBodies".geom))), 0) as sum
FROM step02 as buff left join "_WaterBodies" on ST_Intersects(st_buffer(buff.geom, 2000), "_WaterBodies".geom) group by buff.id)  as sub where step02.id = sub.id;')

#---- buffer 10 KM
dbGetQuery(bc_psql$con, 'update step02 set w_area10k = sub.sum from(
SELECT DISTINCT ON (buff.id) buff.id,  coalesce(sum(ST_Area(ST_Intersection(st_buffer(buff.geom, 10000), "_WaterBodies".geom))), 0) as sum
FROM step02 as buff left join "_WaterBodies" on ST_Intersects(st_buffer(buff.geom, 10000), "_WaterBodies".geom) group by buff.id)  as sub where step02.id = sub.id;')

#------ fetch nearest spatially varying wind information (NOAA 32 km)
dbGetQuery(bc_psql$con, "alter table step02 add column latid bigint, add column lonid bigint;")
dbGetQuery(bc_psql$con, "update step02 set latid = sub.latid, lonid = sub.lonid from (
SELECT DISTINCT ON (a.id) a.id, bg.latid, bg.lonid
	FROM step02 a
		LEFT JOIN _vwind bg ON ST_DWithin(a.geom, bg.geom, 35000) ORDER BY a.id, ST_Distance(a.geom, bg.geom)) as sub where step02.id = sub.id ;")


#-------------------------- ROAD TRANSPORT INTERSECTIONS

#--50 m

dbGetQuery(bc_psql$con, "alter table step02 add column b50m_a2rd double precision;")
dbGetQuery(bc_psql$con, "alter table step02 add column b50m_a3rd double precision;")
dbGetQuery(bc_psql$con, "alter table step02 add column b50m_rta double precision;")
dbGetQuery(bc_psql$con, "alter table step02 add column b50m_trkrte double precision;")


dbGetQuery(bc_psql$con, "update step02 set b50m_a2rd = sub.sum from (
SELECT DISTINCT ON (buff.id) buff.id,  coalesce(sum(ST_Length(ST_Intersection(st_buffer(buff.geom, 50), _hpms2rd.geom))), 0) as sum
FROM step02 as buff left join _hpms2rd on ST_Intersects(st_buffer(buff.geom, 50), _hpms2rd.geom) group by buff.id) as sub where step02.id = sub.id;")

dbGetQuery(bc_psql$con, "update step02 set b50m_a3rd = sub.sum from (
SELECT DISTINCT ON (buff.id) buff.id,  coalesce(sum(ST_Length(ST_Intersection(st_buffer(buff.geom, 50), _hpms3rd.geom))), 0) as sum
FROM step02 as buff left join _hpms3rd on ST_Intersects(st_buffer(buff.geom, 50), _hpms3rd.geom) group by buff.id) as sub where step02.id = sub.id;")

dbGetQuery(bc_psql$con, "update step02 set b50m_rta = sub.sum from (
SELECT DISTINCT ON (buff.id) buff.id,  coalesce(sum(ST_Length(ST_Intersection(st_buffer(buff.geom, 50), _rta.geom))), 0) as sum
FROM step02 as buff left join _rta on ST_Intersects(st_buffer(buff.geom, 50), _rta.geom) group by buff.id) as sub where step02.id = sub.id;")

dbGetQuery(bc_psql$con, "update step02 set b50m_trkrte = sub.sum from (
SELECT DISTINCT ON (buff.id) buff.id,  coalesce(sum(ST_Length(ST_Intersection(st_buffer(buff.geom, 50), _truckrtes.geom))), 0) as sum
FROM step02 as buff left join _truckrtes on ST_Intersects(st_buffer(buff.geom, 50), _truckrtes.geom) group by buff.id) as sub where step02.id = sub.id;")


#-------- 100m

dbGetQuery(bc_psql$con, "alter table step02 add column b100m_a1rd double precision;")
dbGetQuery(bc_psql$con, "alter table step02 add column b100m_hpms double precision;")
dbGetQuery(bc_psql$con, "alter table step02 add column b100m_trkrte double precision;")
dbGetQuery(bc_psql$con, "alter table step02 add column bus_100m double precision;")

dbGetQuery(bc_psql$con, "update step02 set b100m_a1rd = sub.sum from (
SELECT DISTINCT ON (buff.id) buff.id,  coalesce(sum(ST_Length(ST_Intersection(st_buffer(buff.geom, 100), _hpms1rd.geom))), 0) as sum
FROM step02 as buff left join _hpms1rd on ST_Intersects(st_buffer(buff.geom, 100), _hpms1rd.geom) group by buff.id) as sub where step02.id = sub.id;")

dbGetQuery(bc_psql$con, "update step02 set b100m_hpms = sub.sum from (
SELECT DISTINCT ON (buff.id) buff.id,  coalesce(sum(ST_Length(ST_Intersection(st_buffer(buff.geom, 100), _hmps13.geom))), 0) as sum
FROM step02 as buff left join _hmps13 on ST_Intersects(st_buffer(buff.geom, 100), _hmps13.geom) group by buff.id) as sub where step02.id = sub.id;")

dbGetQuery(bc_psql$con, "update step02 set b100m_trkrte = sub.sum from (
SELECT DISTINCT ON (buff.id) buff.id,  coalesce(sum(ST_Length(ST_Intersection(st_buffer(buff.geom, 100), _truckrtes.geom))), 0) as sum
FROM step02 as buff left join _truckrtes on ST_Intersects(st_buffer(buff.geom, 100), _truckrtes.geom) group by buff.id) as sub where step02.id = sub.id;")

dbGetQuery(bc_psql$con, "update step02 set bus_100m = sub.sum from (
SELECT DISTINCT ON (buff.id) buff.id,  coalesce(sum(ST_Length(ST_Intersection(st_buffer(buff.geom, 100), _rta.geom))), 0) as sum
FROM step02 as buff left join _rta on ST_Intersects(st_buffer(buff.geom, 100), _rta.geom) group by buff.id) as sub where step02.id = sub.id;")


# extract aadt for hpms 

#dbGetQuery(bc_psql$con, 'create table step23 as (SELECT DISTINCT ON (buff.id) buff.id,  ST_Length(ST_Intersection(st_buffer(buff.geom, 100), _hmps13.geom), 0) 
#           FROM step02 as buff left join _hmps13 on ST_Intersects(st_buffer(buff.geom, 100), _hmps13.geom) group by buff.id) as sub where step02.id = sub.id;')



#-------- 200 m

dbGetQuery(bc_psql$con, "alter table step02 add column b200m_a2rd double precision;")
dbGetQuery(bc_psql$con, "alter table step02 add column b200m_a3rd double precision;")
dbGetQuery(bc_psql$con, "alter table step02 add column b200m_rta double precision;")
dbGetQuery(bc_psql$con, "alter table step02 add column b200m_trkrte double precision;")
dbGetQuery(bc_psql$con, "alter table step02 add column b200m_hpms double precision;")


dbGetQuery(bc_psql$con, "update step02 set b200m_a2rd = sub.sum from (
SELECT DISTINCT ON (buff.id) buff.id,  coalesce(sum(ST_Length(ST_Intersection(st_buffer(buff.geom, 200), _hpms2rd.geom))), 0) as sum
FROM step02 as buff left join _hpms2rd on ST_Intersects(st_buffer(buff.geom, 200), _hpms2rd.geom) group by buff.id) as sub where step02.id = sub.id;")


dbGetQuery(bc_psql$con, "update step02 set b200m_a3rd = sub.sum from (
SELECT DISTINCT ON (buff.id) buff.id,  coalesce(sum(ST_Length(ST_Intersection(st_buffer(buff.geom, 200), _hpms3rd.geom))), 0) as sum
FROM step02 as buff left join _hpms3rd on ST_Intersects(st_buffer(buff.geom, 200), _hpms3rd.geom) group by buff.id) as sub where step02.id = sub.id;")

dbGetQuery(bc_psql$con, "update step02 set b200m_rta = sub.sum from (
SELECT DISTINCT ON (buff.id) buff.id,  coalesce(sum(ST_Length(ST_Intersection(st_buffer(buff.geom, 200), _rta.geom))), 0) as sum
FROM step02 as buff left join _rta on ST_Intersects(st_buffer(buff.geom, 200), _rta.geom) group by buff.id) as sub where step02.id = sub.id;")

dbGetQuery(bc_psql$con, "update step02 set b200m_trkrte = sub.sum from (
SELECT DISTINCT ON (buff.id) buff.id,  coalesce(sum(ST_Length(ST_Intersection(st_buffer(buff.geom, 200), _truckrtes.geom))), 0) as sum
FROM step02 as buff left join _truckrtes on ST_Intersects(st_buffer(buff.geom, 200), _truckrtes.geom) group by buff.id) as sub where step02.id = sub.id;")

dbGetQuery(bc_psql$con, "update step02 set b200m_hpms = sub.sum from (
SELECT DISTINCT ON (buff.id) buff.id,  coalesce(sum(ST_Length(ST_Intersection(st_buffer(buff.geom, 200), _hmps13.geom))), 0) as sum
FROM step02 as buff left join _hmps13 on ST_Intersects(st_buffer(buff.geom, 200), _hmps13.geom) group by buff.id) as sub where step02.id = sub.id;")


-------- 300 m


dbGetQuery(bc_psql$con, "alter table step02 add column b300m_a2rd double precision;")
dbGetQuery(bc_psql$con, "alter table step02 add column b300m_a3rd double precision;")
dbGetQuery(bc_psql$con, "alter table step02 add column b300m_trkrte double precision;")


dbGetQuery(bc_psql$con, "update step02 set b300m_a2rd = sub.sum from (
SELECT DISTINCT ON (buff.id) buff.id,  coalesce(sum(ST_Length(ST_Intersection(st_buffer(buff.geom, 300), _hpms2rd.geom))), 0) as sum
FROM step02 as buff left join _hpms2rd on ST_Intersects(st_buffer(buff.geom, 300), _hpms2rd.geom) group by buff.id) as sub where step02.id = sub.id;")


dbGetQuery(bc_psql$con, "update step02 set b300m_a3rd = sub.sum from (
SELECT DISTINCT ON (buff.id) buff.id,  coalesce(sum(ST_Length(ST_Intersection(st_buffer(buff.geom, 300), _hpms3rd.geom))), 0) as sum
FROM step02 as buff left join _hpms3rd on ST_Intersects(st_buffer(buff.geom, 300), _hpms3rd.geom) group by buff.id) as sub where step02.id = sub.id;")


dbGetQuery(bc_psql$con, "update step02 set b300m_trkrte = sub.sum from (
SELECT DISTINCT ON (buff.id) buff.id,  coalesce(sum(ST_Length(ST_Intersection(st_buffer(buff.geom, 300), _truckrtes.geom))), 0) as sum
FROM step02 as buff left join _truckrtes on ST_Intersects(st_buffer(buff.geom, 300), _truckrtes.geom) group by buff.id) as sub where step02.id = sub.id;")


################
#     STEP 6   #
################
#6 - Export files from PostgreSQL into R

gis_psql = tbl(bc_psql, "step02")
gis_psql = as.data.frame(gis_psql)

summary(gis_psql)

yourfilepath = ''
saveRDS(gis_psql, paste0(yourfilepath,'/gisoutput.rds'))


meteo_psql  = tbl(bc_psql, "step18")
meteo_psql = as.data.frame(meteo_psql)

saveRDS(meteo_psql, paste0(yourfilepath,'/meteo.rds'))


on.exit(dbUnloadDriver(drv), add = TRUE)
