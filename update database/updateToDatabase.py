import pandas as pd
import psycopg2
  
connection = psycopg2.connect(database="diagnostics_prod",
                        user='diagnostics_role', password='diagnostics123', 
                        host='diag-prod.c6f7bs1lumk2.ap-south-1.rds.amazonaws.com', port='5432'
)
  
connection.autocommit = True
cursor = connection.cursor()

gatedata = pd.read_csv("add _gateways_to_diagnostics.csv")

for gateId,siteID in zip(gatedata[0],gatedata[1]) :
     cursor.execute('''INSERT INTO organisations ("serialNumber", "siteID") VALUES (%s, %s)''',(gateId, siteID))
     connection.commit()

gatedeldata = pd.read_csv("remove _gateways_from_diagnostics.csv")

for gateId in gatedeldata[0] :
     cursor.execute('''DELETE FROM organisations WHERE serialNumber=%s ''',(gateId))
     connection.commit()

data = pd.read_csv("add _organisation_to_diagnostics.csv")

for orgId,name,orgType,parId,intId in zip(data[0],data[1],data[2],data[3],data[4]) :
     cursor.execute('''INSERT INTO organisations ("id", "name", "type", "partnerId", "integratorId") VALUES (%s, %s, %s, %s, %s)''',(orgId, name, orgType, parId, intId))
     connection.commit()

data = pd.read_csv("remove _organisation_from_diagnostics.csv")

for orgId in data[0] :
     cursor.execute('''DELETE FROM organisations WHERE id=%s ''',(orgId))
     connection.commit()


    
siteAdd = pd.read_csv("add _sites_to_diagnostics.csv")

for siteId,siteName,siteLocation,orgId in zip(siteAdd[0],siteAdd[1],siteAdd[2],siteAdd[3]) :
    cursor.execute('''INSERT INTO sites ("id", "name", "location", "organisationId") VALUES (%s, %s, %s, %s)''',(siteId, siteName, siteLocation, orgId))
    connection.commit()

sitedel = pd.read_csv("remove _organisation_from_diagnostics.csv")

for siteId in sitedel[0] :
     cursor.execute('''DELETE FROM sites WHERE id=%s ''',(siteId))
     connection.commit()
    
data = pd.read_csv("add _access_points_to_diagnostics.csv")

for accessPointId,accessPointName,orgId,siteId,accessPointType in zip(data[0],data[1]) :
     cursor.execute('''INSERT INTO access_points ("id", "name") VALUES (%s, %s)''',(accessPointId, accessPointName))
     connection.commit()

accpdel = pd.read_csv("remove _access_points_from_diagnostics.csv")

for accpId in accpdel[0] :
     cursor.execute('''DELETE FROM access_points WHERE id=%s ''',(accpId))
     connection.commit()


    
data = pd.read_csv("add _devices_to_diagnostics.csv")

for serialNumber,accessPointId,deviceType in zip(data["serialNumber"],data["accessPointId"],data["deviceType"]) :
     cursor.execute('''INSERT INTO devices ("serialNumber", "accessPointId") VALUES (%s, %s)''',(serialNumber, accessPointId))
     connection.commit()

devdel = pd.read_csv("remove _devices_from_diagnostics.csv")

for devId in devdel[0] :
     cursor.execute('''DELETE FROM devices WHERE serialNumber=%s ''',(devId))
     connection.commit()
    

connection.close()
