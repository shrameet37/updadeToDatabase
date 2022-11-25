import pandas as pd
import psycopg2
  
connection = psycopg2.connect(database="diagnostics_prod",
                        user='diagnostics_role', password='diagnostics123', 
                        host='diag-prod.c6f7bs1lumk2.ap-south-1.rds.amazonaws.com', port='5432'
)
  
connection.autocommit = True
cursor = connection.cursor()

data = pd.read_csv("org.csv")

for orgId,name,orgType,parId,intId in zip(data["id"],data["name"],data["type"],data["partnerId"],data["integratorId"]) :
     cursor.execute('''INSERT INTO organisations ("id", "name", "type", "partnerId", "integratorId") VALUES (%s, %s, %s, %s, %s)''',(orgId, name, orgType, parId, intId))
     connection.commit()
    
data = pd.read_csv("sites.csv")

for siteId,siteName,siteLocation,orgId in zip(data["id"],data["name"],data["location"],data["orgId"]) :
    cursor.execute('''INSERT INTO sites ("id", "name", "location", "organisationId") VALUES (%s, %s, %s, %s)''',(siteId, siteName, siteLocation, orgId))
    connection.commit()
    
data = pd.read_csv("accesspoints.csv")

for accessPointId,accessPointName,orgId,siteId,accessPointType in zip(data["id"],data["name"],data["orgId"],data["siteId"],data["accessPointType"]) :
     cursor.execute('''INSERT INTO access_points ("id", "name", "forAccess", "forAttendance", "siteId") VALUES (%s, %s, %s, %s, %s)''',(accessPointId, accessPointName, orgId, siteId, accessPointType))
     connection.commit()
    
data = pd.read_csv("devices.csv")

for serialNumber,accessPointId,deviceType in zip(data["serialNumber"],data["accessPointId"],data["deviceType"]) :
     cursor.execute('''INSERT INTO devices ("serialNumber", "accessPointId", "deviceType") VALUES (%s, %s, %s)''',(serialNumber, accessPointId, deviceType))
     connection.commit()
    

connection.close()
