####################################################################################
#Objective: To call the BOLD API for PL1,PL2 and PL3 and return the industry codes 
#
#Version 2021.05.05
# By: Roberto Gonzalez
# 
####################################################################################

import time
from datetime import datetime
import requests
import pandas as pd
import pyodbc
import sys
import logging
import os

pgs = 24000   #Number of pages to loop through for ID's
idsPage = '100' #Number of ID's per page
tSleep = 1    #Time to pause per call

def key_PL1():
    
    global token_PL1

    headers = {'Content-Type': 'application/x-www-form-urlencoded',
    'Ocp-Apim-Subscription-Key': 'Subscription KEY'}

    avionte = 'https://partner-api.avionte.com/'
    address = avionte + "authorize/token?"

    body = {'grant_type': 'client_credentials',
            'client_id':'partner.api.pl1',
            'client_secret':'SECRET',
            'scope':'avionte.aero.compasintegrationservice'}
    response = requests.post(address, headers=headers,data=body)

    response.raise_for_status()

    data = response.json()
    token_PL1 = data['access_token']
    return token_PL1

def bearer_PL1():
    global headers_PL1

    bearer = 'Bearer ' + token_PL1
    headers_PL1 = {
        'Tenant':'pl1',
        'authorization': bearer,
        'Ocp-Apim-Subscription-Key': 'Primary KEY',     #Primary
        #'Ocp-Apim-Subscription-Key':'Secondary KEY',     #Secondary
        'content-type': "application/json"

        }
    return headers_PL1

def key_PL2():
    global token_PL2

    headers = {'Content-Type': 'application/x-www-form-urlencoded',
    'Ocp-Apim-Subscription-Key': 'Key'}

    avionte = 'https://partner-api.avionte.com/'
    address = avionte + "authorize/token?"

    body = {'grant_type': 'client_credentials',
            'client_id':'partner.api.pl1',
            'client_secret':'SECRET',
            'scope':'avionte.aero.compasintegrationservice'}
    response = requests.post(address, headers=headers,data=body)

    response.raise_for_status()

    data = response.json()
    token_PL2 = data['access_token']
    return token_PL2

def bearer_PL2():
    global headers_PL2

    bearer = 'Bearer ' + token_PL2
    headers_PL2 = {
        'Tenant':'pl2',
        'authorization': bearer,
        'Ocp-Apim-Subscription-Key': 'Primary KEY',     #Primary
        #'Ocp-Apim-Subscription-Key':'Secondary KEY',     #Secondary
        'content-type': "application/json"

        }
    return headers_PL2

def key_PL3():
    global token_PL3

    headers = {'Content-Type': 'application/x-www-form-urlencoded',
    'Ocp-Apim-Subscription-Key': 'KEY'}

    avionte = 'https://partner-api.avionte.com/'
    address = avionte + "authorize/token?"

    body = {'grant_type': 'client_credentials',
            'client_id':'partner.api.pl1',
            'client_secret':'SECRET',
            'scope':'avionte.aero.compasintegrationservice'}
    response = requests.post(address, headers=headers,data=body)

    response.raise_for_status()

    data = response.json()
    token_PL3 = data['access_token']
    return token_PL3

def bearer_PL3():
    global headers_PL3

    bearer = 'Bearer ' + token_PL3
    headers_PL3 = {
        'Tenant':'pl3',
        'authorization': bearer,
        'Ocp-Apim-Subscription-Key': 'Primary KEY',     #Primary
        #'Ocp-Apim-Subscription-Key':'Secondary KEY',     #Secondary
        'content-type': "application/json"

        }
    return headers_PL3

def sql(row_name):
    conn =  pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=' + server+ ';Database=' + database + ';Trusted_Connection=yes')
    cursor = conn.cursor()

    for row in range(1):
        cursor.execute('''
                
              INSERT INTO [dbo].[IndustryCode] (id,name,billingAddress,frontOfficeId,link,isArchived,representativeUsers,statusId,status,industry,
                    createdDate,lastUpdatedDate,lastActivityDate,latestActivityName,openJobs,street1,street2,city,stateOrProvince,postalCode,country,
                        county,geoCode,schoolDistrictCode,dbsource) 
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ''',
                (
                field1,     #id
                field2,     #name
                field3,     #billingAddress
                field4,     #frontOfficeId
                field5,     #link
                field6,     #isArchived
                field7,     #representativeUsers
                field8,     #statusId
                field9,     #status
                field10,    #industry
                field11,    #createdDate
                field12,    #lastUpdatedDate
                field13,    #lastActivityDate
                field14,    #latestActivityName
                field15,    #openJobs
                field16,    #street1
                field17,    #street2
                field18,    #city
                field19,    #stateOrProvince
                field20,    #postalCode
                field21,    #country
                field22,    #county
                field23,    #geoCode
                field24,    #schoolDistrictCode
                row_name     #dbsource
                )
            )

        cursor.commit()

def connection():
    global server
    global database

    server = 'SERVER NAME'
    database = 'SQL TABLE'

    conn =  pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server='+ server+ ';Database=' + database + ';Trusted_Connection=yes')
    cursor = conn.cursor()

    cursor.execute('TRUNCATE TABLE dbo.IndustryCode')
    cursor.commit()

def rep():
    field7 = ''
    if not df_t['representativeUsers'].values[0]:
        field7 = 'None'
    else:
        field7 = df_t['representativeUsers'].values[0][0]
    return field7

################### Connecting to the server ###############################
connection()  
    
################### Connecting to BOLD API PL1###############################
key_PL1() #Getting 1st API key


for page in range(pgs): #loops one page and pulls a list of ids

    page = str(page)
    url = 'https://partner-api.avionte.com/front-office/v1/companies/ids/' + page + '/' + idsPage

    bearer_PL1()
    
    r = requests.request("GET", url, headers=headers_PL1)

    r.raise_for_status() #If there is an error, code stops here and displays error

    lists = r.json()
  
    body = str(lists)    #converts int(s) to string for URL
    url = 'https://partner-api.avionte.com/front-office/v1/companies/multi-query'

    response = requests.post(url,headers=headers_PL1,data=body)
    response.raise_for_status()
    data = response.json()

    if len(data) != 0:
        for i in range(len(data)):
            comp = (data[i])
            
            dfCompany = pd.DataFrame.from_dict(comp,orient='index')
            mainAddress = comp['mainAddress']
            dfMain = pd.DataFrame.from_dict(mainAddress,orient='index')
            dfFinal = dfCompany.append(dfMain)
            df = pd.DataFrame(dfFinal)
            df_t = df.T     #make rows to columns
            
            field1  = df_t['id'].values[0]
            field2  = df_t['name'].values[0]
            field3  = df_t['billingAddress'].values[0]
            field4  = df_t['frontOfficeId'].values[0]
            field5  = df_t['link'].values[0]
            field6  = df_t['isArchived'].values[0]
            field7  = rep()
            field8  = df_t['statusId'].values[0]
            field9  = df_t['status'].values[0]
            field10 = df_t['industry'].values[0]
            field11 = df_t['createdDate'].values[0]
            field12 = df_t['lastUpdatedDate'].values[0]
            field13 = df_t['latestActivityDate'].values[0]
            field14 = df_t['latestActivityName'].values[0]
            field15 = df_t['openJobs'].values[0]
            field16 = df_t['street1'].values[0]
            field17 = df_t['street2'].values[0]
            field18 = df_t['city'].values[0]
            field19 = df_t['state_Province'].values[0]
            field20 = df_t['postalCode'].values[0]
            field21 = df_t['country'].values[0]
            field22 = df_t['county'].values[0]
            field23 = df_t['geoCode'].values[0]
            field24 = df_t['schoolDistrictCode'].values[0]
            
            if not df_t['representativeUsers'].values[0]:
                df_t['representativeUsers'].values[0] = 'None'
            

            if 'T' in field11:
                field11 = field11.replace('T',' ')
            #print('Field 11 = ' + field11)

            if field12 == 'None':
                field12 = field12.replace('None','null')
            #print('Field 12 = ' + field12)
            if field13 == 'None':
                field13 = field13.replace('None','null')
            #print('Field 13 = ' + field13)

            sql('PL1')
            os.system('cls')
            print('Id ' + str(field1) + ' was entered from PL1.',flush=True)
        
        time.sleep(tSleep)  #sleeps after companies are called
        key_PL1()
    else:
        print('PL1 is Complete')
        break

################### Connecting to BOLD API PL2###############################
time.sleep(10)
key_PL2() #Getting API key for PL2


for page in range(pgs): #loops one page and pulls a list of ids

    page = str(page)
    url = 'https://partner-api.avionte.com/front-office/v1/companies/ids/' + page + '/' + idsPage

    bearer_PL2()
    
    r = requests.request("GET", url, headers=headers_PL2)

    r.raise_for_status() #If there is an error, code stops here and displays error

    lists = r.json()
  
    body = str(lists)    #converts int(s) to string for URL
    url = 'https://partner-api.avionte.com/front-office/v1/companies/multi-query'

    response = requests.post(url,headers=headers_PL2,data=body)
    response.raise_for_status()
    data = response.json()

    if len(data) != 0:
        for i in range(len(data)):
            comp = (data[i])
            
            dfCompany = pd.DataFrame.from_dict(comp,orient='index')
            mainAddress = comp['mainAddress']
            dfMain = pd.DataFrame.from_dict(mainAddress,orient='index')
            dfFinal = dfCompany.append(dfMain)
            df = pd.DataFrame(dfFinal)
            df_t = df.T     #make rows to columns
            
            field1  = df_t['id'].values[0]
            field2  = df_t['name'].values[0]
            field3  = df_t['billingAddress'].values[0]
            field4  = df_t['frontOfficeId'].values[0]
            field5  = df_t['link'].values[0]
            field6  = df_t['isArchived'].values[0]
            field7  = rep()
            field8  = df_t['statusId'].values[0]
            field9  = df_t['status'].values[0]
            field10 = df_t['industry'].values[0]
            field11 = df_t['createdDate'].values[0]
            field12 = df_t['lastUpdatedDate'].values[0]
            field13 = df_t['latestActivityDate'].values[0]
            field14 = df_t['latestActivityName'].values[0]
            field15 = df_t['openJobs'].values[0]
            field16 = df_t['street1'].values[0]
            field17 = df_t['street2'].values[0]
            field18 = df_t['city'].values[0]
            field19 = df_t['state_Province'].values[0]
            field20 = df_t['postalCode'].values[0]
            field21 = df_t['country'].values[0]
            field22 = df_t['county'].values[0]
            field23 = df_t['geoCode'].values[0]
            field24 = df_t['schoolDistrictCode'].values[0]
            
            if not df_t['representativeUsers'].values[0]:
                df_t['representativeUsers'].values[0] = 'None'
            

            if 'T' in field11:
                field11 = field11.replace('T',' ')
            #print('Field 11 = ' + field11)

            if field12 == 'None':
                field12 = field12.replace('None','null')
            #print('Field 12 = ' + field12)
            if field13 == 'None':
                field13 = field13.replace('None','null')
            #print('Field 13 = ' + field13)

            sql('PL2')
            os.system('cls')
            print('Id ' + str(field1) + ' was entered from PL2.',flush=True)
        
        time.sleep(tSleep)  #sleeps after companies are called
        key_PL2()
    else:
        print('PL2 is Complete')
        break

################### Connecting to BOLD API PL3###############################
time.sleep(10)
key_PL3() #Getting API key for PL3


for page in range(pgs): #loops one page and pulls a list of ids

    page = str(page)
    url = 'https://partner-api.avionte.com/front-office/v1/companies/ids/' + page + '/' + idsPage

    bearer_PL3()
    
    r = requests.request("GET", url, headers=headers_PL3)

    r.raise_for_status() #If there is an error, code stops here and displays error

    lists = r.json()
  
    body = str(lists)    #converts int(s) to string for URL
    url = 'https://partner-api.avionte.com/front-office/v1/companies/multi-query'

    response = requests.post(url,headers=headers_PL3,data=body)
    response.raise_for_status()
    data = response.json()

    if len(data) != 0:
        for i in range(len(data)):
            comp = (data[i])
            
            dfCompany = pd.DataFrame.from_dict(comp,orient='index')
            mainAddress = comp['mainAddress']
            dfMain = pd.DataFrame.from_dict(mainAddress,orient='index')
            dfFinal = dfCompany.append(dfMain)
            df = pd.DataFrame(dfFinal)
            df_t = df.T     #make rows to columns
            
            field1  = df_t['id'].values[0]
            field2  = df_t['name'].values[0]
            field3  = df_t['billingAddress'].values[0]
            field4  = df_t['frontOfficeId'].values[0]
            field5  = df_t['link'].values[0]
            field6  = df_t['isArchived'].values[0]
            field7  = rep()
            field8  = df_t['statusId'].values[0]
            field9  = df_t['status'].values[0]
            field10 = df_t['industry'].values[0]
            field11 = df_t['createdDate'].values[0]
            field12 = df_t['lastUpdatedDate'].values[0]
            field13 = df_t['latestActivityDate'].values[0]
            field14 = df_t['latestActivityName'].values[0]
            field15 = df_t['openJobs'].values[0]
            field16 = df_t['street1'].values[0]
            field17 = df_t['street2'].values[0]
            field18 = df_t['city'].values[0]
            field19 = df_t['state_Province'].values[0]
            field20 = df_t['postalCode'].values[0]
            field21 = df_t['country'].values[0]
            field22 = df_t['county'].values[0]
            field23 = df_t['geoCode'].values[0]
            field24 = df_t['schoolDistrictCode'].values[0]
            
            if not df_t['representativeUsers'].values[0]:
                df_t['representativeUsers'].values[0] = 'None'
            

            if 'T' in field11:
                field11 = field11.replace('T',' ')
            #print('Field 11 = ' + field11)

            if field12 == 'None':
                field12 = field12.replace('None','null')
            #print('Field 12 = ' + field12)
            if field13 == 'None':
                field13 = field13.replace('None','null')
            #print('Field 13 = ' + field13)

            sql('PL3')
            os.system('cls')
            print('Id ' + str(field1) + ' was entered from PL3.',flush=True)
        
        time.sleep(tSleep)  #sleeps after companies are called
        key_PL3()
    else:
        print('PL3 is Complete')
        break

now = datetime.now()

current_time = now.strftime("%H:%M:%S")
print("API's are now done. The done time is: ",current_time)      
        
    
