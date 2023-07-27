import requests  # retrieving API
import xml.etree.ElementTree as ET  # for parsing XML
import pandas as pd  # data frames

#URL
url="https://api.statistiken.bundesbank.de/rest/data/BBK01/SUD231"

#Getting request for url:
xml_data=requests.get(url, allow_redirects=True).content


root = ET.fromstring(xml_data)
data = []

for child in root.iter("*"):
    row = {}
    #for subchild in child:
    row[child.tag] = list(child.attrib.values())
    data.append(row)
#print(child.tag)
print(data)
df = pd.DataFrame(data)
#print(df)

#Generating list for each one of the desired columns and changing format from dict values:
col1 = df['{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}ObsDimension'].tolist()
zeit = [item for item in col1 if not(pd.isnull(item)) == True]
col2 = df['{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}ObsValue'].tolist()
wert = [item for item in col2 if not(pd.isnull(item)) == True]
col3 = df['{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}Obs'].tolist()
wertstatus = [item for item in col3 if not(pd.isnull(item)) == True]
#print(wertstatus)

data_bundesbank = pd.DataFrame(
    {'Zeit': zeit,
     'Wert': wert,
     'Wertstatus': wertstatus
    })

#Remove brackets from lists
data_bundesbank['Zeit'] = data_bundesbank['Zeit'].str.get(0)
data_bundesbank['Wert'] = data_bundesbank['Wert'].str.get(0)
data_bundesbank['Wertstatus'] = data_bundesbank['Wertstatus'].str.get(0)
print(data_bundesbank)
print(data_bundesbank.to_csv('/Users/juancamilovasquezbarrera/Documents/out.csv')  )


#Connetion with MySQL Workbench
import mysql.connector as mysql
import csv

#Connect DB server and database
conn = mysql.connect(user='root', password='password', host='127.0.0.1', database='test_new', auth_plugin='mysql_native_password')
cursor = conn.cursor()

dict_list = list()
#open the csv file
with open('Path.csv', mode='r') as csv_file:
    #read csv using reader class
    csv_reader = csv.reader(csv_file)
    #skip header
    header = next(csv_reader)
    #Read csv row wise and insert into table
    for row in csv_reader:
        dict_list.append({'Row':row[0], 'Zeit':row[1], 'Wert':row[2]})
    
    #cursor.execute("CREATE TABLE bundesbanktable (Row_ VARCHAR(255), Zeit VARCHAR(255), Wert VARCHAR(255));")
    cursor.execute("DELETE FROM bundesbanktable")
    
    #First Insert:
    for item in dict_list:
        sql = "INSERT INTO bundesbanktable(Row_, Zeit, Wert) VALUES (%s, %s, %s)"
        val = item['Row'],item['Zeit'], item['Wert']
        cursor.execute(sql, val)
    conn.commit()

    