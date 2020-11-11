import sqlite3
import requests
from bs4 import BeautifulSoup


url = "http://www.celestrak.com/NORAD/elements"
url_short = "http://www.celestrak.com"

connection = sqlite3.connect('satellite.db')
cursor = connection.cursor()

command_create = """CREATE TABLE IF NOT EXISTS Satellite_TLE(id INTEGER PRIMARY KEY, main_group TEXT, sub_group TEXT, name TEXT, tle TEXT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"""
command_create_archive = """CREATE TABLE IF NOT EXISTS Archive_TLE(id INTEGER PRIMARY KEY, tle_id INTEGER, archived_tle TEXT, new_tle TEXT, updated TEXT, FOREIGN KEY (tle_id) REFERENCES Satellite_TLE (id));"""
command_trigger = """CREATE TRIGGER IF NOT EXISTS archive_tle_after_update AFTER UPDATE ON Satellite_TLE WHEN old.tle <> new.tle BEGIN INSERT INTO Archive_TLE (tle_id, archived_tle, new_tle, updated) VALUES (old.id, old.tle, new.tle, DATETIME('NOW'));END;"""
command_insert = """INSERT OR IGNORE INTO Satellite_TLE(id, main_group, sub_group, name, tle) VALUES ((select id from Satellite_TLE where name=?),?, ?, ?, ?)"""
command_update = """UPDATE Satellite_TLE SET tle=? WHERE name=? and sub_group=? and main_group=?"""

cursor.execute(command_create)
cursor.execute(command_create_archive)
cursor.execute(command_trigger)

response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")
tables = soup.find("table")

def data_preprocesing(x):
    data = [a.replace('\r', '') for a in x if a != '\n']
    data = ''.join(data)
    data = data.split('\n')
    data.remove('')
    return data

def download_TLE(table):
    for link in table.findAll('a'):
        if (('.txt' in link.get('href')) or ('.php' in link.get('href'))) and ('https' not in link.get('href')) and ('frame' not in link.get('href')) and (not link.get('title')):
            if '.php' in link.get('href'):
                sub_group = link.getText()
                output = BeautifulSoup(requests.get(url_short + link.get('href')).text , 'html.parser').findAll(text=True)
                data = data_preprocesing(output)
            else:
                sub_group = link.getText()
                output = BeautifulSoup(requests.get(url + '/' + link.get('href')).text , 'html.parser').findAll(text=True)
                data = data_preprocesing(output)
            return sub_group, data

def tle(data, group, sub_group):
    for i in range(0,len(data),3):
        name = str(data[i].rstrip())
        tle = str(data[i+1] + '\n' + data[i+2])
        cursor.execute(command_insert, (name, group, sub_group, name, tle))
        cursor.execute(command_update, (tle, name, sub_group, group))
        #print(f"Group -> {group} \nSub group -> {sub_group} \nName -> {name} \nTLE -> {tle} \n\r")
    connection.commit()

table_list = []
for table in tables.findAll('table'):
    if table.findAll('th'):
        group = table.findAll('th')[0].getText()
        table_list.append(group)
        sub_group, data = download_TLE(table)
        tle(data, group, sub_group)
    elif not table.findAll('th'):
        group = table_list[-1]
        sub_group, data = download_TLE(table)
        tle(data, group, sub_group)


cursor.close
connection.close()
