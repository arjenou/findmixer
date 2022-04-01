from multiprocessing import Queue
import pymysql
import json
import zmq
import os
from datetime import datetime as dt

time_queue = Queue(500)
ctx = zmq.Context()



filedate = dt.today().strftime('%Y-%m-%d')

def create_folder():
    p=r'./data/'
    foldername = p+filedate
    word_name = os.path.exists(foldername)
    if not word_name:
        os.makedirs(foldername)

#-----------------DataBase creation
configopen = open('config.json','r')
config = json.load(configopen)
dbhost = config['database']['host']
dbport = config['database']['port']
dbuser = config['database']['user']
dbpassword = config['database']['password']
dbname = config['database']['name']

line1 = config['areas']['line 1']
line2 = config['areas']['line 2']

camerawidth = config['camera']['camerawidth']
cameraheight = config['camera']['cameraheight']


db = pymysql.connect(host= dbhost , port= dbport , user= dbuser, password= dbpassword, database=dbname, charset = "utf8", use_unicode = True)#パスワードは適宜変更
cursor=db.cursor()
db.commit()



cursor.execute("""CREATE TABLE IF NOT EXISTS unrecognized_newwtable(
            id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            camera_name VARCHAR(32),
            counted_datetime DATETIME,
            counted_date DATE,
            label_name VARCHAR(32),
            object_name VARCHAR(32),
            direction VARCHAR(32),
            x INT,
            y INT,
            w INT,
            h INT,
            confidence FLOAT 
            );""")
db.commit()
cursor.execute("""CREATE TABLE IF NOT EXISTS truckline_newwtable(
            id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            camera_name VARCHAR(32),
            counted_datetime DATETIME,
            counted_date DATE,
            label_name VARCHAR(32),
            object_name VARCHAR(32),
            direction VARCHAR(32),
            x INT,
            y INT,
            w INT,
            h INT,
            confidence FLOAT 
            );""")
db.commit()
cursor.execute("""CREATE TABLE IF NOT EXISTS mixer_newwtable(
            id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            camera_name VARCHAR(32),
            counted_datetime DATETIME,
            counted_date DATE,
            label_name VARCHAR(32),
            object_name VARCHAR(32),
            direction VARCHAR(32),
            x INT,
            y INT,
            w INT,
            h INT,
            confidence FLOAT 
            );""")
db.commit()
cursor.execute("""CREATE TABLE IF NOT EXISTS others_newwtable(
            id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            camera_name VARCHAR(32),
            counted_datetime DATETIME,
            counted_date DATE,
            label_name VARCHAR(32),
            object_name VARCHAR(32),
            direction VARCHAR(32),
            x INT,
            y INT,
            w INT,
            h INT,
            confidence FLOAT 
            );""")
db.commit()
#--------------



def dearea(jsonname):
    f = open(jsonname,'r')
    json_obj = json.load(f)
    return (json_obj["direction"],json_obj["label_name"],json_obj["x"],json_obj["y"],json_obj["w"],json_obj["h"])
     
def datebase(jsonname,savename,vehicleType,table):
    f = open(jsonname,'r')
    json_obj = json.load(f)
    vehicleTypeNameDict = {
        0 : "alumivan",
        1 : "mixer",
        2 : "dump",
        3 : "normalcar",
        4 : "other",
        5 : "pump",
        6 : "roughterraincrane",
        7 : "unic",
        8 : "paracement"
    }
    data = {
        "camera_name":f'{json_obj["camera_name"]}',
        "counted_datetime":f'{json_obj["counted_at"]}',
        "counted_date":f'{json_obj["counted_date"]}',
        "label_name":f'{json_obj["label_name"]}',
        "object_name":vehicleTypeNameDict.get(vehicleType, "ErrorType"),
        "direction":f'{json_obj["direction"]}',
        "x":json_obj["x"],
        "y":json_obj["y"],
        "w":json_obj["w"],
        "h":json_obj["h"],
        "confidence":json_obj["confidence"]
    }
    try:
        if table == 0:
            json_post_analysis = f"./done/passedLineTruck/log_{savename}.json"
            with open(json_post_analysis, mode='wt', encoding='utf-8') as afile:
                json.dump(data, afile, ensure_ascii=False, indent=2)
            cursor.execute("INSERT INTO truckline_newwtable (camera_name,counted_datetime,counted_date,label_name,object_name,direction,x,y,w,h,confidence) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);", 
                           (json_obj["camera_name"],json_obj["counted_at"],json_obj["counted_date"],json_obj["label_name"],vehicleTypeNameDict.get(vehicleType,                                                       "ErrorType"),json_obj["direction"],json_obj["x"],json_obj["y"],json_obj["w"],json_obj["h"],json_obj["confidence"]))

        elif table == 1:
            json_post_analysis = f"./done/passedAreaMixer/log_{savename}.json"
            with open(json_post_analysis, mode='wt', encoding='utf-8') as afile:
                json.dump(data, afile, ensure_ascii=False, indent=2)        
            cursor.execute("INSERT INTO mixer_newwtable (camera_name,counted_datetime,counted_date,label_name,object_name,direction,x,y,w,h,confidence) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);", 
                           (json_obj["camera_name"],json_obj["counted_at"],json_obj["counted_date"],json_obj["label_name"],vehicleTypeNameDict.get(vehicleType,                                                       "ErrorType"),json_obj["direction"],json_obj["x"],json_obj["y"],json_obj["w"],json_obj["h"],json_obj["confidence"]))

        elif table == 2:
            json_post_analysis = f"./done/passedAreaOthers/log_{savename}.json"
            with open(json_post_analysis, mode='wt', encoding='utf-8') as afile:
                json.dump(data, afile, ensure_ascii=False, indent=2)            
            cursor.execute("INSERT INTO others_newwtable (camera_name,counted_datetime,counted_date,label_name,object_name,direction,x,y,w,h,confidence) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);", 
                           (json_obj["camera_name"],json_obj["counted_at"],json_obj["counted_date"],json_obj["label_name"],vehicleTypeNameDict.get(vehicleType,                                                       "ErrorType"),json_obj["direction"],json_obj["x"],json_obj["y"],json_obj["w"],json_obj["h"],json_obj["confidence"]))
        db.commit()
    except Exception as error:
        db.ping(True)

def unrecognized_datebase(json_file):
    p = open(json_file,'r')
    json_obj = json.load(p)
    data = {
        "camera_name":f'{json_obj["camera_name"]}',
        "counted_datetime":f'{json_obj["counted_at"]}',
        "counted_date":f'{json_obj["counted_date"]}',
        "label_name":f'{json_obj["label_name"]}',
        "object_name":f'{json_obj["object_name"]}',
        "direction":f'{json_obj["direction"]}',
        "x":json_obj["x"],
        "y":json_obj["y"],
        "w":json_obj["w"],
        "h":json_obj["h"],
        "confidence":json_obj["confidence"]
    }
    try:
        cursor.execute("INSERT INTO unrecognized_newwtable (camera_name,counted_datetime,counted_date,label_name,object_name,direction,x,y,w,h,confidence) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);", (json_obj["camera_name"],json_obj["counted_at"],json_obj["counted_date"],json_obj["label_name"],json_obj["object_name"],json_obj["direction"],json_obj["x"],json_obj["y"],json_obj["w"],json_obj["h"],json_obj["confidence"]))
        db.commit()
    except Exception as erro:
        db.ping(True)