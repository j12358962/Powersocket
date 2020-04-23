import pymysql
import os
import json
import datetime
import logging
import paho.mqtt.client as mqtt
from recognition import recognize
from dotenv import load_dotenv
from pathlib import Path  # python3 only
env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)
# MySQL Setting
DB_local = os.getenv("DB_HOST")
DB_username = os.getenv("DB_USER")
DB_password = os.getenv("DB_PASS")
DB_database = os.getenv("DB_NAME")
print(DB_local)
# MQTT Settings
MQTT_Broker = "140.116.39.212"
MQTT_Port = 1883
# Keep_Alive_Interval = 60
Keep_Alive_Interval = 60 * 60 * 3
MQTT_Topic = "PowerSocket2"

# Clustering parameters
global frame
frame = 200  # number of data samples considered one time
data_accumulation = 0
data_list = []


# Subscribe power data
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    # mqttc.subscribe(MQTT_Topic, 0)
    mqttc.subscribe("PowerSocket2", 0)

# Save Data into DB Table


def on_message(client, userdata, msg):
    # print("Data: " + str(msg.payload))
    # print("Topic: " + msg.topic + "\n" +
    #       "Message: " + msg.payload.decode("utf-8"))
    Save_power_Data(msg.payload)  # save raw data samples
    global data_accumulation, data_list
    data_accumulation = data_accumulation + 1
    if data_accumulation >= frame:
        recognize(data_list)  # run recognition function
        data_accumulation = 0
        data_list = []
    else:
        # 有時候下面這行 json_data = json.loads(msg.payload)  會出錯, 錯誤訊息如下
        # raise JSONDecodeError("Expecting value", s, err.value) from None
        # json.decoder.JSONDecodeError: Expecting value: line 1 column 54 (char 53)
        try:
            json_data = json.loads(msg.payload)
            data_list.append([json_data['V'], json_data['A'], json_data['PF'],
                              json_data['W'],  json_data['VA'], json_data['VAR']])
        except Exception as e:
            pass


def on_subscribe(mosq, obj, mid, granted_qos):
    pass


# Function to save power information to DB Table
def Save_power_Data(jsonData):
    db4 = pymysql.connect(DB_local, DB_username, DB_password, DB_database)
    # print("DB connect susscessful")
    try:
        # Parse Data
        json_Dict = json.loads(jsonData)
        Status = json_Dict['S']
        Voltage = json_Dict['V']
        Current = json_Dict['A']
        PowerFactor = json_Dict['PF']
        ActivePower = json_Dict['W']
        ApparentPower = json_Dict['VA']
        ReactivePower = json_Dict['VAR']

        DataTime = datetime.datetime.now().strftime(
            '%Y-%m-%d %H:%M:%S.%f')[:-3]

        # Push into DB Table
        cursor = db4.cursor()
        sql = ("INSERT INTO powersocketinfo(Data_Time, Voltage, Current, \n"
               "	         Power_Factor,Active_Power, Apparent_Power, Reactive_Power, ApplianceID)\n"
               "         	VALUES (%s,%s,%s,%s,%s,%s,%s, 7281)")
        sqldata = (DataTime, Voltage, Current, PowerFactor,
                   ActivePower, ApparentPower, ReactivePower)
    except:
        pass

    try:
        cursor.execute(sql, sqldata)
        db4.commit()
        print("insert Data susscessful.")

    except Exception as e:
        db4.rollback()
        logging.exception(e)
        print("insert Data fail.")
    db4.close()


# print("start")
mqttc = mqtt.Client()
# Assign event callbacks
mqttc.on_connect = on_connect
mqttc.on_message = on_message
# mqttc.on_subscribe = on_subscribe

# Connect
mqttc.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval))

# Continue the network loop
mqttc.loop_forever()
