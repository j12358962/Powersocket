#! python3
# -*- coding: utf-8 -*-

import json
import pymysql
import datetime
import logging
import paho.mqtt.client as mqtt

# MySQL Setting
# 資料庫帳密等 要打自己的
DB_local = "localhost"
DB_username = "root"
DB_password = "123456"
DB_database = "power-socket-sql2"

# MQTT Settings
MQTT_Broker = "140.116.39.212"
MQTT_Port = 1883
Keep_Alive_Interval = 60
MQTT_Topic = "PowerSocket2"


# Subscribe power data
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    mqttc.subscribe(MQTT_Topic, 0)


# Save Data into DB Table
def on_message(client, userdata, msg):
    # This is the Master Call for saving MQTT Data into DB
    # print ("MQTT Data Received...")
    # print ("MQTT Topic: " + msg.topic)
    print("Data: " + str(msg.payload))
    Save_power_Data(msg.payload)


def on_subscribe(mosq, obj, mid, granted_qos):
    pass


# Function to save power information to DB Table
def Save_power_Data(jsonData):
    db = pymysql.connect(DB_local, DB_username, DB_password, DB_database)

    # Parse Data
    json_Dict = json.loads(jsonData)
    Status = json_Dict['S']
    Voltage = json_Dict['V']
    Current = json_Dict['A']
    PowerFactor = json_Dict['PF']
    ActivePower = json_Dict['W']
    ApparentPower = json_Dict['VA']
    ReactivePower = json_Dict['VAR']

    DataTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    # Push into DB Table
    cursor = db.cursor()
    sql = ("INSERT INTO powerSocketInfo(Data_Time, Status, Voltage, Current, \n"
           "	         Power_Factor,Active_Power, Apparent_Power, Reactive_Power)\n"
           "         	VALUES (%s,%s,%s,%s,%s,%s,%s,%s)")
    sqldata = (DataTime, Status, Voltage, Current, PowerFactor,
               ActivePower, ApparentPower, ReactivePower)

    try:
        cursor.execute(sql, sqldata)
        db.commit()
        print("Insert Data done.")

    except Exception as e:
        db.rollback()
        logging.exception(e)
        print("Insert Data fail.")
    db.close()


mqttc = mqtt.Client()

# Assign event callbacks
mqttc.on_connect = on_connect
mqttc.on_message = on_message
mqttc.on_subscribe = on_subscribe

# Connect
mqttc.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval))

# Continue the network loop
mqttc.loop_forever()
