from sqlalchemy import create_engine
from sklearn.preprocessing import MinMaxScaler
import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
import pymysql
import json
from disaggregation import labeling

# MySQL Setting
DB_host = "localhost"
DB_username = "root"
DB_password = "123456"
DB_database = "power-socket-5"

# Clustering parameters
global eps, MinPts, frame
frame = 200  # number of data samples considered one time
eps = 0.5
MinPts = int(frame / 20)
data_list = []


def recognize(dataList):
    # New Data Input
    np_new = np.array(dataList)

    db = pymysql.connect(DB_host, DB_username, DB_password, DB_database)
    cursor = db.cursor()
    engine = create_engine(
        'mysql+pymysql://root:123456@localhost:3306/power-socket-5')

    # 從record取出前一批資料的結果#######################################################
    sql_db = "select * from record order by id desc limit 1"
    df_db = pd.read_sql_query(sql_db, engine)
    df_db = df_db.loc[:, ['centroid_I', 'centroid_S',
                        'centroid_Q', 'radius', 'doubleCheck']]
    np_db = df_db.values

    if len(np_db) != 0:  # 若record非空(有前一筆資料)
        # 計算與前一批資料的距離#############################################################
        pts = 0
        for i in range(0, len(np_new), 1):
            dist = np.linalg.norm(np_new[i, [1, 4, 5]] - np_db[0, 0:3])
            if dist <= np_db[0, 3]:
                pts = pts + 1
        print("\n落在前一群的資料數量: ", pts)
        if pts >= frame * 0.2:  # 一定數量的點落在前一群裡 目前是*0.2
            ischange = 0  # 負載未發生改變
            doubleCheck = 1
            check_again = 1
        else:
            ischange = 1
            doubleCheck = 0
            check_again = 0
    else:  # record為空(表示沒有前一筆資料):
        ischange = 1
        doubleCheck = 0
        check_again = 0


    # 從record取出前前一批資料的結果#######################################################
    sql_db_two = "select * from record order by id desc limit 1,1"
    df_db_two = pd.read_sql_query(sql_db_two, engine)
    df_db_two = df_db_two.loc[:, ['centroid_I', 'centroid_S',
                        'centroid_Q', 'radius', 'doubleCheck']]
    np_db_two = df_db_two.values

    changed = 0
    if check_again == 1:
        # 從record取出前前一批資料的結果#######################################################
        sql_db_two = "select * from record order by id desc limit 1,1"
        df_db_two = pd.read_sql_query(sql_db_two, engine)
        df_db_two = df_db_two.loc[:, ['centroid_I', 'centroid_S',
                            'centroid_Q', 'radius', 'doubleCheck']]
        np_db_two = df_db_two.values

        if len(np_db_two) != 0:  # 若record非空(有前前一筆資料)
            # 計算與前一批資料的距離#############################################################
            pts_two = 0
            for j in range(0, len(np_new), 1):
                dist = np.linalg.norm(np_new[j, [1, 4, 5]] - np_db_two[0, 0:3])
                if dist <= np_db_two[0, 3]:
                    pts_two = pts_two + 1
            print("\n落在前前一群的資料數量: ", pts_two)
            if pts_two >= frame * 0.2:  # 一定數量的點落在前一群裡 目前是*0.2
                ischange = 0  # 負載未發生改變
                doubleCheck = 1
            else:
                ischange = 1
                doubleCheck = 0
                changed = 1

        else:  # record為空(表示沒有前一筆資料):
            ischange = 1
            doubleCheck = 0

    # 計算此批資料的中心及半徑############################################################
    # Calculate the centroid and radius
    # Normalization
    # norm_new = MinMaxScaler(feature_range=(
    #     0, 1)).fit_transform(np_new[:, [1, 4, 5]])

    # dbscan_model = DBSCAN(eps=eps, min_samples=MinPts).fit(norm_new)
    # # core點位置設true，非core點設false
    # core_samples_mask = np.zeros_like(dbscan_model.labels_, dtype=bool)
    # core_samples_mask[dbscan_model.core_sample_indices_] = True
    # dbscan_labels = dbscan_model.labels_  # DBSCAN後，點的屬群

    # # 得一ndarray, 為該群(cluster 0)則元素設true, 否則false
    # class_member_mask = (dbscan_labels == 0)

    # # core point
    # core_new = np_new[class_member_mask &
    #                   core_samples_mask]  # 屬於該群又是core point

    # if len(core_new) == 0:
    #     core_new = np_new

    # centroid = np.mean(core_new, axis=0)
    # radius = 0
    # for i in range(0, len(core_new), 1):
    #     dist = np.linalg.norm(
    #         core_new[i, [1, 4, 5]] - centroid[[1, 4, 5]])
    #     radius = radius + dist
    # radius = radius / len(core_new)
    # print("\n\n # of core point: ", len(core_new))

    centroid = np.mean(np_new, axis=0)
    radius = 0
    for i in range(0, len(np_new), 1):
        dist = np.linalg.norm(
            np_new[i, [1, 4, 5]] - centroid[[1, 4, 5]])
        radius = radius + dist
    radius = radius / len(np_new)

    # np_db[0, 4]和ischange會有幾種可能
    # 0 1 表示前次改變這次又變，故判定為負載不穩或有noise，繼續觀察
    # 0 0 表示前次改變這次沒變(穩了下來)，故判定為負載改變了，會進到下面的if statement
    # 1 0 表示前次沒變，這次也沒變，故判定為負載穩定中
    if len(np_db) != 0:
        print("\n\n### check1 ###", pts, np_db[0, 4], ischange)

    if (ischange == 0 and np_db[0, 4] == 0) or changed == 1:  # 確定負載發生改變了
        doubleCheck = 1
        # run disaggregate
        # 從record，把前前一批的記錄(即變化前的)取出來#############################

        sql_init = "select * from record order by id desc limit 2,1"  # 前前
        df_init = pd.read_sql_query(sql_init, engine)
        df_init = df_init.loc[:, ['centroid_I', 'centroid_S',
                                  'centroid_Q']]
        np_init = df_init.values

        sql_init_two = "select * from record order by id desc limit 3,1"  # 前前前
        df_init_two = pd.read_sql_query(sql_init_two, engine)
        df_init_two = df_init_two.loc[:, ['centroid_I', 'centroid_S',
                                  'centroid_Q']]
        np_init_two = df_init_two.values

        # call disaggregation function#########################################
        if len(np_init) != 0:
            if len(np_init_two) !=0:
                diff = np_new[:, [1, 4, 5]] - np_init_two[0, :]
            else:
                diff = np_new[:, [1, 4, 5]] - np_init[0, :]
        else:
            diff = np_new[:, [1, 4, 5]]

        # print("\ndiff: ", diff)
        belong, incr, centroid_diff = labeling(diff, engine)  # 把'差'拿去辨識、拆解
        # incr為1表示附載是上升的(turn on)，0表示下降的(turn off)
        # belong長度0表示新電器 || >=0表示組合



        print("belong: ", belong, "centroid_diff: ", centroid_diff)
        # 將結果更新到my_device表################################################
        if len(belong) == 0:  # 新電器
            print("\n=============================新電器 ============================")
            # the max applianceID in DB
            sql_max = "select max(ApplianceID) from my_device"
            df_max = pd.read_sql_query(sql_max, engine)
            if not df_max.values:  # DB is empty
                appID = 1
                status_on = 1
            else:
                appID = int(df_max.values[0, 0]) + 1
                status_on = incr
            sql_new = "insert into my_device (ApplianceID, StartTime, Name, centroid_I, centroid_P, centroid_S, centroid_Q, radius, status) values ({}, now(), '{}', {}, {}, {}, {}, {}, {})".format(
                appID, "Appliance" + str(appID), abs(centroid_diff[0]), np.sqrt(pow(abs(centroid_diff[1]), 2) + pow(abs(centroid_diff[2]), 2)), abs(centroid_diff[1]), abs(centroid_diff[2]), radius, 1)

            cursor.execute(sql_new)
            db.commit()
            sql_change = "insert into changes(RecordTime, result, Name, Current, Apparent_Power, Reactive_Power, Status) values(now(), {}, '{}', {}, {}, {}, {})".format(
                appID, "Appliance" + str(appID), centroid_diff[0], centroid_diff[1], centroid_diff[2], status_on)
            cursor.execute(sql_change)
            db.commit()

        elif len(belong) == 1:  # 既有單一電器
            sql_update = "UPDATE my_device SET status={}, StartTime=now() WHERE ApplianceID={}".format(
                incr, belong[0])
            cursor.execute(sql_update)
            db.commit()
            sql_name = "select Name from my_device where ApplianceID = {}".format(
                belong[0])
            df_name = pd.read_sql_query(sql_name, engine)
            print("\n=======================既有單一電器選項  各個變數如下: ========================")
            print(belong[0])
            print(df_name.values[0, 0])
            print(centroid_diff)
            sql_change = "insert into changes(RecordTime, result, Name, Current, Apparent_Power, Reactive_Power, Status) values(now(), {}, '{}', {}, {}, {}, {})".format(
                belong[0], df_name.values[0, 0], abs(centroid_diff[0]), abs(centroid_diff[1]), abs(centroid_diff[2]), incr)
            cursor.execute(sql_change)
            db.commit()

        else:  # 多電器
            remove = []
            if len(belong) > 1 and incr == 0: # 多電器拔除 避免大吃小的應變
                    belong_left, incr_left, centroid_diff_left = labeling(np_new[:, [1, 4, 5]], engine)  # 把現在的狀況拿去做辨識
                    if len(belong_left) == 0:
                        pass
                    elif len(belong_left) == 1:
                        remove.append(belong)
                    else:
                        remove - belong_left
            print("剩餘值辨識: ", belong_left, remove)

            print("\n=================進到多電器選項 各個變數如下: ===================")
            for k in range(0, len(belong), 1):
                if k not in remove:
                    sql_update = "UPDATE my_device SET status={}, StartTime=now() WHERE ApplianceID={}".format(
                        incr,belong[k])
                    cursor.execute(sql_update)
                    db.commit()
                    sql_name = "select Name from my_device where ApplianceID = {}".format(
                        belong[k])
                    df_name = pd.read_sql_query(sql_name, engine)
                    # #### 下面的sql指令有時候會錯 應該是df_name.values有時是一維的 故[0]
                    print(belong[k])
                    print(df_name.values[0, 0])
                    print(centroid_diff)
                    sql_change = "insert into changes(RecordTime, result, Name, Current, Apparent_Power, Reactive_Power, Status) values(now(), {}, '{}', {}, {}, {}, {})".format(
                        belong[k], df_name.values[0, 0], centroid_diff[0], centroid_diff[1], centroid_diff[2], incr)
                    cursor.execute(sql_change)
                    db.commit()


    # 無論是否有發生變化，將記錄存於record表######################################
    # Store to db for recording
    sql_on = "select ApplianceID from my_device where status = 1"
    cursor.execute(sql_on)
    db.commit()
    str_app = ""
    onon = cursor.fetchall()
    for item in onon:
        str_app = str_app + ',' + str(item[0])

    str_app = str_app[1:]

    # 單一電器的情況則不停調整電器的數值=============================================================

    # 挑出noise的數值
    sql_noise = "select centroid_I, centroid_P, centroid_S, centroid_Q, radius from my_device where ApplianceID = 1"
    df_noise = pd.read_sql_query(sql_noise, engine)
    np_noise = df_noise.values

    if len(np_noise) != 0 and len(onon) <= 2:

        sql_on = "select radius, ApplianceID , centroid_I, centroid_P, centroid_S, centroid_Q from my_device where status = 1"
        df_on = pd.read_sql_query(sql_on, engine)
        np_on = df_on.values

        print("centroid: ", centroid)
        # 單一電器的情況則不停調整電器的數值
        if ischange == 0 and len(onon) == 2:
            ori_radius = np_on[1, 0]
            if radius > ori_radius:
                ori_radius = radius
            sql_update_app = "update my_device set centroid_I={}, centroid_P={}, centroid_S={}, centroid_Q={}, radius={} where applianceID= {}".format(
                np_on[1, 2]*0.8 + (centroid[1] - float(np_noise[0, 0]))*0.2, np_on[1, 3]*0.8 + (centroid[3] - float(np_noise[0, 1]))*0.2, np_on[1, 4]*0.8 + (centroid[4] - float(np_noise[0, 2]))*0.2, np_on[1, 5]*0.8 + (centroid[5] - float(np_noise[0, 3]))*0.2, ori_radius, np_on[1, 1])
            cursor.execute(sql_update_app)
            db.commit()
            print("sql: ", sql_update_app)

        elif ischange == 0 and len(onon) == 1:

            sql_update_noise = "update my_device set centroid_I={}, centroid_P={}, centroid_S={}, centroid_Q={}, radius={} where applianceID= {}".format(
               float(np_noise[0, 0])*0.8+centroid[1]*0.2, float(np_noise[0, 1]*0.8+centroid[3])*0.2, float(np_noise[0, 2]*0.8+centroid[4])*0.2, float(np_noise[0, 3]*0.8+centroid[5])*0.2, radius*0.2+float(np_noise[0, 4])*0.8, np_on[0, 1])
            cursor.execute(sql_update_noise)
            db.commit()
            print("sql: ", sql_update_noise)

    # 將記錄存於record表=====================================================================
    if str_app:
        sql_record = "insert into record (RecordTime, centroid_V, centroid_I, centroid_PF, centroid_P, centroid_S, centroid_Q, radius, doubleCheck, on_device) values (now(),{}, {}, {}, {}, {}, {}, {}, {}, '{}')".format(
            centroid[0], centroid[1], centroid[2], centroid[3], centroid[4], centroid[5], radius, doubleCheck, str_app)

    else:
        sql_record = "insert into record (RecordTime, centroid_V, centroid_I, centroid_PF, centroid_P, centroid_S, centroid_Q, radius, doubleCheck, on_device) values (now(),{}, {}, {}, {}, {}, {}, {}, {}, {})".format(
            centroid[0], centroid[1], centroid[2], centroid[3], centroid[4], centroid[5], radius, doubleCheck, 1)

    cursor.execute(sql_record)
    db.commit()

    if len(np_db) != 0:
        # double check的數值問題
        if ischange == 0 and np_db[0, 4] == 0:
            sql_secondID = "select id from record order by id desc limit 1,1"
            df_secondID = pd.read_sql_query(sql_secondID, engine)
            np_secondID = df_secondID.values
            if len(np_secondID) != 0:
                sql_update_ondevice = "update record set on_device = '{}' where id = {}".format(
                    str_app, np_secondID[0, 0])

                cursor.execute(sql_update_ondevice)
                db.commit()

    db.close()
    print("Finish")
