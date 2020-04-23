import numpy as np
import pandas as pd
import pymysql
from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import DBSCAN
from itertools import combinations


def granularity(app_a, app_b, eps):
    # 從資料庫把兩電器的資料取出
    sql_select = "select * from individual_record where applianceID = {} "
    sql_db = "select * from my_device where ID != 1"  # 這裡where條件把背景noise去掉
    df_db = pd.read_sql_query(sql_db, engine)
    df_db = df_db.loc[:, [
        'ApplianceID', 'centroid_I', 'centroid_S', 'centroid_Q', 'radius']]
    np_db = df_db.values

    # 將兩電器資料標準化至0-1之間的特徵空間

    # 執行DBSCAN，看是否成功分出兩群
    # 是：將兩資料特別註記，讓下次偵測到該電器能成功辨識
    # 否: 調整半徑再試一次此function


def labeling(diff, engine):

    #===============================================================
    # 差值可能性有2: 符合某單一舊電器 || 不符合某單一舊電器
    # 不符合某單一電器原因有2: 為單一新電器 || 為多項電器組成, 則遞迴尋找
    #===============================================================

    # 從my_device，把既有電器取出來###################################
    sql_db = "select * from my_device where ID != 1"  # 這裡where條件把背景noise去掉
    df_db = pd.read_sql_query(sql_db, engine)
    df_db = df_db.loc[:, [
        'ApplianceID', 'centroid_I', 'centroid_S', 'centroid_Q', 'radius']]
    np_db = df_db.values

    # 計算 '差值' 與 '既有電器' 的距離#################################
    print("\n", "差值 | ", "既有 | ", "距離 | ", "半徑門檻")
    record = []
    for i in range(0, len(diff), 1):
        isNew = 1
        for j in range(0, len(np_db), 1):
            dist = np.linalg.norm(
                abs(abs(diff[i, 0:3])) - np_db[j, 1:4])
            print(diff[i, 0:3], np_db[j, 1:4], dist, np_db[j, 4])
            if dist <= np_db[j, 4]:
                record.append(int(np_db[j, 0]))
                isNew = 0

        if isNew == 1:
            record.append(0)

    print("record: ", set(record))

    # Statistics and Determination the cluster belong
    # 得到 belong, 0為未知, 非0為某既有的單一電器
    if len(np_db) == 0:  # new system no appliance in db
        belong = 0
    else:
        belong = 0
        if max(set(record), key=record.count) != 0:  # 最多的非0(非未知的意思)
            print("\n有max不是0,是", max(set(record)))
            belong = max(set(record), key=record.count)
        else:  # 以下這裡是為避免最多的是0, 但是也有一定數量的點屬於某電器的狀況
            frame = 200
            for k in set(record):
                print(k, ":", list(record).count(k))  # test
                if list(record).count(k) >= (frame * 0.05) and k != 0:
                    print("\n最多是0,但是有一定數量的", k)
                    belong = k
                    break

    result_list = []
    if belong != 0:
        result_list.append(belong)
        print("belong = ", belong)
        centroid_diff, radius = centroid_cal(diff)

    else:
        # belong=0, 可能為單一新電器
        # 或為多項電器組成, 則遞迴尋找
        result_list, centroid_diff = disaggregate(np_db, diff)
        print("belong = 0")

    if diff[0, 0] > 0:
        incr = 1
    else:
        incr = 0

    return result_list, incr, centroid_diff


def disaggregate(sets, sums):
    # subset sum problem
    # set: sets (np_db)
    # sum: sums (diff)

    # 計算diff的centroid及radius
    centroid, radius = centroid_cal(sums)

    # subset sum
    if sums[0, 0] > 0:
        increase = 1
    else:
        increase = 0
    current_set = set(sets[:, 1].flat)  # 電流
    result_set = subset_sum(current_set, centroid[0], increase)

    index_list = []
    for m in result_set:
        for n in range(0, len(sets), 1):
            if sets[n, 1] == m:
                index_list.append(sets[n, 0])

    return(index_list, centroid)


def subset_sum(items, goals, increase):
    if increase == 1:  # 負載是增加的話
        for length in range(2, len(items) + 1):
            for subset in combinations(items, length):
                if abs(sum(subset) - goals) <= 0.2:  # 差距小於0.2的話就算符合
                    return set(subset)
        return set()  # 回傳空set表示沒有找到結果

    else:  # 負載是減少的話 (絕對是既有的 一定有解)
        min_dist = 1000000
        close_set = set()
        for length in range(1, len(items) + 1):
            for subset in combinations(items, length):
                if abs(sum(subset) - abs(goals)) < min_dist:  # 找出最接近的
                    min_dist = abs(sum(subset) - abs(goals))
                    close_set = set(subset)
        return close_set


def centroid_cal(sums):
    # 用centroid代表群
    # Normalization
    frame = 200  # number of data samples considered one time
    eps = 0.5
    MinPts = int(frame / 20)
    norm = MinMaxScaler(feature_range=(
        0, 1)).fit_transform(abs(sums[:, :]))

    dbscan_model = DBSCAN(eps=eps, min_samples=MinPts).fit(norm)
    # core點位置設true，非core點設false
    core_samples_mask = np.zeros_like(dbscan_model.labels_, dtype=bool)
    core_samples_mask[dbscan_model.core_sample_indices_] = True
    dbscan_labels = dbscan_model.labels_  # DBSCAN後，點的屬群

    # 得一ndarray, 為該群(cluster 0)則元素設true, 否則false
    class_member_mask = (dbscan_labels == 0)

    # core point
    core_sums = sums[class_member_mask &
                     core_samples_mask]  # 屬於該群又是core point

    if len(core_sums) == 0:
        core_sums = sums
    centroid = np.mean(core_sums, axis=0)
    radius = 0
    for i in range(0, len(core_sums), 1):
        dist = np.linalg.norm(
            core_sums[i, :] - centroid[:])
        radius = radius + dist
    radius = radius / len(core_sums)

    return centroid, radius
