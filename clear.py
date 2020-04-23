# 清空 my_device / record / changes 三個資料表
import pymysql

# MySQL Setting
DB_host = "localhost"
DB_username = "root"
DB_password = "123456"
DB_database = "power-socket-5"


db = pymysql.connect(DB_host, DB_username, DB_password, DB_database)
cursor = db.cursor()


sql_clear = "TRUNCATE `my_device`;TRUNCATE `record`;TRUNCATE `powersocketinfo`;TRUNCATE `changes`;"
cursor.execute(sql_clear)
db.commit()
db.close
