# 切繼電器
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
import pandas as pd

# Broker資訊
HOST = "140.116.39.212"
PORT = 1883
TOPIC = "ControlPowerSocket2"
data =  0 # 1 is turn on




# 當Client接收到來自Broker的確認連線請求時，發佈主題及訊息，並結束連線
def on_connect(client, userdata, flags, rc):
    # 發佈 (Topic, Payload, QoS, BrokerIP)
    publish.single(TOPIC, data, qos=2, hostname=HOST)
    client.disconnect()


def start():
    client = mqtt.Client()  # 建立Client個體
    client.on_connect = on_connect
    client.connect(HOST, PORT, 30)  # 連接至broker (IP, Port, Timeout)
    client.loop_forever()  # 持續連線


if __name__ == '__main__':
    start()
