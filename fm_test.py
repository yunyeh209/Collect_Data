import json
import requests
import paho.mqtt.client as mqtt
from datetime import datetime,timedelta
import random
import string
import pymysql

# 获取当前时间的时间戳（精确到微秒）
timestamp = datetime.utcnow().timestamp()
time_in_s = int(timestamp)
time_ms = int((timestamp - time_in_s) * 1000000)
event_time = datetime.utcfromtimestamp(time_in_s).strftime('%Y-%m-%dT%H:%M:%S') + f'.{time_ms:06d}Z'

collection_end_time = int(timestamp - (timestamp % 900))
collection_start_time = collection_end_time - 900
collection_start_time_micros = collection_start_time * 1000000
collection_end_time_micros = collection_end_time * 1000000
interval_start_time = datetime.utcfromtimestamp(collection_start_time).strftime('%a, %d %b %Y %H:%M:%S GMT')
interval_end_time = datetime.utcfromtimestamp(collection_end_time).strftime('%a, %d %b %Y %H:%M:%S GMT')

def generate_event_id(prefix='fault_', length=10):
    random_string = ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
    return prefix + random_string

event_id = generate_event_id()

# Fault ID to Device Type and Status Mapping
fault_to_device_mapping = {
    5: ('RU', 3),
    6: ('RU', 3),
    7: ('RU', 3),
    8: ('RU', 3),
    12: ('RU', 3),
    13: ('RU', 3),
    14: ('RU', 3),
    16: ('CU', 3),
    17: ('DU', 3),
    18: ('RU', 3),
    41: ('CU', 3),
    44: ('CU', 3),
    49: ('CU', 3),
    52: ('DU', 3),
    55: ('DU', 3),
    60: ('DU', 3),
    186: ('RU', 3),
    193: ('RU_DU', 3),
    194: ('RU_DU', 3),
    195: ('RU_DU', 3),
    196: ('DU_CU', 3),
    197: ('DU', 3),
    198: ('DU', 3),
    199: ('CU', 3),
}

# Update MySQL Device Status Based on Fault ID
def update_device_status(fault_id, device_type, new_status, is_cleared):
    db_connection = pymysql.connect(
        host='192.168.0.39',
        user='root',
        password='ubuntu',
        database='devicelist'
    )

    try:
        with db_connection.cursor() as cursor:
            if is_cleared == "false":
                message = f"Fault detected, Fault ID: {fault_id}"
                new_status = 3
            else:
                message = "The device is healthy"
                new_status = 1

            if device_type == 'RU_DU':
                cursor.execute(
                    "UPDATE devicelist SET status = %s, message = %s WHERE devicename LIKE 'CU%%'",
                    (new_status, message)
                )
                cursor.execute(
                    "UPDATE devicelist SET status = %s, message = %s WHERE devicename LIKE 'CU%%'",
                    (new_status, message)
                )
            elif device_type == 'DU_CU':
                cursor.execute(
                    "UPDATE devicelist SET status = %s, message = %s WHERE devicename LIKE 'CU%%'",
                    (new_status, message)
                )
                cursor.execute(
                    "UPDATE devicelist SET status = %s, message = %s WHERE devicename LIKE 'CU%%'",
                    (new_status, message)
                )
            else:
                cursor.execute(
                    "UPDATE devicelist SET status = %s, message = %s WHERE devicename LIKE %s",
                    (new_status, message, f'{device_type}%')
                )

        db_connection.commit()
    except Exception as e:
        print("Error occurred while updating the database:", e)
    finally:
        db_connection.close()

# Generate JSON for Sending
def generate_json(mqtt_data):
    event_id = generate_event_id()

    a = mqtt_data['notification']['alarm-notif']['is-cleared']
    if a == "false":
        a = "Idle"
    elif a == "true":
        a = "Active"

    template = {
        "event": {
            "commonEventHeader": {
                "domain": "fault",
                "eventId": event_id,
                "eventName": mqtt_data['notification']['alarm-notif']['fault-text'],
                "eventType": "oam_Alarms",
                "sequence": 0,
                "priority": "High",
                "reportingEntityId": "",
                "reportingEntityName": mqtt_data['notification']['alarm-notif']['ran-id'],
                "sourceId": "",
                "sourceName": mqtt_data['notification']['alarm-notif']['ran-id'],
                "startEpochMicrosec": collection_start_time_micros,
                "lastEpochMicrosec": collection_end_time_micros,
                "nfNamingCode": "oam",
                "nfVendorName": "greigns",
                "timeZoneOffset": "+00:00",
                "version": "4.1",
                "vesEventListenerVersion": "7.2.1"
            },
            "faultFields": {
                "faultFieldsVersion": "4.0",
                "eventSeverity": mqtt_data['notification']['alarm-notif']["fault-severity"],
                "eventSourceType": "fault",
                "eventCategory": "network",
                "alarmCondition": mqtt_data['notification']['alarm-notif']["fault-id"],
                "specificProblem": mqtt_data['notification']['alarm-notif']["fault-text"],
                "vfStatus": a,
                "alarmInterfaceA": "N/A"
            }
        }
    }

    print("Generated JSON template:", template)
    return template

# POST设备数据到上层API
def post_fault_data(device_type, DeviceId, alarm_data):
    base_url = "http://192.168.0.40/api/v1/ORAN/O1Fault"
    server_id = 10001
    url = f"{base_url}?serverid={server_id}"

    headers = {'Content-Type': 'application/json'}

    formatted_data = {
        "DeviceId": DeviceId,
        "DeviceType": device_type,
        "AlarmId": alarm_data.get("AlarmId", 0),
        "EventTime": alarm_data.get("EventTime", ""),
        "EventSeverity": alarm_data.get("EventSeverity", ""),
        "SystemDN": alarm_data.get("SystemDN", ""),
        "AlarmType": "oam_Alarms",
        "ProbableCause": alarm_data.get("ProbableCause", ""),
        "IsCleared": alarm_data.get("IsCleared", "")
    }

    print(formatted_data)

    response = requests.post(url, headers=headers, json=formatted_data)
    if response.status_code == 200:
        print(f"Success: {response.json()}")
    else:
        print(f"Error: {response.status_code}, {response.text}")

# 根据 fault_id 处理不同设备类型的告警
def handle_fault(fault_id, is_cleared, mqtt_data):
    if fault_id in fault_to_device_mapping:
        device_type, _ = fault_to_device_mapping[fault_id]

        # 设置告警事件数据
        alarm_data = {
            "AlarmId": fault_id,
            "EventTime": (datetime.now() + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S"),
            "EventSeverity": mqtt_data['notification']['alarm-notif']["fault-severity"],
            "SystemDN": "gregins",
            "ProbableCause": mqtt_data['notification']['alarm-notif']["fault-text"],
            "IsCleared": "Idle" if is_cleared == "false" else "Active"
        }

        # 根据设备类型设置 DeviceId 并发送不同的数据包
        if device_type == "RU":
            post_fault_data("RU01001", 2001, alarm_data)
        elif device_type == "DU":
            post_fault_data("DU01001", 3001, alarm_data)
        elif device_type == "CU":
            post_fault_data("CU01001", 4001, alarm_data)
        elif device_type == "RU_DU":
            post_fault_data("RU01001", 2001, alarm_data)  # RU设备数据包
            post_fault_data("DU01001", 3001, alarm_data)  # DU设备数据包
        elif device_type == "DU_CU":
            post_fault_data("DU01001", 3001, alarm_data)  # DU设备数据包
            post_fault_data("CU01001", 4001, alarm_data)  # CU设备数据包
        else:
            print(f"Unexpected device_type: {device_type} for fault_id: {fault_id}")

    else:
        print(f"Fault ID {fault_id} not found in mapping.")

# MQTT 消息回调函数
def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode("utf-8")
        print("Received message:", payload_str)

        mqtt_data = json.loads(payload_str)
        fault_id = int(mqtt_data['notification']['alarm-notif']["fault-id"])
        is_cleared = mqtt_data['notification']['alarm-notif']['is-cleared']

        if fault_id in fault_to_device_mapping:
            # 更新数据库状态
            device_type, _ = fault_to_device_mapping[fault_id]
            if is_cleared == "false":
                update_device_status(fault_id, device_type, 3, is_cleared)
            elif is_cleared == "true":
                update_device_status(fault_id, device_type, 1, is_cleared)

            # 处理告警并转发到上层装置
            handle_fault(fault_id, is_cleared, mqtt_data)

    except Exception as e:
        print('Error occurred while processing message:', e)

# 设置 MQTT 客户端
client = mqtt.Client(protocol=mqtt.MQTTv311)
client.on_message = on_message

# 连接到 MQTT 代理
broker_address = '192.168.135.102'
broker_port = 1883
client.connect(broker_address, broker_port)

# 订阅主题
client.subscribe('netconf-proxy/oran-o1/fm')

# 开始循环
client.loop_forever()
