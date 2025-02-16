import os
import time
import xml.etree.ElementTree as ET
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# 设置 InfluxDB 连接
bucket = "o1_performance"
org = "influxdata"
token = "bzgBwlyDG8ZWBo0LP2hpbJ48I9zhZtMR"
url = "http://192.168.0.39:30001"
client = InfluxDBClient(url=url, token=token)
write_api = client.write_api(write_options=SYNCHRONOUS)

# 检查文件是否稳定
def is_file_stable(file_path, checks=3, interval=1.0):
    """检查文件是否稳定（文件大小多次一致）"""
    size = os.path.getsize(file_path)
    for _ in range(checks):
        time.sleep(interval)
        new_size = os.path.getsize(file_path)
        if new_size != size:
            size = new_size
        else:
            continue
    return new_size == size

# 延迟解析以确保文件完成写入
def delayed_parse(file_path, delay=2.0):
    time.sleep(delay)
    return os.path.exists(file_path) and os.path.getsize(file_path) > 0

# 解析 XML 文件并写入 InfluxDB（CU 和 DU 通用）
def process_xml(file_path, data_type):
    try:
        # 延迟检查并确保文件稳定
        if not delayed_parse(file_path, delay=3.0) or not is_file_stable(file_path):
            print(f"Error: File is not stable for parsing: {file_path}")
            return

        tree = ET.parse(file_path)
        root = tree.getroot()

        # 创建 measType 字典
        meas_types = {}
        for measType in root.findall('.//{http://www.3gpp.org/ftp/specs/archive/28_series/28.532#measData}measType'):
            p_value = measType.attrib.get('p')
            meas_types[p_value] = measType.text

        # 遍历 measValue 标签
        for measValue in root.findall('.//{http://www.3gpp.org/ftp/specs/archive/28_series/28.532#measData}measValue'):
            meas_obj = measValue.attrib.get('measObjLdn')

            # 获取指标数据
            fields = {}
            for r in measValue.findall('.//{http://www.3gpp.org/ftp/specs/archive/28_series/28.532#measData}r'):
                p_value = r.attrib.get('p')
                meas_name = meas_types.get(p_value, f"metric_{p_value}")
                try:
                    fields[meas_name] = float(r.text)
                except (ValueError, TypeError):
                    print(f"Invalid value for {meas_name}: {r.text}")
                    continue

            # 打印 fields 确认其内容是字典
            print(f"Parsed fields for {meas_obj} ({data_type}): {fields}")

            # 批量写入数据
            points = [
                Point(data_type).tag("measObjLdn", meas_obj).field(meas_name, value)
                for meas_name, value in fields.items()
            ]
            write_api.write(bucket=bucket, org=org, record=points)

    except ET.ParseError:
        print(f"Error: Unable to parse the {data_type} XML file: {file_path}")

# 监控新文件的创建
class NewFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith(".xml"):
            print(f"New XML file detected: {event.src_path}")

            # 根据路径选择处理函数
            if "/CU/" in event.src_path:
                process_xml(event.src_path, "CU01001")
            elif "/DU/" in event.src_path:
                process_xml(event.src_path, "DU01001")

# 监控 CU 和 DU 文件夹
def monitor_directories():
    event_handler = NewFileHandler()
    observer = Observer()

    # 监控 CU 文件夹
    observer.schedule(event_handler, path='/home/sftpuser/CU', recursive=False)

    # 监控 DU 文件夹
    observer.schedule(event_handler, path='/home/sftpuser/DU', recursive=False)

    observer.start()
    try:
        while True:
            time.sleep(1)  # 降低 CPU 占用
    except KeyboardInterrupt:
        observer.stop()
    finally:
        observer.join()

# 启动监控
monitor_directories()
