import pandas as pd
import json
from datetime import datetime
import s3fs
import pymysql
from db_setting import DB_HOST, DB_NAME, DB_PASSWORD, DB_USER
import requests
import zlib

def extract_cp2_log():
    # mysql 에서 extract
    # conn = pymysql.connect(host=DB_HOST,user=DB_USER,password=DB_PASSWORD,db=DB_NAME,charset='utf8')
    # cur = conn.cursor(pymysql.cursors.DictCursor)
    # sql = 'SELECT * FROM user';
    # cur.execute(sql)
    # result=cur.fetchall()

    # api extract
    api_get=requests.get('http://3.34.232.234:8000/log')
    log_json=json.loads(api_get.text)
    result=pd.DataFrame(log_json)

    # result.to_csv('s3://hssong-cp2-bucket/cp2_db_user.csv')
    result.to_csv('cp2_log.csv',index=False)
    data=pd.read_csv('cp2_log.csv')
    for i in range(len(data)):
        # len 426
        log = data.logs[i]
        # len 362
        compress_data = zlib.compress(log.encode(encoding='utf-8'))
        data.logs[i]=compress_data
    data.to_csv('cp2_transform.csv',index=False)
    data=pd.read_csv('cp2_transform.csv')
    data.to_csv('s3://hssong-cp2-bucket/cp2_log.csv')
    

# def transform_cp2_log():
#     data=pd.read_csv('cp2_log.csv')
#     for i in range(len(data)):
#         # len 426
#         log = data.logs[i]
#         # len 362
#         compress_data = zlib.compress(log.encode(encoding='utf-8'))
#         data.logs[i]=compress_data
#     data.to_csv('cp2_transform.csv',index=False)

# def load_cp2_log():
#     data=pd.read_csv('cp2_transform.csv')
#     data.to_csv('s3://hssong-cp2-bucket/cp2_db_user.csv')


