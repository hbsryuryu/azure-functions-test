import logging
import os
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
import azure.functions as func
from dotenv import load_dotenv

import re
import pyodbc

import pandas as pd

import time

load_dotenv(override=True)  # ローカル用。Azure ではアプリ設定を参照
TARGET_WEBAPP_URL = os.getenv("TARGET_WEBAPP_URL", "")

HOST = os.environ.get("HOST_NAME") # ホスト名
USN = os.environ.get("USER_NAME") # ユーザー名
PWD = os.environ.get("PASSWORD") # パスワード
DSN = os.environ.get("DB_NAME") # データベース名


# 必要なライブラリをインポート
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, Text, JSON, ForeignKey, Sequence, CheckConstraint, UniqueConstraint, Index, LargeBinary
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.orm import joinedload


from sqlalchemy import create_engine, Column,Integer, String, Boolean, DateTime, Text, ForeignKey, JSON, inspect
from sqlalchemy.orm import scoped_session, sessionmaker, Session, relationship, joinedload
from sqlalchemy.orm import declarative_base
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy import update, select
from sqlalchemy.exc import DBAPIError

import datetime
datetime_datetime_now = datetime.datetime.now # 読み込み少しでも高速化
# del datetime # メモリ削減にならない、関連の参照先がいるため残る
from zoneinfo import ZoneInfo
japan_zoneinfo = ZoneInfo("Asia/Tokyo") # 読み込み少しでも高速化

import orjson
default_nested_bson = orjson.dumps({})
orjson_dumps = orjson.dumps
orjson_loads = orjson.loads

def main(myTimer: func.TimerRequest) -> None:  # ← function.json の name と一致させる

    if myTimer.past_due:
        print("Timer is past due!")
    else:
        



        con_str_f = "{"
        con_str_b = "}"
        versions = []
        drivers = pyodbc.drivers()
        pattern = re.compile(r"ODBC Driver (\d+) for SQL Server")

        for driver in drivers:
            match = pattern.search(driver)
            if match:
                versions.append(int(match.group(1)))

        odbc_driver = None
        if versions:
            lastest = sorted(versions, reverse=True)[0]
            odbc_driver = con_str_f + "ODBC Driver " + str(lastest) + " for SQL Server" + con_str_b

        elif "SQL Server" in drivers:
            odbc_driver = con_str_f + "SQL Server" + con_str_b

        print("ここODBCドライバー")
        print(odbc_driver)
        print("ここODBCドライバー")

        # base_url = "https://script.google.com/macros/s/AKfycbxhiKxdfJDRhkF7DZfJfEI8p-O1ZjUJHWdzc3Voalug8VNsryuRWEUwAGnUiAbQ7qxF/exec"

        # headers = {'content-type': 'application/json'}
        # data = {
        #     "col_1":"log送信テスト",
        #     "col_2":"log送信テスト",
        #     "col_3":"log送信テスト",
        #     "col_4":"log送信テスト",
        #     "col_5":"log送信テスト",
        #     "col_6":"log送信テスト",
        #     "col_7":"log送信テスト",
        # }
        # res_auth = requests.post(url=base_url,headers=headers,json=data)
        # res_auth.text

        # time.sleep(2600)

        # base_url = "https://script.google.com/macros/s/AKfycbxhiKxdfJDRhkF7DZfJfEI8p-O1ZjUJHWdzc3Voalug8VNsryuRWEUwAGnUiAbQ7qxF/exec"

        # headers = {'content-type': 'application/json'}
        # data = {
        #     "col_1":"log送信テスト",
        #     "col_2":"log送信テスト",
        #     "col_3":"log送信テスト",
        #     "col_4":"log送信テスト",
        #     "col_5":"log送信テスト",
        #     "col_6":"log送信テスト",
        #     "col_7":"log送信テスト",
        # }
        # res_auth = requests.post(url=base_url,headers=headers,json=data)
        # res_auth.text

        # test_i = 0
        # for i in range(36000):
        #     test_i +=1
        #     if test_i > 200:
        #         base_url = "https://script.google.com/macros/s/AKfycbxhiKxdfJDRhkF7DZfJfEI8p-O1ZjUJHWdzc3Voalug8VNsryuRWEUwAGnUiAbQ7qxF/exec"

        #         headers = {'content-type': 'application/json'}
        #         data = {
        #             "col_1":"log送信テスト",
        #             "col_2":"log送信テスト",
        #             "col_3":"log送信テスト",
        #             "col_4":"log送信テスト",
        #             "col_5":"log送信テスト",
        #             "col_6":"log送信テスト",
        #             "col_7":str(i),
        #         }
        #         res_auth = requests.post(url=base_url,headers=headers,json=data)
        #         res_auth.text
        #         test_i = 0
        #     print(f"作業中{i}", flush=True)  # flushで即時出力




        # engineの設定
        engine = create_engine(f'mysql+mysqlconnector://{USN}:{PWD}@{HOST}/{DSN}?charset=utf8mb4')

        # セッションの作成
        SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=engine
        )

        class DBSession:
            def __enter__(self):
                self.db = SessionLocal()
                return self.db

            def __exit__(self, exc_type, exc_val, exc_tb):
                self.db.close() # エラーはいても先にclose走る

        # Baseの作成（モデル定義がまだの場合）
        Base = declarative_base()

        class SearchIndexName(Base):
            __tablename__ = 'search_index_name'
            id = Column(Integer, primary_key=True)
            index_name = Column(String(100),nullable=False,unique=True) # mysqlでは文字数指定必須
            # python内部の連携（物理接続ではない）
            search_index_data = relationship('SearchIndexData', back_populates='search_index_name') # SearchIndexName.search_index_data -> SearchIndexData.search_index_name

        class SearchIndexData(Base):
            __tablename__ = 'search_index_data'
            id = Column(Integer, primary_key=True)
            index_pk_id = Column(String(100),nullable=False,unique=True)
            nested_bson = Column(LargeBinary(length=65535),default=default_nested_bson) # mysqlだけlength適応64KB
            # 外部キー
            search_index_name_id = Column(Integer, ForeignKey('search_index_name.id'))
            # python内部の連携（物理接続ではない）
            search_index_name = relationship('SearchIndexName', back_populates='search_index_data') # SearchIndexData.search_index_name -> SearchIndexName.search_index_data

        print("データ取得開始！！！")
        stmt_sample = (
            select(SearchIndexData.nested_bson)
            .limit(10)
        )

        with DBSession() as db_session:
            basic_auth = db_session.execute(stmt_sample).all() # sqlalchmey2.0の書き方
            if basic_auth:
                db_res = [
                    orjson_loads(_b) for (_b,) in basic_auth
                ]
            else:
                db_res = []
            print(len(db_res))
        print("データ取得おわり！！！")

        df = pd.DataFrame({'A': ['A1', 'A2', 'A3'],
            'B': ['B1', 'B2', 'B3'],
            'C': ['C1', 'C2', 'C3']},
            index=['ONE', 'TWO', 'THREE'])
        print(df)

    pass