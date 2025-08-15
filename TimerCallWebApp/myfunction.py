import logging
import os
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

import re
import pyodbc

import pandas as pd
import numpy as np

import time

load_dotenv(override=True)  # ローカル用。Azure ではアプリ設定を参照
TARGET_WEBAPP_URL = os.getenv("TARGET_WEBAPP_URL", "")

HOST = os.environ.get("HOST_NAME") # ホスト名
USN = os.environ.get("USER_NAME") # ユーザー名
PWD = os.environ.get("PASSWORD") # パスワード
DSN = os.environ.get("DB_NAME") # データベース名

SEARCH_SERVICE_URL = os.getenv("COG_END_POINT")
API_KEY = os.getenv("COG_API_KEY")

INDEX_NAME_MEDIA_AGENT = os.getenv("INDEX_NAME_MEDIA_AGENT")
INDEX_NAME_MEDIA_CLIENT = os.getenv("INDEX_NAME_MEDIA_CLIENT")


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





def testtest2():
    print("ちゃんと外部のここ読みおkまれました！")

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
        .limit(1)
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
    df = pd.DataFrame(db_res)
    len(df)
    print("これデータフレーム！")

    df2 = pd.DataFrame({'A': ['A1', 'A2', 'A3'],
        'B': ['B1', 'B2', 'B3'],
        'C': ['C1', 'C2', 'C3']},
        index=['ONE', 'TWO', 'THREE'])
    print(df2)

    time.sleep(15)
    print("処理終わり！")

    return None



def get_df():
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
        # .limit(1)
    )
    db_res = []
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
    df = pd.DataFrame(db_res)
    return df



def push_index(df):
    df["THEME"] = df["THEME"].replace({
        "mkt": "marketing",
        "crt": "creative"
    })
    print(list(df["THEME"].unique()))

    df["CATEGORY"] = df["CATEGORY"].replace({
        "mda": "media",
        "pbl": "publish",
        "edc": "education",
        "evt": "event",
    })

    print(list(df["CATEGORY"].unique()))

    df["BRAND"] = df["BRAND"].replace({
        "khk": "kouhoukaigi",
        "hsk": "hansokukaigi",
        "sdk": "sendenkaigi",
        "brn": "brain"
    })
    # print(list(df["BRAND"].unique()))

    df.loc[df["BRAND"] != "kkb", "ARTICLE_URL"] = (
        "https://www.sendenkaigi.com/" + df["THEME"] + "/" +
        df["CATEGORY"] + "/" + df["BRAND"] + "/" +
        df["CONTENT_ID"] + "/"
    )

    df.loc[df["BRAND"] == "kkb", "ARTICLE_URL"] = (
        "https://www.kankyo-business.jp/news/" +
        df["CONTENT_ID"]
    )

    df.loc[df["CATEGORY"] == "publish", "ARTICLE_URL"] = (
        "https://www.sendenkaigi.com/" +
        df["THEME"] + "/books/" + df["CONTENT_ID"]+ "/"
    )

    df.loc[df["CATEGORY"] == "event", "ARTICLE_URL"] = (
        "https://www.sendenkaigi.com/" +
        "marketing/event/" + df["CONTENT_ID"]+ "/"
    )

    df.loc[df["CATEGORY"] == "education", "ARTICLE_URL"] = (
        "https://www.sendenkaigi.com/" +
        df["THEME"] + "/courses"+ "/" + df["CONTENT_ID"]+ "/"
    )

    df["BRAND"] = df["BRAND"].replace({
        "kouhoukaigi": "月刊『広告会議』",
        "hansokukaigi": "月刊『販促会議』",
        "sendenkaigi": "月刊『宣伝会議』",
        "brain": "月刊『ブレーン』",
        "kkb": "環境ビジネス"
    })
    # print(list(df["BRAND"].unique()))

    df["MDA_FREE_FLG"] = df["MDA_FREE_FLG"].apply(
        lambda x: True if str(x).lower() == "true" else
                False if str(x).lower() == "false" else
                None
    )
    df["MDA_FREE_FLG"] = df["MDA_FREE_FLG"].astype(str)
    # print(list(df["MDA_FREE_FLG"].unique()))


    def remove_html_tag(str_html):
        if not str_html:
            return ""
        str_html = str_html.replace("\n\n\n","").replace("\n\n","").replace("&quot;","")
        return re.sub(re.compile('<.*?>'), '', str_html).replace("\n\n\n","").replace("\n\n","")

    df["LEAD_TEXT"] = df["LEAD_TEXT"].map(remove_html_tag)
    df["ARTICLE_TEXT"] = df["ARTICLE_TEXT"].map(remove_html_tag)
    df["LEAD_TEXT"] = df["LEAD_TEXT"].replace(r'^\s*$|nan', "null", regex=True)
    df["ARTICLE_TEXT"] = df["ARTICLE_TEXT"].replace(r'^\s*$|nan', "null", regex=True)
    conditions = (df["LEAD_TEXT"].str.len() < 50) & (df["ARTICLE_TEXT"].str.len() < 300) & (df["TITLE"].str.len() < 300)
    df = df[~conditions]

    def generate_summary(row):
        res_str = ""
        if len(row["LEAD_TEXT"]) > 5:
            res_str += "記事のまとめは「" + row["LEAD_TEXT"] + "」です。"
        if len(row["ARTICLE_TEXT"]) > 5:
            res_str += "記事の詳細は、「" + row["ARTICLE_TEXT"] + "」です。"
        if len(row["TITLE"]) > 5:
            res_str += "記事のタイトルは、「" + row["TITLE"] + "」です。"
        if len(row["TITLE"]) > 5:
            res_str += "記事のタイトルは、「" + row["TITLE"] + "」です。"
        if row["BRAND"]:
            res_str += "掲載号／雑誌名は、"+ row["BRAND"] + "です。"
        if len(row["PUBLISH_START_DATE"]) > 5:
            res_str += "公開年月は、"+ row["PUBLISH_START_DATE"] + "です。"
        res_str += "記事URLは、" + row["ARTICLE_URL"] + "です。"
        return res_str

    df["SUMMARY"] = df.apply(generate_summary,axis=1)


    # CONTENT_ID と DATA_SOURCE を結合して INDEX_ID を作成
    df["INDEX_ID"] = (
        df["CONTENT_ID"].fillna("").astype(str) + "-" +
        df["DATA_SOURCE"].fillna("").astype(str)
    )

    # 正規化処理
    df["INDEX_ID"] = (
        df["INDEX_ID"]
        .str.lower()                  # 小文字化
        .str.replace(".", "", regex=False) 
        .str.replace("‗", "_", regex=False) 
        .str.replace(" ", "_", regex=False)  # スペースをハイフンに（念のため）
        .str.strip("-")                # 先頭末尾のハイフンを除去
    )

    PK_ID = "CONTENT_ID"

    # media-data-searchable-only-summary-for-agent

    index_name = INDEX_NAME_MEDIA_AGENT
    index_schema = {
            "name": str(index_name),
            "fields": [
                {'key': True, 'searchable': False, 'filterable': False, 'retrievable': True, 'sortable': False, 'facetable': False, 'type': 'Edm.String', 'synonymMaps': [], 'name': PK_ID},
                # {'key': False, 'searchable': False, 'filterable': False, 'retrievable': False, 'sortable': False, 'facetable': False, 'type': 'Edm.String', 'synonymMaps': [], 'name': 'BRAND'},
                # {'key': False, 'searchable': False, 'filterable': False, 'retrievable': False, 'sortable': False, 'facetable': False, 'type': 'Edm.String', 'synonymMaps': [], 'name': 'TITLE'},
                # {'key': False, 'searchable': False, 'filterable': False, 'retrievable': False, 'sortable': False, 'facetable': False, 'type': 'Edm.String', 'synonymMaps': [], 'name': 'LEAD_TEXT'},
                {'key': False, 'searchable': True, 'filterable': True, 'retrievable': True, 'sortable': True, 'facetable': True, 'type': 'Edm.String', "analyzer": "ja.microsoft", 'synonymMaps': [], 'name': 'SUMMARY'},
            ]
    }

    url = f"{SEARCH_SERVICE_URL}/indexes?api-version=2023-07-01-Preview"
    headers = {
        "Content-Type": "application/json",
        "api-key": API_KEY
    }
    response = requests.post(url, headers=headers, json=index_schema)
    print(response.json())

    df_upload = df[df["CATEGORY"]=="media"].copy()
    print(list(df_upload["CATEGORY"].unique()))
    # 特定カラムのみ残す
    df_upload = df_upload[[
        "INDEX_ID",
        # "BRAND",
        # "TITLE",
        # "LEAD_TEXT",
        "SUMMARY"
    ]]
    df_upload.columns = [
        PK_ID,
        # "BRAND",
        # "TITLE",
        # "LEAD_TEXT",
        "SUMMARY"
    ]

    # Azure Search のアクション指定
    df_upload["@search.action"] = "upload"

    # NaN, inf を空文字へ（文字列フィールド用）
    df_upload = df_upload.replace([np.nan, np.inf, -np.inf], "")

    # Azure AI Search のエンドポイントと API キー
    url = f"{SEARCH_SERVICE_URL}/indexes/{index_name}/docs/index?api-version=2023-07-01-Preview"
    headers = {
        "Content-Type": "application/json",
        "api-key": API_KEY
    }

    batch_size = 100  # 必要に応じて調整

    for i in range(0, len(df_upload), batch_size):
        time.sleep(1)
        batch_df = df_upload.iloc[i:i+batch_size].copy()

        batch_data = {
            "value": batch_df.to_dict(orient="records")
        }

        response = requests.post(url, json=batch_data, headers=headers)

        # print(f"Batch {i} response status: {response.status_code}")
        try:
            print(response.json())
        except Exception as e:
            print(f"Error decoding JSON: {e}")

    # 結果を表示
    # print(response.json())
    print("agent側index書き込み完了！")


    # media-data-searchable-only-summary-for-client

    index_name = INDEX_NAME_MEDIA_CLIENT
    index_schema = {
        "name": str(index_name),
        "fields": [
            {'key': True, 'searchable': False, 'filterable': False, 'retrievable': True, 'sortable': False, 'facetable': False, 'type': 'Edm.String', 'synonymMaps': [], 'name': PK_ID},
            # {'key': False, 'searchable': False, 'filterable': False, 'retrievable': False, 'sortable': False, 'facetable': False, 'type': 'Edm.String', 'synonymMaps': [], 'name': 'CONTENT_ID'},
            # {'key': False, 'searchable': False, 'filterable': False, 'retrievable': False, 'sortable': False, 'facetable': False, 'type': 'Edm.String', 'synonymMaps': [], 'name': 'DATA_SOURCE'},
            {'key': False, 'searchable': False, 'filterable': False, 'retrievable': True, 'sortable': False, 'facetable': False, 'type': 'Edm.String', 'synonymMaps': [], 'name': 'BRAND'},
            {'key': False, 'searchable': False, 'filterable': False, 'retrievable': True, 'sortable': False, 'facetable': False, 'type': 'Edm.String', 'synonymMaps': [], 'name': 'CATEGORY'},
            # {'key': False, 'searchable': False, 'filterable': False, 'retrievable': False, 'sortable': False, 'facetable': False, 'type': 'Edm.String', 'synonymMaps': [], 'name': 'UNIT'},
            {'key': False, 'searchable': False, 'filterable': False, 'retrievable': True, 'sortable': False, 'facetable': False, 'type': 'Edm.String', 'synonymMaps': [], 'name': 'TITLE'},
            {'key': False, 'searchable': False, 'filterable': False, 'retrievable': True, 'sortable': False, 'facetable': False, 'type': 'Edm.String', 'synonymMaps': [], 'name': 'LEAD_TEXT'},
            # {'key': False, 'searchable': True, 'filterable': True, 'retrievable': False, 'sortable': True, 'facetable': True, 'type': 'Edm.String', "analyzer": "ja.microsoft", 'synonymMaps': [], 'name': 'SUMMARY'},
            # {'key': False, 'searchable': False, 'filterable': False, 'retrievable': False, 'sortable': False, 'facetable': False, 'type': 'Edm.String', 'synonymMaps': [], 'name': 'IMAGE_URL'},
            # {'key': False, 'searchable': False, 'filterable': False, 'retrievable': False, 'sortable': False, 'facetable': False, 'type': 'Edm.String', 'synonymMaps': [], 'name': 'PRICE'},
            {'key': False, 'searchable': False, 'filterable': False, 'retrievable': True, 'sortable': False, 'facetable': False, 'type': 'Edm.DateTimeOffset', 'synonymMaps': [], 'name': 'PUBLISH_START_DATE'},
            # {'key': False, 'searchable': False, 'filterable': False, 'retrievable': False, 'sortable': False, 'facetable': False, 'type': 'Edm.String', 'synonymMaps': [], 'name': 'FLAGS'},
            # {'key': False, 'searchable': False, 'filterable': False, 'retrievable': False, 'sortable': False, 'facetable': False, 'type': 'Edm.String', 'synonymMaps': [], 'name': 'TAGS'},
            {'key': False, 'searchable': False, 'filterable': False, 'retrievable': True, 'sortable': False, 'facetable': False, 'type': 'Edm.String', 'synonymMaps': [], 'name': 'ARTICLE_TEXT'},
            {'key': False, 'searchable': False, 'filterable': False, 'retrievable': True, 'sortable': False, 'facetable': False, 'type': 'Edm.String', 'synonymMaps': [], 'name': 'ARTICLE_URL'},
            # {'key': False, 'searchable': False, 'filterable': False, 'retrievable': False, 'sortable': False, 'facetable': False, 'type': 'Edm.String', 'synonymMaps': [], 'name': 'MDA_FREE_FLG'},
            {'key': False, 'searchable': False, 'filterable': False, 'retrievable': True, 'sortable': False, 'facetable': False, 'type': 'Edm.String', 'synonymMaps': [], 'name': 'THEME'}
        ]
    }

    url = f"{SEARCH_SERVICE_URL}/indexes?api-version=2023-07-01-Preview"
    headers = {
        "Content-Type": "application/json",
        "api-key": API_KEY
    }
    response = requests.post(url, headers=headers, json=index_schema)
    print(response.json())

    df_upload = df[df["CATEGORY"]=="media"].copy()
    print(list(df_upload["CATEGORY"].unique()))
    df_upload["PRICE"] = df_upload["PRICE"].astype(str)

    df_upload = df_upload[[
        'INDEX_ID',
        # 'CONTENT_ID',
        # 'DATA_SOURCE',
        'BRAND',
        'CATEGORY',
        # 'UNIT',
        'TITLE',
        'LEAD_TEXT',
        # 'SUMMARY',
        # 'IMAGE_URL',
        # 'PRICE',
        'PUBLISH_START_DATE',
        # 'FLAGS',
        # 'TAGS',
        'ARTICLE_TEXT',
        'ARTICLE_URL',
        # 'MDA_FREE_FLG',
        'THEME'
    ]]
    df_upload.columns = [
        PK_ID,
        # 'CONTENT_ID',
        # 'DATA_SOURCE',
        'BRAND',
        'CATEGORY',
        # 'UNIT',
        'TITLE',
        'LEAD_TEXT',
        # 'SUMMARY',
        # 'IMAGE_URL',
        # 'PRICE',
        'PUBLISH_START_DATE',
        # 'FLAGS',
        # 'TAGS',
        'ARTICLE_TEXT',
        'ARTICLE_URL',
        # 'MDA_FREE_FLG',
        'THEME'
    ]

    # Azure Search のアクション指定
    df_upload["@search.action"] = "upload"

    # NaN, inf を空文字へ（文字列フィールド用）
    df_upload = df_upload.replace([np.nan, np.inf, -np.inf], "")

    # Azure AI Search のエンドポイントと API キー
    url = f"{SEARCH_SERVICE_URL}/indexes/{index_name}/docs/index?api-version=2023-07-01-Preview"
    headers = {
        "Content-Type": "application/json",
        "api-key": API_KEY
    }

    batch_size = 100  # 必要に応じて調整

    for i in range(0, len(df_upload), batch_size):
        time.sleep(1)
        batch_df = df_upload.iloc[i:i+batch_size].copy()

        batch_data = {
            "value": batch_df.to_dict(orient="records")
        }

        response = requests.post(url, json=batch_data, headers=headers)

        # print(f"Batch {i} response status: {response.status_code}")
        try:
            print(response.json())
        except Exception as e:
            print(f"Error decoding JSON: {e}")

    # 結果を表示
    # print(response.json())
    print("client側index書き込み完了！")


def testtest():
    df = get_df()
    if len(df) == 0:
        print("DBアクセスOKだけどデータなし！")
        return None
    
    print("indexへ書き込み！")
    push_index(df)
    print("完了！indexへ書き込み！")
    return None
    
