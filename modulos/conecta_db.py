import pandas as pd
import requests
import ast
import mariadb
from logger.Logger import etlLogger
import sys
from datetime import datetime, timedelta

def connect_db(LOGGER_OBJ):
    LOGGER_OBJ.info('Conectando ao banco de dados')
    try:
        conn = mariadb.connect(
            user="ciurlini",
            password="casa2406",
            host="localhost",
            port=3306,
            database="ALBION"
        )
        LOGGER_OBJ.info("Conexão estabelecida")
    except mariadb.Error as e:
        LOGGER_OBJ.error(f"Falha na conexão: {e}")
        sys.exit(1)

    cur = conn.cursor()

    return conn, cur

def ingest_data_gold(df_ingest, conn, cur, LOGGER_OBJ):

    LOGGER_OBJ.info("Realizando ingestão de dados")
    cols = "`,`".join([str(i) for i in df_ingest.columns.tolist()])

    try:
        for i,row in df_ingest.iterrows():
            sql = "INSERT INTO `"+table+"` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
            cur.execute(sql, tuple(row))
        conn.commit()
        LOGGER_OBJ.info("Ingestão realizada com sucesso")
    except Exception as e:
        LOGGER_OBJ.error(f"Falha ao realizar ingestão de dados: {e}")