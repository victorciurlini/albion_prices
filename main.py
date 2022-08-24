import pandas as pd
import requests
import ast
import mariadb
from logger.Logger import etlLogger
import sys
from datetime import datetime, timedelta
from modulos.conecta_db import connect_db, ingest_data_gold
from modulos.crawler import get_response
from modulos.data_processing import create_df_gold

def main():
    LOGGER_OBJ = etlLogger(project_name='gold_predict')
    LOGGER_OBJ.info("Inicio da rotina")
    cities = ['Bridgewatch', 'Caerleon', 'Lymhurst', 'Martlock', 'Thetford']
    dt_today = datetime.now()
    dt_tomorrow = datetime.now() + timedelta(days=1)
    str_date_today = dt_today.strftime("%m-%d-%Y")
    str_date_tomorrow = dt_tomorrow.strftime("%m-%d-%Y")
    URL = f"https://www.albion-online-data.com/api/v2/stats/gold?date={str_date_today}&end_date={str_date_tomorrow}"

    list_of_items = ['T4_POTION_HEAL','T6_POTION_HEAL', 'T4_POTION_HEAL@1', 'T6_POTION_HEAL@1']
    
    conn, cur = connect_db(LOGGER_OBJ)
    content_list = get_response(URL, LOGGER_OBJ)
    df_ingest, ingest = create_df_gold(content_list, conn, LOGGER_OBJ)
    ingest_data_gold(df_ingest, conn, cur, LOGGER_OBJ)
    for table in list_of_items:
        pass

if __name__ == '__main__':
    main()
