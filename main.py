import pandas as pd
import requests
import ast
import mariadb
from logger.Logger import etlLogger
import sys
from datetime import datetime, timedelta
from modulos.conecta_db import connect_db, ingest_data
from modulos.crawler import get_response
from modulos.data_processing import create_df, get_urls

def main():
    LOGGER_OBJ = etlLogger(project_name='gold_predict')
    LOGGER_OBJ.info("Inicio da rotina")
    table_gold = 'gold_prices'
    table_potion = 'potion_prices'
    list_of_tables = ['gold_prices', 'potion_prices']
    list_of_urls = get_urls()

    conn, cur = connect_db(LOGGER_OBJ)
    for table, url in zip(list_of_tables, list_of_urls):
        content_list = get_response(url, LOGGER_OBJ)
        df_ingest, ingest = create_df(content_list, table, conn, LOGGER_OBJ)
        if ingest == True:
            ingest_data(df_ingest, table, conn, cur, LOGGER_OBJ)
            ingest = False
        else:
            LOGGER_OBJ.info("Não há dados para ingestão")

if __name__ == '__main__':
    main()
