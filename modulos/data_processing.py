import pandas as pd
import numpy as np
import sys


def create_df(content_list, table, conn, LOGGER_OBJ):

    LOGGER_OBJ.info(f"Realizando leitura no banco de dados: {table}")
    df_dbmaria = pd.read_sql(f"SELECT  * FROM ALBION.{table}", conn)

    LOGGER_OBJ.info(f"Criando DataFrame para ingestão de dados")
    if table =='potion_prices':
        df_scrapy = process_potion_table(content_list, LOGGER_OBJ)

    elif table == 'gold_prices':
        df_scrapy = process_gold_table(content_list, LOGGER_OBJ)

    try:
        df_ingest = compare_dataframes(table, df_scrapy, df_dbmaria, LOGGER_OBJ)

        
    except Exception as e:
        LOGGER_OBJ.error(f"Falha na criação do dataframe: {e}")
        sys.exit(1)
    
    LOGGER_OBJ.info(f"Verificando se há registros para ingestão")

    if len(df_ingest) > 0:
        LOGGER_OBJ.info(f"{len(df_ingest)} a serem inseridas")
        ingest = True

    else:
        LOGGER_OBJ.info("Não há novos registros")
        ingest = False

    return df_ingest, ingest

def process_potion_table(content_list, LOGGER_OBJ):
    LOGGER_OBJ.info(f"Criando DataFrame adquirido na requisição")
    data_from_item = []
    for all_info in content_list:
        data = all_info['data']
        for item_info in data:
            row_of_item = {'timestamp': item_info['timestamp'],
            'item_id': all_info['item_id'],
            'location': all_info['location'],
            'item_count': item_info['item_count'],
            'price': item_info['avg_price']}
            data_from_item.append(row_of_item)
    df_scrapy = pd.DataFrame(data_from_item)
    df_scrapy[['timestamp']] = df_scrapy[['timestamp']].apply(pd.to_datetime)

    return df_scrapy

def process_gold_table(content_list, LOGGER_OBJ):
    LOGGER_OBJ.info(f"Criando DataFrame adquirido na requisição")
    df_scrapy = pd.DataFrame(content_list)
    df_scrapy[['timestamp']] = df_scrapy[['timestamp']].apply(pd.to_datetime)

    return df_scrapy
    
def df_ingest_gold(df_scrapy, df_dbmaria):
    dfe = pd.merge(df_scrapy, df_dbmaria, on=['timestamp'], how='left', indicator='Exist')
    dfe['Exist'] = np.where(dfe.Exist == 'both', True, False)
    df_diff = dfe[dfe['Exist'] == False]
    df_diff = df_diff[['timestamp', 'price_x']]
    df_diff.rename(columns={'price_x': 'price'}, inplace=True)           
    df_ingest = df_diff.astype({'timestamp': str, 'price': int})

    return df_ingest

def df_ingest_item(df_scrapy, df_dbmaria):
    dfe = pd.merge(df_scrapy, df_dbmaria, on=['timestamp','item_id'], how='left', indicator='Exist')
    dfe['Exist'] = np.where(dfe.Exist == 'both', True, False)
    df_diff = dfe[dfe['Exist'] == False]
    df_diff = df_diff[['timestamp', 'item_id', 'location_x', 'item_count_x', 'price_x']]
    df_diff.rename(columns={'location_x': 'location', 'item_count_x': 'item_count', 'price_x': 'price'}, inplace=True)
    df_ingest = df_diff.astype({'timestamp': str, 'item_id': str, 'location': str, 'item_count': int, 'price': int})

    return df_ingest

def compare_dataframes(table, df_scrapy, df_dbmaria, LOGGER_OBJ):
    LOGGER_OBJ.info(f"Comparando tabelas para ingestão de dados faltantes")
    if table == 'gold_prices':
        LOGGER_OBJ.info(f"Criando tabela de ingestão para ouro")
        df_ingest = df_ingest_gold(df_scrapy, df_dbmaria)

    elif table == 'potion_prices':
        LOGGER_OBJ.info(f"Criando tabela de ingestão para itens")
        df_ingest = df_ingest_item(df_scrapy, df_dbmaria)

    return df_ingest