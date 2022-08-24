import pandas as pd
import ast
from logger.Logger import etlLogger

def create_df_gold(content_list, conn, LOGGER_OBJ):

    LOGGER_OBJ.info(f"Criando DataFrame para ingestão de dados")

    try:
        LOGGER_OBJ.info(f"Criando DataFrame adquirido na requisição")
        df_index = pd.DataFrame(content_list)
        df_index = df_index[['timestamp', 'price']]
        df_index[['timestamp']] = df_index[['timestamp']].apply(pd.to_datetime)

        LOGGER_OBJ.info(f"Realizando leitura no banco de dados")
        df_dbmaria = pd.read_sql(f"SELECT  * FROM ALBION.gold_prices", conn)

        LOGGER_OBJ.info(f"Comparando tabelas para ingestão de dados faltantes")
        df_diff = pd.concat([df_dbmaria, df_index]).drop_duplicates(keep=False)
        df_ingest = df_diff.astype({'timestamp': str, 'price': int})

    except Exception as e:
        LOGGER_OBJ.error(f"Falha na criação do dataframe: {e}")

    LOGGER_OBJ.info(f"Verificando se há registros para ingestão")

    if len(df_ingest) > 0:
        LOGGER_OBJ.info(f"{len(df_diff)} a serem inseridas")
        ingest = True
    else:
        LOGGER_OBJ.info("Não há novos registros")
        ingest = False
    return df_ingest, ingest