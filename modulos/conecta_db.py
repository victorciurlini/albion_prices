import mariadb
import sys
from modulos.funcoes_aux import read_yaml

def connect_db(LOGGER_OBJ):
    LOGGER_OBJ.info('Conectando ao banco de dados')
    cred = read_yaml('config/config.yaml')
    try:
        conn = mariadb.connect(
            user=cred['DB']['USER'],
            password=cred['DB']['PSSWRD'],
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

def ingest_data(df_ingest, table, conn, cur, LOGGER_OBJ):

    LOGGER_OBJ.info(f"Realizando ingestão de dados da tabela {table}")
    cols = "`,`".join([str(i) for i in df_ingest.columns.tolist()])

    try:
        for i,row in df_ingest.iterrows():
            sql = f"INSERT INTO ALBION."+table+" (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
            cur.execute(sql, tuple(row))
        conn.commit()
        LOGGER_OBJ.info("Ingestão realizada com sucesso")
    except Exception as e:
        LOGGER_OBJ.error(f"Falha ao realizar ingestão de dados: {e}")