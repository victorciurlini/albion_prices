import requests
import ast
from logger.Logger import etlLogger
import sys

def get_response(URL, LOGGER_OBJ):
    LOGGER_OBJ.info(f"Realizando requisição: {URL}")

    try:
        response = requests.get(URL)
        content = response.content
        content_string = content.decode('UTF-8')
        content_list = ast.literal_eval(content_string)

    except Exception as e:
        LOGGER_OBJ.error(f"Falha na raspagem dos dados: {e}")
        sys.exit(1)

    return content_list