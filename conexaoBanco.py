import sqlalchemy.engine as eng
from sqlalchemy import exc
import json

def conectarBanco():
    with open("./conf/config.json") as json_data_file:
        config = json.load(json_data_file)

    DIALECT = config['DATABASE']['DIALECT']
    SQL_DRIVER = config['DATABASE']['SQL_DRIVER']
    USERNAME = config['DATABASE']['USERNAME']
    PASSWORD = config['DATABASE']['PASSWORD']
    PORT = config['DATABASE']['PORT']
    SERVICE =  config['APP']['ENVIRONMENT']
    HOST =  config['APP']['HOST']

    ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + USERNAME + ':' + PASSWORD +'@' + HOST + ':' + str(PORT) + '/?service_name=' + SERVICE

    con = eng.create_engine(ENGINE_PATH_WIN_AUTH)
    cur = con.raw_connection()

    return con, cur

