import requests
import os
import pandas as pd
import numpy as np
import json
import urllib.request
from sqlalchemy import create_engine
from datetime import datetime
import cx_Oracle
from conexaoBanco import conectarBanco

#TRAZENDO PARA O CÓDIGO O ARQUIVO CONFIG, ONDE ESTÁ PARAMETRIZADO AS CONEXÕES EXTERNAS

with open("./conf/config.json") as json_data_file:
        config = json.load(json_data_file)

con, cur = conectarBanco()
CURSOR = cur.cursor()

page = 1
while True:
    #REQUISIÇÃO DA API + NORMALIZAÇÃO DE DATA
    url = config['API']['URL'] + str(page)
    payload = {}
    headers = {
        'Authorization': config['API']['AUTH']
    }

    response = requests.request("GET", url, headers=headers, data=payload, )
    data = json.loads(response.content)
    dados = pd.json_normalize(data['items'], max_level=50)
    dados.replace({np.nan:''}, inplace=True)

    #PAGINAÇÃO SE CASO ESTIVER VAZIO PARAR O LOOP
    if dados.empty:
     print('Pagina Vazia!')
     break


    #FORMATAÇÃO DE COLUNAS
    colunas =['event_id','question.id','section.name','question.name','content_format', 
              'comment','attachments','file_url','latitude','longitude','created_at',
              'id','item_id', 'author_id','has_media','collection_item_id','event_created_at',
              'event_finished_at', 'event_status','author.full_name','author.email', 'author.username',
              'formulary.name',
              'formulary.created_at','section.id','section.description','section.created_at',
              'question.question_type','question.parent_question_id','question.created_at',
              'option.id','option.name', 'formulary.id', 'author_id','score_weight',
              'formulary_group.id', 'user_profile.name', 'user_profile.id', 'section.position','option.position','question.position','question.score_weight', 'formulary_group.name']
    formatacao = dados.loc[:, colunas]

    #FORMATAÇÃO DE DATAS 
    for index, row in formatacao.iterrows():
        try:
        #Formatando a data de criação do formulario.
            formatacao.at[index,'formulary.created_at'] = pd.to_datetime(row['formulary.created_at'], format='%Y/%m/%dT%H:%M:%S.%f%z')
            formatacao.at[index,'formulary.created_at'] = formatacao.at[index,'formulary.created_at'].strftime('%Y/%m/%d %H:%M:%S')
        
        #Formatando a data da criação do objeto de pergunta 
            formatacao.at[index,'created_at'] = pd.to_datetime(row['created_at'], format='%Y/%m/%dT%H:%M:%S.%f%z')
            formatacao.at[index,'created_at'] = formatacao.at[index,'created_at'].strftime('%Y/%m/%d %H:%M:%S')
        
        #Formatando a data do ínicio de um preenchimento
            formatacao.at[index,'event_created_at'] = pd.to_datetime(row['event_created_at'], format='%Y/%m/%dT%H:%M:%S.%f%z')
            formatacao.at[index,'event_created_at'] = formatacao.at[index,'event_created_at'].strftime('%Y/%m/%d %H:%M:%S')            

        #Formatando a data do fim de um preenchimento
            formatacao.at[index,'event_finished_at'] = pd.to_datetime(row['event_finished_at'], format='%Y/%m/%dT%H:%M:%S.%f%z')
            formatacao.at[index,'event_finished_at'] = formatacao.at[index,'event_finished_at'].strftime('%Y/%m/%d %H:%M:%S')             

        #Formatando a data de uma seção criada
            formatacao.at[index,'section.created_at'] = pd.to_datetime(row['section.created_at'], format='%Y/%m/%dT%H:%M:%S.%f%z')
            formatacao.at[index,'section.created_at'] = formatacao.at[index,'section.created_at'].strftime('%Y/%m/%d %H:%M:%S')            

        #Formatando a data de uma questão criada
            formatacao.at[index,'question.created_at'] = pd.to_datetime(row['question.created_at'], format='%Y/%m/%dT%H:%M:%S.%f%z')
            formatacao.at[index,'question.created_at'] = formatacao.at[index,'question.created_at'].strftime('%Y/%m/%d %H:%M:%S')            
            
        except:
            print('Provavel data nulo', index)

    #RETORNO DE ANEXOS
    for index, row in formatacao.iterrows():
        if row["attachments"] and row["attachments"][0]:
            formatacao.at[index,'attachments'] = row["attachments"][0]['remote_url'] 
        else:
            formatacao.at[index,'attachments'] = ''
           
    # CRIANDO UMA LISTA DE TUPLAS COM OS DADOS DO DATAFRAME
    values = [tuple(str(x) for x in row) for row in formatacao.values]


    sql = open(config['SQL']['CAMINHO'] + config['SQL']['WECK_IDRESPOSTA'], 'r', encoding='ansi').read()

    #TRATANDO DADOS EXISTENTES
    seleciona = pd.read_sql_query(sql, connection,index_col=None,coerce_float=True)
    formatacao = pd.merge(formatacao, seleciona, how='left', left_on='id', right_on='ID_RESPOSTA')
                  
    for index, row in formatacao.iterrows():
        if pd.isnull(row['ID_RESPOSTA']):
            try:
                CURSOR.execute("INSERT INTO weck_dadoentrada (ID_DADO, ID_EVENTO, ID_QUESTAO, NM_SECAO,DS_QUESTAO, DS_RESPOSTA, DS_COMENTARIO, DS_ANEXO, DS_URL, GS_LATITUDE, GS_LONGITUDE, DT_CRIACAO, ID_RESPOSTA, ID_FORMULARIO, ID_USER,FG_MEDIA, ID_COLECAO, DT_INICIOPREENCHIMENTO, DT_FIMPREENCHIMENTO, FG_STATUS, NM_USER, DS_USER, CD_USER, NM_FORM, DT_FORMCRIADO, ID_SECAO, DS_SECAO, DT_SECAOCRIADA, FG_QUESTAO, ID_QFILHO, DT_QUESTAOCRIADA, ID_OPCAO, NM_OPCAO, ID_FORMULARIOCRIADO, ID_CRIADOR, QT_PESO, ID_PROJETO, NM_USERPERFIL, ID_USERPERFIL, NO_SECAO, NO_OPCAO, NO_QUESTAO, QT_PESOFILHO, NM_PROJETO) VALUES ('', :0, :1, :2, :3, :4, :5, :6, :7, :8, :9, to_date(:10, 'YYYY/MM/DD HH24:MI:SS'), :11, :12, :13, :14, :15, to_date(:16, 'YYYY/MM/DD HH24:MI:SS'), to_date(:17, 'YYYY/MM/DD HH24:MI:SS'), :18, :19, :20, :21, :22, to_date(:23, 'YYYY/MM/DD HH24:MI:SS'),:24, :25, to_date(:26, 'YYYY/MM/DD HH24:MI:SS'),:27, :28, to_date(:29, 'YYYY/MM/DD HH24:MI:SS'),:30, :31, :32, :33, :34, :35, :36, :37, :38, :39, :40, :41, :42)", values[index])
                cur.commit()
                print('Dados inseridos no banco Oracle com sucesso!')
            except cx_Oracle.DatabaseError as e:
                print('Error:', e)
                cur.rollback()
        else:
          pass
    #Paginação
    page+= 1