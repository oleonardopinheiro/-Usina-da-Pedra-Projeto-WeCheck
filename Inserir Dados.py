import requests
import os
import pandas as pd
import json
import urllib.request
from sqlalchemy import create_engine
from datetime import datetime
import cx_Oracle

#TRAZENDO PARA O CÓDIGO O ARQUIVO CONFIG, ONDE ESTÁ PARAMETRIZADO AS CONEXÕES EXTERNAS

with open("./conf/config.json") as json_data_file:
        config = json.load(json_data_file)

#CONFIGURAÇÕES DE CONEXÃO ORACLE CLOUD

client = config['DATABASE']['CLIENT']
cx_Oracle.init_oracle_client(lib_dir=client)
os.environ[config['DATABASE']['TNS']] = config['DATABASE']['ENVIRON']

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

    #PAGINAÇÃO SE CASO ESTIVER VAZIO PARAR O LOOP
    if dados.empty:
     print('Pagina Vazia!')
     break


    #FORMATAÇÃO DE COLUNAS (TRAZER APENAS O NECESSÁRIO)
      
    #dados['formulary.created_at'] = pd.to_datetime(dados['formulary.created_at'])
    colunas =['event_id','question.id','section.name','author.id','question.name','content_format', 
              'comment','attachments','file_url','section.position','latitude','longitude','answered_at','created_at',
              'id','item_id', 'has_media','collection_item_id','last_modified_at','event_created_at',
              'event_finished_at', 'event_status','author.full_name','author.email', 'author.username','formulary.name','formulary.deleted_at',
              'formulary.created_at','section.id','section.description','section.deleted_at','section.created_at','question.position',
              'question.question_type','question.parent_question_id','question.score_weight','question.created_at','question.deleted_at',
              'option.id','option.name','option.position', 'option.created_at', 'option.deleted_at', 'formulary.id', 'author_id','score_weight', 'formulary_group.id']
    formatacao = dados.loc[:, colunas]
    

    #SALVANDO OS ANEXOS ADICIONADOS AO APLICATIVO PELO USUÁRIO NO SERVIDOR LOCAL
    for index, row in formatacao.iterrows():
        try:
            url = formatacao.at[index,'attachments'] = row['attachments'][0]['remote_url']
            urllib.request.urlretrieve(url, "N:/Temp_duração_7_dias/Leo/"+str(row['question.id'])+".jpeg")
            print("Imagem salva!")
        except:
            print('Imagem não encontrada')

    #TRATAMENTO DE ANEXOS E NULOS
    for index, row in formatacao.iterrows():
        if row["attachments"] and row["attachments"][0]:
            formatacao.at[index,'attachments'] = row['attachments'] #'uspedra.com.br/' + str(row['id'] + '.jpeg') para redirecionamento
        else:
            formatacao.at[index,'attachments'] = 'https://tinypic.host/images/2023/02/24/no_image.jpg' #+ 'jllopes.png'

    #TRATAMENTO DE PESO NULO
    for index, row in formatacao.iterrows():
        if row["score_weight"] is None:
            formatacao.at[index, "score_weight"] = "0"
        else:
            formatacao.at[index, "score_weight"] = row["score_weight"]

    # CRIANDO UMA LISTA DE TUPLAS COM OS DADOS DO DATAFRAME
    values = [tuple(str(x) for x in row) for row in formatacao.values]

    #INICIANDO CONEXÃO COM BANCO DE DADOS ORACLE E INSERÇÃO DE DADOS
    connection = cx_Oracle.connect(user=config['DATABASE']['USER'], password=config['DATABASE']['PASSWORD'], dsn=config['DATABASE']['DSN'])
    cursor = connection.cursor()
    try:
        cursor.executemany("""INSERT INTO weck_dadoentrada (ID_DADO_ENTRADA, EVENTO, QUESTAO, NM_SECAO, ID_USER, NM_QUESTAO, RESPOSTAS, COMENTARIO, ANEXOS, URL, ORDEM_SECAO, LATITUDE, LONGITUDE, DATA_RESPOSTA, DATA_CRIACAO, ID_RESPOSTA, FORM_PREENCHIDO, MEDIA, COLECAO, ULT_MODIF, DATA_INICIO_EVENTO, DATA_FIM_EVENTO, STATUS_EVENTO, NOME_USER, EMAIL_USER, MATR_USER, NM_FORM, DATA_FORM_DELETADO, DATA_FORM_CRIACAO, ID_SECAO, DS_SECAO, DATA_DELETE_SECAO, DATA_CRIACAO_SECAO, ORDEM_QUESTAO, TIPO_QUESTAO, QUESTAO_FILHO,PESO_QUESTAO, CRIACAO_QUESTAO, DELETE_QUESTAO, OPCAO, NM_OPCAO, ORDEM_OPCAO, CRIACAO_OPCAO, DELETE_OPCAO, ID_FORM, CRIADOR_OBJETO, QT_PESO, ID_PROJETO) VALUES ('', :0, :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, :35, :36, :37, :38, :39, :40, :41, :42, :43, :44, :45, :46)""", values)
        connection.commit()
        print('Dados inseridos no banco Oracle com sucesso!')
    except cx_Oracle.DatabaseError as e:
        print('Error:', e)
        connection.rollback()

    #Paginação
    page+= 1

#ADICIONAR DADOS NO EXCELL, ATIVAR = TRUE, DESATIVAR = FALSE. 
ex = False
if ex == True:
    dataframe = pd.DataFrame(formatacao)
    dataframe.to_excel("DadosWeCheck.xlsx")
    print('Dados inseridos no Excell com sucesso!')
else:
    print('Inserção de Dados no Excell Desativado!')
