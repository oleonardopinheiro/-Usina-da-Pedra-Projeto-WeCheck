# Usina da Pedra - Projeto WeCheck
Projeto de Checklists, onde participo e ajudo na organização, estruturação e criação do projeto, usamos a plataforma WeCheck.

<b> PASSOS PARA O DESENVOLVIMENTO DO PROJETO WECHECK <br>
  
  VERSÃO 1.0
  
  <strong> Inicio do desenvolvimento </strong> <br>
  Entendimento das funcionalidade<br>
  Primeiros passos de registros checklist<br>
  Estruturação de perguntas<br>
  Organização de opções<br>
  Formatação de perguntas para melhor análise com graficos<br>
  Estruturação do Banco de Dados (Oracle) <br>
  Criação do WeCheck<br>
  1° Passo para Integração via API usando Python (Excell)<br>
  2° Passo para Integração via API usando Python conexão com Banco de Dados Oracle<br>
  Normalização de colunas e tratamentos de retornos usando dataframes via Python<br>
  Graficos para melhor análise de qualidade usando Tableau<br>
  
    
Esse script python, está obtendo dados da API do wecheck, verifica se a pagina está vazia, caso não estiver ele irá colocar os dados em um dataframe e normalizar as colunas, após isso irá formatar as datas e também traz as url dos anexos, por fim faz uma lista de tupla com os dados, confere se os dados já existe no banco de dados e insere caso ainda não exista, caso exista irá ignorar.

Script com:

Api + Paginação Dataframe + Normalização Loc de Colunas Formatação de Datas Insert Oracle com verificação se já existe o dado.

Criado por Leonardo Pinheiro - PedraAgroIndustrial 2023!
