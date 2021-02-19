# INSTRUMENTS_AUX - versão  1.0
# time: 17.387s

# ---------------------------------------------------------- Descrição geral ------------------------------------------------------------------ #
# Recolher informações para tabelas auxiiares


# ---------------------------------------------------------- Bibliotecas utilizadas ----------------------------------------------------------- #
import pandas as pd
import sqlalchemy as sa
import numpy as np
import json
import urllib

# ---------------------------------------------------------- API/SQL settings ------------------------------------------------------------------ #
# Dados de autenticação para acesso a API e servidor SQL
base_url = 'http://alpha.onepartners.com.br'
api_user = 'oneapi'
api_pass = 'one2021'
params = urllib.parse.quote_plus("DRIVER={SQL Server};SERVER=spdbprd02;DATABASE=MFO_dev")
engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, echo=False)

# ---------------------------------------------------------- Funções auxiliares ----------------------------------------------------------- #
# Solicida dados de APIs internas - feitas pelo Alpha - e retorna no fomato json/ dicionario
def get_api_data(module, method, params):
	import requests, requests.auth
	url = '%s/api/2/sync/%s/%s' % (base_url, module, method)
	auth = requests.auth.HTTPBasicAuth(api_user, api_pass)
	r = requests.post(url, json=params, auth=auth)
	try:
		r.raise_for_status()
	except requests.exceptions.HTTPError as e:
		error_msg = "Error: {} - Description: {}".format(e, r.text)
		return error_msg
	return r.json()

# Solicida dados de APIs de cadastro - não fornecidas pelo Alpha - e retorna no fomato json/ dicionario
def get_api_cadastro(module, method):
	import requests, requests.auth
	url = '%s/api/2/rest/%s/%s/' % (base_url, module, method)
	payload={}
	headers = {'Authorization': 'Basic b25lYXBpOm9uZTIwMjE='}
	r = requests.request("POST", url, headers=headers, data=payload)
	return r.json()

#--------------------------------------------------------------------------------------------------------------------------------------------------

instrument_types = get_api_data('instruments', 'get_types',{})
instrument_types = pd.DataFrame.from_dict(instrument_types, orient='index', dtype=None, columns=None)	#transforma o dicionario em dataframe
instrument_types = instrument_types.reset_index(drop = True) 										# refaz o index do dataframe, començando de 0 e tran
instrument_types.to_sql(name='instrument_types_info',con=engine, if_exists ='replace',index=False)

#Instruments_class
instrument_classes = get_api_cadastro('instruments', 'instrument_class')
instrument_classes = pd.DataFrame.from_dict(instrument_classes, orient = 'columns', dtype=None, columns=None)	#transforma o dicionario em dataframa
instrument_classes.drop(['children', 'instrument_set','is_active'], axis=1, inplace=True)					#exclui colunas descnecessárias
instrument_classes.rename(columns={'name':'class_name'}, inplace=True)										#renomear a coluna com o nome da classe para "class_name"
instrument_classes.to_sql(name='instrument_classes_info',con = engine, if_exists = 'replace',index=False)
