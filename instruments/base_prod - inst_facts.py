# INSTRUMENTS_FACTS - versão  1.0
# time: 4195.257s

# ---------------------------------------------------------- Descrição geral ------------------------------------------------------------------ #
# Recolher informações para tabela de fatos do módulo Instrument
# Compoem o módulo instruments: instrumentos cadastrados padrão do Alpha, fundos insternos e externos e passivo (clientes)
# O programa não suporta a leitura de muitos dados ao mesmo tempo, ou seja, não é viável enviar todas as datas e todos os instrumentos - MemoryError:
# para isso dividimos o range de datas e o lista de instruments em subsets e enviamos parâmetros em um loop


# Facts de clientes: consideramos as variações de posições dos shareholdes com o tempo

# próximos passos: update dos dados de forma rápida
# junção de shareholders facts com instrument_facts
# ---------------------------------------------------------- Bibliotecas utilizadas ----------------------------------------------------------- #
import pandas as pd
import pprint as pp
import pyodbc
from datetime import datetime, timedelta, date
import sqlalchemy as sa
import numpy as np
import urllib

# ---------------------------------------------------------- API/SQL settings ------------------------------------------------------------------ #
# Dados de autenticação para acesso a API e servidor SQL
base_url = 'http://alpha.onepartners.com.br'
api_user = 'oneapi'
api_pass = 'one2021'
params = urllib.parse.quote_plus("DRIVER={SQL Server};SERVER=spdbprd02;DATABASE=MFO_dev")
engine = sa.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, echo=False)
conn = pyodbc.connect('Driver={SQL Server};'		#conn é a conexao feita com a base de dados para ler as tabelas ja existentes
                      'Server=spdbprd02;'
                      'Database=MFO_dev;'
                      'Trusted_Connection=yes;')

# ---------------------------------------------------------- Funções auxiliares ----------------------------------------------------------- #

# Solicida dados de APIs internas - feitas pelo Alpha - e retorna no fomato json/ diciánario
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

def get_api_cadastro(module, method):
	import requests, requests.auth
	url = '%s/api/2/rest/%s/%s/' % (base_url, module, method)
	payload={}
	headers = { 'Authorization': 'Basic b25lYXBpOk9uZUAyMDE4IUA='}
	response = requests.request("POST", url, headers=headers, data=payload)
	response.json()
	return response.json()

# divide_chunk divide uma lista em listas menores (subsets) - sera utilizado para separação dos instruments_ids
def divide_chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


#--------------------------------------------------------------------------------------------------------------------------------------------------
# com a tabela de dimensoes ja na base de dados, listamos os instruments_ids
# type <> 200 pora excluir os instrumentos do tipo 'passivo/clientes'
cursor = conn.cursor()
sql = "select id from instrument_dim where type <> 200"
instrument_ids = pd.read_sql_query(sql, con=conn)['id'].tolist() #le a query e a tranforma em tabela
instrument_ids_chunk = list(divide_chunks(instrument_ids,100))   #subsets dos ids

# cria-se pares de datas (inicio e final do ms)
date_list = pd.date_range(start='2017-01-01', end='2021-01-31', freq='M')
date_list_end = date_list.strftime('%Y-%m-%d').tolist() # lista com ultimo dia do mÊs
date_list = pd.date_range(start='2017-01-01', end='2021-01-31', freq='MS')
date_list_ini = date_list.strftime('%Y-%m-%d').tolist() #lista com primeiro dia do mês

# lista de tuples: inicio e final de cada mês  desde de 2017
date_range = [(date_list_ini[i], date_list_end[i]) for i in range(0, len(date_list_end))]


for datas in date_range: 			#para cada par de datas (inicio e fim dos meses)
	for list in instrument_ids_chunk: # tem-se um subset de instruments (100)
		# enviamos 100 ids e um mes como parametros para API
		# 'show_month_last_price' : False - queremos dados de todos os dias do mês
		instrument_facts_aux = get_api_data('instruments','get_prices_for_display',{'instrument_ids' : list, 'start_date' : datas[0],'end_date' : datas[1], 'show_month_last_price' : False})
		instrument_facts_aux = pd.json_normalize(data = instrument_facts_aux, record_path=['prices'])
		# algumas combinações de parametros não retornam dados (empty), checamos isso antes de prosseguir
		if not prices_for_display.empty:
			# as seguintes colunas não existiam antes de 2017, para evitar 'KeyError' (quando a coluna não exite), cria-se a coluna com o valor 0
			check = 'extra_values.yield_value' in instrument_facts_aux.columns
			if check == False:
				instrument_facts_aux['extra_values.yield_value'] = 0
			check = 'extra_values.accrued_interest' in instrument_facts_aux.columns
			if check == False:
				instrument_facts_aux['extra_values.accrued_interest'] = 0
			check = 'extra_values.exposure_value' in instrument_facts_aux.columns
			if check == False:
				instrument_facts_aux['extra_values.exposure_value'] = 0
			check = 'vol' in instrument_facts_aux.columns
			if check == False:
				instrument_facts_aux['vol'] = 0
			check = 'settlement_price' in instrument_facts_aux.columns
			if check == False:
				instrument_facts_aux['settlement_price'] = 0
			check = 'settlement_value_per_unit' in instrument_facts_aux.columns
			if check == False:
				instrument_facts_aux['settlement_value_per_unit'] = 0
			check = 'duration' in instrument_facts_aux.columns
			if check == False:
				instrument_facts_aux['duration'] = 0
			check = 'delta' in instrument_facts_aux.columns
			if check == False:
				instrument_facts_aux['delta'] = 0
			check = 'trade_volume' in instrument_facts_aux.columns
			if check == False:
				instrument_facts_aux['trade_volume'] = 0
			check = 'extra_values.total_traded_shares' in prices_for_display.columns
			if check == False:
				instrument_facts_aux['extra_values.total_traded_shares'] = 0

            # filtro dos dados com as colunas finais
			instrument_facts = instrument_facts_aux[['date', 'instrument_id', 'agreement_id','nav_unit_value', 'market_cap', 'nav',
													'shares_outstanding','rate', 'settlement_price',
													'delta', 'vol', 'trade_volume', 'duration','price_variation',
													'settlement_value_per_unit','extra_values.total_traded_shares',
													'extra_values.exposure_value','modified_on', 'opening_value',
													'price_source', 'administrator_id', 'corporate_actions_factor',
													'is_latest_entry','corporate_actions_adjusted_value',
													'instrument_name','extra_values.yield_value', 'extra_values.accrued_interest']]


			# alguns tipos e tamanhos de dados não são aceitos pelo SQL
			instrument_facts = instrument_facts.applymap(str)      # para evitar TypeError de variaveis, convertemos todos os valores para tipo string.
            print(instrument_facts)                                # print para pré-vizualizar a tabele

            #inputamos os dados do loop atual na base de dados

            #menter essa linha comentada para que não altera a tabela da base final
			#instrument_facts.to_sql(name='instrument_facts',con=engine,if_exists='append',index=False, chunksize=100)


# ----------------------------------------------------- Instrument facts - clientes -------------------------------------------------------- #
# time: 4724.305s

sql = "select id, shareholder from instrument_dim where type = 200"
shareholder_ids = pd.read_sql_query(sql, con=conn)['shareholder'].tolist() #lista com os shareholdes e instrument ids, adquirida da tabela instrument_dim, considerando o tipo de instrumentos = 200

shareholder_ids_instrument_ids = pd.read_sql_query(sql, con=conn)
shareholder_ids_instrument_ids.set_index('shareholder', inplace=True)

date_range = pd.date_range(start='2017-12-21', end='2021-01-31')    #periodo de datas avaliado
shareholder_facts_aux = pd.DataFrame()                              #tabela vazia onde serão colocados os dados

for date in date_range:
	date = date.strftime('%Y-%m-%d')
    #considera-se 'facts' para o instrumento do tipo passivo, as posições de passivo - usa-se a API get_positions e o resultado é tranformado em dataframe
	shareholder_position_aux = get_api_data('liabilities','get_position',{'shareholder_ids':shareholder_ids,'date':date,'estimate_values':True,'use_converting_transactions_for_position':True,'show_percentage_of_aum':True})
	shareholder_position_aux = pd.json_normalize(data = shareholder_position_aux, record_path = ['results'])
	shareholder_facts_aux = shareholder_facts_aux.append(shareholder_position_aux)        #append dos dados do loop atual na tabela
	print(shareholder_facts_aux)                                                          #print para pre-vizualizar a tabela

shareholder_facts_aux.drop(['shareholder_group_ids', 'clients'], axis = 1, inplace = True) #exclui
shareholder_facts_aux.set_index('shareholder_id', inplace=True)
shareholder_facts_aux['instrument_id'] = pd.Series(shareholder_ids_instrument_ids.index, index = shareholder_ids_instrument_ids.index).map(shareholder_ids_instrument_ids['id'])

shareholder_facts = shareholder_facts_aux[['date','instrument_id','shareholder_full_name','fund_id', 'shares','share_percentage', 'gross_value', 'committed_value',
										 'shares_without_transactions', 'original_amortization_factor', 'original_nav', 'current_nav',
										 'proffitability_percentage', 'percentage_of_aum', 'conversion_navps', 'display_currency_navps', 'current_exchange_rate',
								         'display_currency_id', 'public_participation', 'public_shares','public_balance', 'proffitability']]

shareholder_facts.rename(columns={'display_currency_id' : 'currency_id',
								  'shareholder_full_name' : 'name'}, inplace = True)

#menter essa linha comentada para que não altera a tabela da base final
#shareholder_facts.to_sql(name='shareholder_facts',con=engine, if_exists='replace',index=False)
