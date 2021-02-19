# INSTRUMENTS_DIMENTIONS - versão  1.0
# aquisição do dados sobre os instrumentos que compõem a base
# time: 55.875s.

# ---------------------------------------------------------- Descrição geral ------------------------------------------------------------------ #
# Recolher informações para tabela de dimensões do módulo Instrument
# Compoem o módulo instruments: instrumentos cadastrados padrão do Alpha, fundos insternos e externos e passivo (clientes)


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
#	 API 'get_instruments' para listar os ids dos instrumentos cadastrados no sistema
instruments = get_api_data('instruments', 'get_instruments',{ })
instruments_ids = list(instruments.keys())	 # trasforma resultado em uma Lista com ids dos instrumentos cadastrodos no Alpha

#--------------------------------------------------------------------------------------------------------------------------------------------------
#	 API 'get_instrument_details' retorna dados dos instrumentos cadastrados
# include_instrument_base_info = True para incluir informações especificas de acordo com o tipo de instrumento
instrument_info_aux = get_api_data('instruments', 'get_instrument_details',{'ids' : instruments_ids, 'include_instrument_base_info' : True})

# transforma dicionario em DataFrame com as colunas de dados necessários
instrument_info_aux = pd.DataFrame.from_dict(instrument_info_aux, orient='index', dtype=None,
											columns=['id', 'name', 'type_id', 'is_active', 'symbol', 'isin_code',
										       'bloomberg_pricing_source', 'bloomberg_ticker', 'cusip_code',
										       'sedol_code', 'issuer_id', 'market_id', 'currency_id', 'price_divisor',
										       'round_lot', 'allocation_lot', 'settlement_days', 'bloomberg_formula',
										       'liquidity', 'instrument_class_id', 'stock_type', 'base_fund_id',
										       'base_fund_classification', 'tax_classification', 'maturity_date',
										       'liquidity_days', 'cetip_code', 'issue_date', 'index_id',
										       'uses_agreement', 'face_value', 'original_return_rate',
										       'index_relative_return_rate', 'accrual_type',
										       'compound_only_after_lockup', 'coupon_business_days_dynamic',
										       'accrual_day_count', 'payment_frequency', 'index_variation_behavior',
										       'redemption_behavior', 'principal_lockup_months', 'rate_lockup_months',
										       'penalty_fee', 'other_fees_percent', 'other_fees_financial',
										       'risk_rating', 'fixed_duration_per_day', 'coupon_offset',
										       'is_convertible', 'bond_type_id', 'bond_type_name', 'is_governmental',
										       'selic_code', 'overnight_price', 'return_date', 'is_reverse',
										       'pricing_rule', 'market_price_max_age'])
instrument_info_aux = instrument_info_aux.reset_index(drop=True)


# Renomear as colunas para que seja possivel identificar a mesma informação em outras tabelas (mesma informação = mesmo nome)
instrument_info_aux.rename(columns = {"type_id": "type", "base_fund_id": "base_fund","instrument_class_id":"instrument_class",'bond_type_id':'bond_type'}, inplace=True)

#a coluna base_fund possui o fund_id associado ao instrument_id de todos os fundos cadastrados

#--------------------------------------------------------------------------------------------------------------------------------------------------
#		APIs para aquisição de dados de fundos cadastrados
# Fundos internos e externos possuem informações extras que não estao no módulo Alpha 'instruments'

fund_internals_aux = get_api_cadastro('funds', 'fund')  # Obtem-se informações de fundos internos cadastrados
fund_internals_aux = pd.DataFrame.from_dict(fund_internals_aux, orient='columns', dtype=None,columns= None)  # Transforma o dicionario em DataFrame
# Filtra-se as colunas do Dataframe os dados com informações necessarias
fund_internals_aux = fund_internals_aux[['id', 'is_internal', 'qualified_investors_only',
								'professional_investors_only', 'closed_to_new_investor',
								'classification', 'administrator', 'legal_id_type',
								'legal_name', 'start_date', 'external_start_date',
								'end_date', 'manager',  'secondary_currency',
								'holiday_type', 'min_balance', 'min_initial_subscription',
								'min_additional_subscription', 'erisa_percentage_limit',
								'subscription_limit_time', 'redemption_limit_time',
								'is_exclusive', 'master_fund', 'cetip',
								'distributor', 'anbima_classification', 'requires_capital_commitment',
								'management_area', 'legal_entity_type', 'is_managerial',
								'is_opening_fund', 'liabilities_mode', 'is_investment_vehicle',
								'cblc_code', 'maximum_administration_fee', 'dividends_passthrough',
								'initial_navps', 'shareholder']]



fund_externals_aux = get_api_cadastro('funds', 'external_fund')  # Obtem-se informações de fundos externos cadastrados

fund_externals_aux = pd.DataFrame.from_dict(fund_externals_aux, orient='columns', dtype=None, columns=None)   # Transforma o dicionario em DataFrame

fund_externals_aux = fund_externals_aux[['id', 'is_internal', 'qualified_investors_only',
								'professional_investors_only', 'closed_to_new_investor',
								'classification', 'administrator', 'legal_id_type',
								'legal_name',  'start_date', 'external_start_date',
								'end_date', 'manager',  'secondary_currency',
								'holiday_type', 'min_balance', 'min_initial_subscription',
								'min_additional_subscription', 'erisa_percentage_limit',
								'subscription_limit_time', 'redemption_limit_time',
								'is_exclusive',  'master_fund', 'cetip',
								'distributor', 'anbima_classification', 'requires_capital_commitment',
								'is_money_market_sweep', 'days_for_average_maturity']]

# Junção dos DataFrames em uma unic tabela que consolida as informações extras de fundos cadastrados (internos e externos)
funds_aux = pd.concat([fund_internals_aux,fund_externals_aux], axis=0)
# Renomear colunas que contem a mesma informação para utilização das funções concat/ merge
funds_aux.rename(columns={'classification':'fund_class','id':'base_fund'}, inplace = True)

funds_aux['fund_id'] = funds_aux['base_fund'] 		# Copia as insformações da coluna base_fund para outra coluna "fund_id", para que essa informação não seja perdida
funds_aux.set_index('base_fund', inplace = True)	# a coluna "base_fund" será o indice da tabela de fundos para utilizarmos a função 'map'

#--------------------------------------------------------------------------------------------------------------------------------------------------
#	Para unir as informações extras de fundos a tabela principal de instrumentos, precisamos correlacionar um instrument_id a cada fund_id
# A tabela 'funds' não possue informação sobre o instrument_id correspondente de cada fundo,
# obtemos essa relação na em 'instrument_info', com as colunas 'id'(correspondente ao instrument_id) e 'base_fund' (correspondente ao fund_id)

fund_id_instrument_id_aux = instrument_info_aux.loc[:, ('id', 'base_fund')]						#Tabela que associa a cada 'fund_id' seu respectivo 'instrument_id'
fund_id_instrument_id_aux.dropna(axis=0, how='any', thresh=None, subset=None, inplace=True)		#exclui linhas com 'null'
fund_id_instrument_id_aux.set_index('base_fund', inplace=True)									# estabelecemos a coluna 'base_fund' como indice, tal como na tabela anterior 'funds'



# as tabelas 'funds' e 'fund_id_instrument_id' possuem o mesmo indice sendo possivel utilizar a função 'map'

# Cria-se uma coluna na tabela 'funds_aux' de nome 'id' onde coloca-se o respectivo instrument_id
# a função map identica que ambas as tabelas possuem o mesmo index e assim
funds_aux['id'] = pd.Series(fund_id_instrument_id_aux.index, index=fund_id_instrument_id_aux.index).map(fund_id_instrument_id_aux['id'])


# Cria-se a tabela instruments_dim_aux com a junção de instrument_info_aux e funds_aux
#
instruments_dim_aux = instrument_info_aux.merge(funds_aux, on ='id', how='outer')

#-------------------------------------------------------------------------------------------------------------------------------------------------------------
# O módulo instruments tem informações de passivo: esses são considerados como instrumentos
# API get_shareholder_info para listar os cotistas cadastrados

shareholder_info_aux = get_api_data('liabilities', 'get_shareholder_info',{})
shareholder_info_aux = pd.DataFrame.from_dict(shareholder_info_aux, orient='index', dtype=None,
						columns=['id','legal_id','full_name','currency_id','type','creation_date','fund_id','fund_name'])  #Transforma o dicionario em DataFrame com as colunas necessarias
# renomear colunas para seguir com padrão anterior e facilitar o merge com a tabela 'instruments'
shareholder_info_aux.rename(columns={'id' : 'shareholder',
								'legal_id' : 'symbol',
								'full_name' : 'name',
								'type' : 'shareholder_type',
								'creation_date' : 'start_date',
								'fund_id' : 'base_fund'}, inplace = True)

# queremos apenas shareholder do tipo pessoa física ou pessoa jurídica (tipo 1 e 3)
shareholder_info_aux.drop(shareholder_info_aux[shareholder_info_aux['shareholder_type'] == 2].index, inplace=True)
shareholder_info_aux.drop(shareholder_info_aux[shareholder_info_aux['shareholder_type'] == 4].index, inplace=True)

# estabelecemos um id para o tipo de instrumento: cliente. Nos módulos Alpha essa categoria não existe
shareholder_info_aux['type'] = 200

# Cria-se um instrument_id para cada cliente com a junção de : '200' +id do shareholder + tipo de shareholder
shareholder_info_aux['id'] = shareholder_info_aux['type'].astype(str) + shareholder_info_aux['shareholder'].astype(str) + shareholder_info_aux['shareholder_type'].replace(np.nan, 0).astype(str)
# para concatenar as informações das colunas precisamos transformá-las em string, depois retornamos para o tipo 'int'
shareholder_info_aux['id'] = shareholder_info_aux['id'].astype(float)
shareholder_info_aux['id'] = shareholder_info_aux['id'].astype(int)

#-------------------------------------------------------------------------------------------------------------------------------------------------------------
# final
# juntamos as informações de instrumentos com as informações de passivo - agora tratados como instruments também
instruments_dim = instruments_dim_aux.merge(shareholder_info_aux, how='outer')
instruments_dim.to_sql(name='instrument_dim', con=engine, if_exists='replace',index=False) #input na base de dados
