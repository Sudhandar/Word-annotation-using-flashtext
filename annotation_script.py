import pandas as pd
from flashtext import KeywordProcessor
from sqlalchemy import create_engine

def data_from_db(db_name, query):
    
    eng = create_engine("mysql+pymysql://database_username:password@ip:port/"+db_name)
    sql_ct = query 
    data = pd.read_sql(sql_ct, eng)
    return data 
    
def preprocess_articles(db_name, query):
    
	base_data = data_from_db(db_name, query)
	base_data = pd.melt(base_data,id_vars = ['article_id'])
	base_data = base_data.dropna()
	base_data = base_data.drop_duplicates()
	base_data.columns = ['article_id', 'column_name', 'article_data']
	base_data['article_data'] = base_data['article_data'].str.strip()
    
	return base_data

def keywords(all_needles, case_flag = False):

	all_needles = all_needles[['source_id','needle']].drop_duplicates()
	all_needles['needle'] = all_needles['needle'].str.strip()
	gen_needles = all_needles['needle'].to_list()
	keyword_processor = KeywordProcessor(case_sensitive=case_flag)
	keyword_processor.add_keywords_from_list(gen_needles)
	return keyword_processor

def annotation(data,needles, case_flag):

	keyword_processor = keywords(needles, case_flag)
	data['found_terms'] = data['article_data'].apply( lambda x: keyword_processor.extract_keywords(x))
	data['len'] = data['found_terms'].str.len()
	data = data[data['len']>0]
	new_data = pd.DataFrame(data['found_terms'].values.tolist(), index = data.index)
	new_data = pd.merge(data[['article_id', 'column_name']], new_data, left_index = True, right_index = True, how = 'inner')
	new_data = new_data.melt(id_vars = ['article_id', 'column_name'])
	new_data.pop('variable')
	new_data = new_data.drop_duplicates(subset = ['article_id', 'column_name', 'value'])
	new_data = new_data.dropna()

	final = pd.merge(new_data, needles, left_on = 'value', right_on = 'needle', how = 'inner')
	final = final.drop_duplicates()
	return final

all_needles = pd.read_csv('needles_file.csv')
abbreviation_needles = pd.read_csv('abbr_needles.csv')
base_data = preprocess_articles(db_name, query)
base_data1 = base_data.copy()
case_insensitive_output = annotation(base_data, all_needles, case_flag = False)
case_sensitive_output = annotation(base_data1, abbreviation_needles, case_flag = True)
consolidated_output = case_insensitive_output.append(case_sensitive_output)
consolidated_output = consolidated_output.sort_values('article_id', ascending = True)
consolidated_output.to_csv('annotation_results.csv', index = False)