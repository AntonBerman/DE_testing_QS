# Use this script to write the python needed to complete this task

from src.utils import *
from dotenv import load_dotenv

load_dotenv()

############ configs ---------------------------------

configs = {
    'DB_NAME': os.getenv('DB_NAME'),
    'TMP_DIR_TRANSACTIONS': os.getenv('TMP_DIR_TRANSACTIONS'),
    'TMP_DIR_STOCKS': os.getenv('TMP_DIR_STOCKS'),
}

map_files = {
    'budapest':'budapest',
    'london':'london',
    'new york':'ny',
}

########################################################


#### initialization instances
## DB instance
local_db = DataToFromDB(configs['DB_NAME'])

## API instance
dfa = DataFromAPI()

## CSV loader instance
#data_proc = DataFromCSV()

############ CREATE tables ------------------------------
for sql_scripts in os.listdir('queries'):
    if '.SQL' not in sql_scripts: continue
    with open(os.path.join('queries', sql_scripts), 'r') as f:
        sql_create = f.read()
    local_db.execute_query(sql_create)

############ inserting transactions ---------------------

for files in os.listdir(configs['TMP_DIR_TRANSACTIONS']):
    bars = [k for k,v in map_files.items() if v in files]
    print(f"Defined next file in transactions: {files}: {bars[0]}")
    data_proc = DataFromCSV(os.path.join(configs['TMP_DIR_TRANSACTIONS'], files))
    if not data_proc.main_instance: 
        print(data_proc.msg)
        continue
    if data_proc.clean_df_transactions():
        load = local_db.insert_bars_transactions(data_proc.df_main, bars[0])
        print(f"Uploaded transactions : {files}: {load}")
    else:
        print(data_proc.msg)
        continue
local_db.query_to_df("SELECT count(*) as total_count FROM bars_transactions")
print(f"Total values of transactions in DB: {local_db.df['total_count'].values[0]}")


########### inserting stocks info ------------------------

for files in os.listdir(configs['TMP_DIR_STOCKS']):
    print(f"Defined next file in stocks: {files}")
    data_proc = DataFromCSV(os.path.join(configs['TMP_DIR_STOCKS'], files), True)
    if not data_proc.main_instance: 
        print(data_proc.msg)
        continue
    if data_proc.clean_df_stocks():
        load = local_db.insert_stocks(data_proc.df_main)
        print(f"Uploaded stock : {files}: {load}")
    else:
        print(data_proc.msg)
        
local_db.query_to_df("SELECT count(*) as total_count FROM stocks")
print(f"Total values of stocks in DB: {local_db.df['total_count'].values[0]}")


########### Updating glasses id in table Drink
local_db.query_to_df(local_db.sql_queries['sql_select_new_drinks'])
if not local_db.df.empty:
    print(f"Starting updating glass_type_id through {local_db.df.shape[0]} values")
    df_map = local_db.df
    for i in df_map.index:
        if dfa.get_data_from_api(dfa.urls['coctails_search'].format(**{'value':df_map.loc[i, 'name'].replace(' ','_')})):
            df = pd.json_normalize(dfa.response.json()['drinks'])
            df_map.loc[i, 'glass_type_id'] = df.loc[df['strDrink']==df_map.loc[i, 'name'], 'strGlass'].values[0].lower()
        sleep(1)
    print(f"Received {df_map.loc[~df_map['glass_type_id'].isnull()].shape[0]} values from API")
    _ = local_db.insert_maps(df_map.loc[~df_map['glass_type_id'].isnull(),'glass_type_id'].unique(), local_db.tables['glass_type_id'])
    local_db.select_maps('glass_type_id')
    df_map.loc[~df_map['glass_type_id'].isnull(),'glass_type_id'] = \
        df_map.loc[~df_map['glass_type_id'].isnull(),'glass_type_id'].map(local_db.maps['glass_type_id'])
    load = local_db.update_glass_type_id(df_map)
    print(f"Updated glasses types for drinks: {load}")
else:
    print(f"All data - glass types updated")
    

########### Ananlitycal area -----------------------------------------------------------------------------------
## retreiving data from DB about current situation with current stock of glasses with last consumption by all bars and glasses
## next fields: bar_name, coctail_name, glass, current_stock_glasses, last_date_consumption

with open('poc_tables.SQL', 'r') as f:
    sql_poc = f.read()
local_db.query_to_df(sql_poc)

if local_db.df.empty:
    print(f"Problem with retreiving data about current statistic")
else:
    fname_general = f"general_stats_for_{datetime.now().strftime('%Y%m%d')}.xlsx"
    local_db.df.to_excel(fname_general)
    print(f"Got and Saved next general stats table: {local_db.df.shape}:{fname_general}")
    
    filename_procurement = f"need_for_procurement_for_{datetime.now().strftime('%Y%m%d')}.xlsx"
    local_db.df['saldo_glasses'] = local_db.df.apply(lambda x: x['current_stock_glasses'] - x['last_date_consumption'], axis = 1)
    procurement = local_db.df.loc[local_db.df['saldo_glasses'] < 0]\
                             .groupby(['bar_name','glass'], as_index = False)\
                             .agg({'saldo_glasses':sum})
    procurement.to_excel(filename_procurement)
    print(f"Got and Saved next procurement info : {procurement.shape}:{filename_procurement}")