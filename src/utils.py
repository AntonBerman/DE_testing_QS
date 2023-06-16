### Main classes for processing or retreiving data from data sources
from datetime import datetime
from time import sleep
import sqlite3 as db
import pandas as pd
import requests
import os


class DataFromCSV:
    """Class that can process data from CSV files."""
    
    
    def __init__(self,
                csv_path: str  = '',
                index_col_use: bool = False, 
                ):
        """
        :csv_path: the path of CSV file
        """

        self.csv_path = csv_path
        self.index_col_use = index_col_use
        self.main_instance = False
        self.msg = ''
        self.main_columns = {
            'bars_transactions':['datetime','drink_id','amount'],
            'stocks_info': ['glass_type_id','stock','bars_id'],
        }
        self._apply_main_preprocessing()
    
    
    def _checking_separator(self, df: object = None) -> bool:
        """
        trying to check separator between ',' and  '\t'
        if True - separaator correct
        """

        return not len(list(df)) < 2
    
    
    def _checking_header(self, df_columns: list = True) -> bool:
        """
        trying to check header by assuming that there is not digits in fields names
        if True - header exist
        """

        return not any([x.isdigit() for x in ''.join(df_columns)])
    
    
    def _apply_main_preprocessing(self):
        """
        trying to read file and parse to dataframe
        Assumption: 
        """
        
        index_col = None if self.index_col_use else 0
        go_on = True
        if not os.path.exists(self.csv_path):
            go_on = False
            self.msg = f'Path {self.csv_path} not exists' 
            self.df_main = pd.DataFrame()
        
        ### try to define separator ------------------------------
        for sep in [',', '\t']:
            if not go_on: continue
            
            df = pd.read_csv(self.csv_path, sep = sep, index_col = index_col, nrows=1)
            
            if self._checking_separator(df): break
        
        ### try to check if exist header -------------------------
        if go_on:
            header_exist = self._checking_header(list(df))
            header = 0 if header_exist else None
            
            self.df_main = (
                pd
                .read_csv(self.csv_path, sep = sep, index_col = index_col, header = header)
                .reset_index(drop=True)
            )
            self.df_main = self.df_main[list(self.df_main)[:3]]
            
            self.msg = f'Got next table from path - {self.df_main.shape}'
        
        
        self.main_instance = not self.df_main.empty      
              
    

    def clean_df_transactions(self) -> bool:
        """
        Assemble main instance of data
        """
        
        self.feature = 'bars_transactions'
        if not self.main_instance:
            return False
        elif len(self.df_main.columns) < 3:
            self.msg = f'Absent some fields in table'
            return False
        else:
            self.df_main.columns = self.main_columns[self.feature]
            self.df_main['datetime'] = pd.to_datetime(self.df_main['datetime'], errors = 'coerce')
            self.df_main['amount'] = (
                self.df_main['amount']
                .astype(str)
                .apply(lambda x: x.split()[0])
                .astype(float)
            )
            self.msg = f'Got next table for {self.feature} - {self.df_main.shape}'
            return not self.df_main.empty
    
    
    def clean_df_stocks(self) -> bool:
        """
        Assemble main instance of data
        """
        
        self.feature = 'stocks_info'
        if not self.main_instance:
            return False
        elif len(self.df_main.columns) < 3:
            self.msg = f'Absent some fields in table'
            return False
        else:
            self.df_main.columns = self.main_columns[self.feature]
            self.df_main['stock'] = (
                self.df_main['stock']
                .astype(str)
                .apply(lambda x: x.split()[0])
                .astype(int)
            )
            self.msg = f'Got next table for {self.feature} - {self.df_main.shape}'
            return not self.df_main.empty

        

class DataToFromDB:
    """Class that can process data to DB"""
    
    
    
    def __init__(self,
                db_name: object  = None
                ):
        """
        :csv_path: the path of CSV file
        """

        self.db_name = db_name
        self.connector = None
        self.maps = {}
        self.msg = ''
        self.sql_queries = {
            'sql_map': """INSERT INTO {table} (name) values(?)""",
            'sql_bars_transactions': """INSERT INTO {table} (datetime, drink_id, amount, bars_id) values(?,?,?,?)""",
            'sql_stocks': """INSERT INTO {table} (glass_type_id, stock, bars_id, modifiedon) values(?,?,?,?)""",
            'sql_select_map': """SELECT name,id FROM {table};""",
            'sql_select_name': """SELECT name FROM {table};""",
            'sql_update_drinks': """INSERT INTO drinks (id, name, glass_type_id)
                                       VALUES(?, ?, ?) 
                                       ON CONFLICT(id) 
                                       DO UPDATE SET glass_type_id=excluded.glass_type_id;""",
            'sql_select_new_drinks': """SELECT id
                                            , name
                                            , glass_type_id 
                                        FROM drinks 
                                        WHERE glass_type_id is NULL;""",
        }
        self.tables = {
            'bars_id':'bars',
            'drink_id':'drinks',
            'glass_type_id':'glasstype',
            'bars_transactions':'bars_transactions',
            'stocks':'stocks',
        }
    
    
    def connect_db(self):
        """
        set up connection to db
        """
        
        try:
            self.connector = db.connect(self.db_name)
        except Exception as err:
            print(f"Problem with establishing connection: {err}")
        
    
    def close_db(self):
        """
        close connection to DB
        """
        
        if self.connector: self.connector.close()
        
    
    def query_to_df(self, query: str = ''):
        """
        using pd.read_sql_query for retreiving data from DB
        """
        
        self.connect_db()
        self.df = None
        try:
            self.df = pd.read_sql_query(query, self.connector)
        except Exception as err:
            print(f"Problem with retreiving data from DB: {err}")
        
        self.close_db()
        
    
    def select_maps(self, kind: str = ''):
        """
        using pd.read_sql_query for retreiving data from DB
        """
        
        self.connect_db()
        if kind not in self.maps.keys(): 
            self.maps[kind] = {}
        try:
            df = pd.read_sql_query(self.sql_queries['sql_select_map'].format(**{'table':self.tables[kind]}), self.connector)
            self.maps[kind] = dict(df.values)
        except Exception as err:
            print(f"Problem with retreiving data from DB for map: {kind}: {err}")
        
        self.close_db()
        
    
    def insert_maps(self, values: list = [], table: str = '') -> bool:
        """
        inserting data to DB: new bars, drinks, glasstype
        """
        
        if len(values) == 0: return True
        
        load = False
        self.query_to_df(self.sql_queries['sql_select_name'].format(**{'table':table}))
        if isinstance(self.df, pd.DataFrame):
            self.connect_db()
            try:
                self.connector.executemany(self.sql_queries['sql_map'].format(**{'table':table}),\
                                         [(x,) for x in values if x not in self.df['name'].tolist()])
                self.connector.commit()
                load = True
            except Exception as err:
                print(f"Problem with inserting data to DB: {table}:  {err}")
        
            self.close_db()
        
        return load
    
    
    def insert_bars_transactions(self, df: object = None, bar_name: str = '') -> bool:
        """
        inserting data to DB: new bars transactions
        """
        
        load = False
        
        ## inserting new bars if exist in current data
        self.insert_maps([bar_name], self.tables['bars_id'])
        ### getting map of bars
        self.select_maps('bars_id')
        df['bars_id'] = self.maps['bars_id'][bar_name]        
        
        ### inserting new drinks if exist in current data
        self.insert_maps(df['drink_id'].unique(), self.tables['drink_id'])
        ### getting map of drinks
        #self.query_to_df(self.sql_queries['sql_select_map'].format(**{'table':self.tables['drink_id']}))
        #if isinstance(self.df, pd.DataFrame):
        #    self.maps['drinks'] = dict(self.df.values)
        self.select_maps('drink_id')
        df['drink_id'] = df['drink_id'].map(self.maps['drink_id'])
                
        self.connect_db()
        try:
            self.connector.executemany(self.sql_queries['sql_bars_transactions'].format(**{'table':self.tables['bars_transactions']}),\
                                       df.apply(lambda x: (x['datetime'].isoformat(),x['drink_id'],x['amount'],x['bars_id'],), axis = 1).tolist())
            self.connector.commit()
            load = True
        except Exception as err:
            print(f"Problem with inserting data to DB: {self.tables['bars_transactions']}:  {err}")
        
        self.close_db()
        
        return load
    
    
    def insert_stocks(self, df: object = None) -> bool:
        """
        inserting data to DB: new stocks
        """
        
        load = False
        
        ## inserting new bars if exist in current data
        self.insert_maps(df['bars_id'].unique(), self.tables['bars_id'])
        ### getting map of bars
        self.select_maps('bars_id')
        df['modifiedon'] = datetime.now()
        df['bars_id'] = df['bars_id'].map(self.maps['bars_id'])
        
        ## inserting new glasstypeid if exist in current data
        self.insert_maps(df['glass_type_id'].unique(), self.tables['glass_type_id'])
        ### getting map of glasstype
        self.select_maps('glass_type_id')
        df['glass_type_id'] = df['glass_type_id'].map(self.maps['glass_type_id'])
        
        self.connect_db()
        try:
            self.connector.executemany(self.sql_queries['sql_stocks'].format(**{'table':self.tables['stocks']}),\
                                       df.apply(lambda x: (x['glass_type_id'],x['stock'],x['bars_id'],x['modifiedon'].isoformat(),), axis = 1).tolist())
            self.connector.commit()
            load = True
        except Exception as err:
            print(f"Problem with inserting data to DB: {self.tables['stocks']}:  {err}")
        
        self.close_db()
        
        return load
    
    
    def update_glass_type_id(self, df: object = None) -> bool:
        """
        inserting data to DB: new data in maps drinks
        """
        
        load = False
        
        self.connect_db()
        try:
            self.connector.executemany(self.sql_queries['sql_update_drinks'],\
                                       df.loc[~df['glass_type_id'].isnull()]\
                                         .apply(lambda x: (x['id'],x['name'],x['glass_type_id'],), axis = 1)\
                                         .tolist())
            self.connector.commit()
            load = True
        except Exception as err:
            print(f"Problem with upserting data to DB: {self.tables['drink_id']}:  {err}")
        
        self.close_db()
        
        return load
    
    
    def execute_query(self, query: str = '') -> bool:
        """
        executing SQL query without retreiving some data
        """
        
        load = False
        
        self.connect_db()
        try:
            self.connector.execute(query)
            self.connector.commit()
            load = True
        except Exception as err:
            print(f"Problem with query to DB: {query}:  {err}")
        
        self.close_db()
        
        return load


class DataFromAPI:
    """Class that can process data from API."""
    
    
    
    def __init__(self):
        """
        
        """

        self.msg = ''
        self.response = None
        self.urls = {
            'coctails_search':'https://www.thecocktaildb.com/api/json/v1/1/search.php?s={value}',
            'glass_search': 'https://www.thecocktaildb.com/api/json/v1/1/filter.php?g={value}',
        }
            
    
    def get_data_from_api(self, url: str = '') -> bool:
        """
        trying to retreive data from url
        """
        
        self.response, load = None, False
        
        try:
            self.response = requests.get(url)
            if self.response.status_code == 200:
                load = True
        except Exception as err:
            print(f"Problem with retreiving data from API: {err}")
        
        return load