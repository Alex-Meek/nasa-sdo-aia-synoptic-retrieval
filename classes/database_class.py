import sys
sys.path.append("..")
import psycopg2
import logging

from contextlib import contextmanager
from functions.utils import quit_pogram_with_error_print_and_log

class PostgreSQL_Database:
    
    def __init__ (
        self,
        database_name: str,
        host_ip: str,
        username: str,
        password: str,
        port: str
        ):
        self.database_name    =    database_name
        self.host_ip          =    host_ip
        self.username         =    username
        self.password         =    password
        self.port             =    port
        self.check_is_connected()
        return None
    
    def __str__ (self):
        string = "Database: \n\n"
        string += f"\t Name: {self.database_name}\n"
        string += f"\t IP: {self.host_ip}\n"
        string += f"\t Port: {self.port}\n"
        string += f"\t Status: {self.is_connected}\n\n"
        return string

    def connect (self):
        try:
            logging.info(f"Connecting to database: {self}")
            self.connection = psycopg2.connect(
                database   =  self.database_name,
                host       =  self.host_ip,
                user       =  self.username,
                password   =  self.password,
                port       =  self.port
            )
            
        except Exception as e:
            quit_pogram_with_error_print_and_log(
                user_information = "Failed to connect to database: " + \
                    self + "with error: " + e + "\n"
            )
        
        self.check_is_connected()
        return True
    
    def check_is_connected (self):
        logging.info("Checking connection status.")
        if hasattr(self, 'connection'):
            if self.connection.status == psycopg2.extensions.STATUS_READY:
                logging.info("Connected.")
                self.is_connected = True
                return self.is_connected
        logging.info("Not connected.")
        self.is_connected = False
        return self.is_connected
    
    @contextmanager
    def activate_cursor (self):
        self.cursor = self.connection.cursor()
        try: 
            yield
        finally:
            self.cursor.close()
    
    
    def table_exists (self, table_name: str) -> bool:
        with self.activate_cursor():
            self.cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_name = %s
                )
                """,
                (table_name,),
            )
            return self.cursor.fetchone()[0]
    
    
    # def intialise_database ():
        
    #     return None
    
class PostgreSQL_Table:
    
    def __init__ (
        self, 
        database: PostgreSQL_Database,
        table_name: str,
        table_column_dict: dict
        ):
        self.database = database
        self.table_name = table_name
        self.table_column_dict = table_column_dict
        self.create_table()
        
    def create_table (self):
        
        if self._valid_table_column_dict_format():
            
            logging.info(f'Creating table \'{self.table_name}\'.')
            with self.database.activate_cursor():
                try:
                    self.build_table_creation_query()
                    self.database.cursor.execute(
                        self.table_creation_query
                    )
                    self.database.connection.commit()
                except Exception as error_msg:
                    quit_pogram_with_error_print_and_log (
                        f"Failed to create table {self.table_name} in {self.database}. Error: {error_msg}"
                    )
        return None
    
    def build_table_creation_query (self):
        
        self.table_creation_query = f"""CREATE TABLE IF NOT EXISTS {self.table_name} ( """
        for i in range(len(self.table_column_dict['column_names'])):
            self.table_creation_query += self.table_column_dict['column_names'][i]
            self.table_creation_query += ' '
            self.table_creation_query += self.table_column_dict['data_types'][i]
            self.table_creation_query += ' '
            if self.table_column_dict['other_args'][i] != '':
                self.table_creation_query += self.table_column_dict['other_args'][i]
            self.table_creation_query += ',\n'
            
        for i in range(len(self.table_column_dict['key_columns'])):
            if self.table_column_dict['key_columns'][i] != '':
                self.table_creation_query += self.table_column_dict['key_columns'][i]
            self.table_creation_query += '\n'
        self.table_creation_query += ")"
        return None
    
    def _valid_table_column_dict_format (self):
        logging.info("Checking the table dict is valid for creation of a postgresql table.")
        err_msg = 'Invalid table dict format.'
        
        # Check dict keys
        if not self._valid_dict_keys():
            quit_pogram_with_error_print_and_log(err_msg)
            return False
        
        # Check column names
        if self._duplicate_column_names(self.table_column_dict["column_names"]):
            quit_pogram_with_error_print_and_log(err_msg)
            return False
        if not all([self._valid_column_name(name) for name in self.table_column_dict["column_names"]]):
            quit_pogram_with_error_print_and_log(err_msg)
            return False
        
        # Check column data types
        if not all([self._valid_column_data_type(dtype) for dtype in self.table_column_dict["data_types"]]):
            quit_pogram_with_error_print_and_log(err_msg)
            return False
        
        return True

    def _valid_dict_keys (self):
        logging.info('Checking dict keys.')
        if not all([
            key in ['column_names', 'data_types', 'other_args', 'key_columns'] 
            for key in self.table_column_dict
        ]):
            logging.error('Missing or invalid dict key(s).')
            return False
        return True
    
    def _valid_column_name (self, column_name: str):
        # I'm not allowing quotes in names for simplicity. Otherwise based on Postgres 7.0 documentation.
        logging.info(f'Checking table {self.table_name} column name.')
        if "\"" in column_name:
            logging.error('Column name must not contain quotes.')
            return False
        column_name_length_constraint = 31
        if len(column_name) > column_name_length_constraint:
            logging.error(f'Column name must be less than {column_name_length_constraint} characters.')
            return False
        reserved_words = ['tableoid', 'xmin', 'cmin', 'xmax', 'cmax', 'ctid']
        if any([column_name == word for word in reserved_words]):
            logging.error(f'Invalid column name. \'{column_name}\' is a reserved word.')
            return False
        if not column_name[0].isalpha() and not column_name[0] == '_':
            logging.error(f'Column name first character \'{column_name[0]}\' is not allowed. Must be a letter (a-z) or \'_\'.')
            return False
        if not all([(element.isalpha() or element.isdigit() or element == '_') for element in column_name[1:]]):
            logging.error('Column name must only contain letters (a-z), digits (0-9), or underscores.')
            return False
        
        logging.info(f'{column_name} is valid.')
        return True
    
    def _duplicate_column_names (self, column_names):
        logging.info('Checking column names for duplicates.')
        if len(column_names) != len(set(column_names)):
            logging.info('Found duplicate column names. Note that \'FOO\' and \'foo\' are duplicates.')
            return True
        return False
        
    
    def _valid_column_data_type (self, dtype: str):
        logging.info('Checking data types of columns.')
        postres_data_types = [
            'bigint', 'bigserial', 'bit', 'bit varying', 'boolean', 'box', 
            'bytea', 'character', 'character varying', 'cidr', 'circle', 
            'date', 'double precision', 'inet', 'integer', 'interval',
            'json', 'jsonb', 'line', 'lseg', 'macaddr', 'macaddr8', 'money',
            'numeric', 'path', 'pg_lsn', 'pg_snapshot', 'point', 'polygon',
            'real', 'smallint', 'smallserial', 'serial', 'text', 'time', 'timestamp',
            'timestamp', 'tsquery', 'tsvector', 'txid_snapshot', 'uuid', 'xml'
        ]
        if dtype.lower() in postres_data_types:
            return True
        quit_pogram_with_error_print_and_log (
            f'Requested data type \'{dtype}\' not found in table \'{self.table_name}\'. Valid types are:\n {postres_data_types}.'
        )
        return False