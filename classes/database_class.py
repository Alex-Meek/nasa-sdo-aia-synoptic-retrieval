import sys
sys.path.append("..")
import psycopg2
import logging
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
    
    # def intialise_database ():
        
    #     return None
    