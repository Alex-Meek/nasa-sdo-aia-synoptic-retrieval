import logging
import keyring
from classes.database_class import PostgreSQL_Database, PostgreSQL_Table
from functions.utils import create_logfile

create_logfile("./log.log")

this_db = PostgreSQL_Database(
    database_name   =   "nasa_sdo_aia",
    host_ip         =   "192.168.0.46",
    username        =   "omv_pg",
    password        =   keyring.get_password("nasa_sdo_aia_postgres", "omv_pg"),
    port            =   '65432'
)

this_db.connect()

core_information_table = PostgreSQL_Table(
    database=this_db, 
    table_name='a_table', 
    table_column_dict={
        "column_names": [
            "column_1", 'column_2', 'column_3', 'column_4', 'column_5'
            ],
        "data_types": [
            'integer', 'bigint', 'boolean', 'double precision', 'character'
            ],
        "other_args": [
            '', '', '', '', ''
            ],
        'key_columns': [
            'PRIMARY KEY (column_1)',
        ]
    }
)


