import logging
import keyring
from classes.database_class import PostgreSQL_Database
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

