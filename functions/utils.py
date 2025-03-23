import logging
import sys

def quit_program ():
    try: 
        logging.info("Exiting program.")
        sys.exit (0)
    except Exception:
        sys.exit (0)
    return None

def quit_pogram_with_error_print_and_log (user_information: str):
    logging.error(user_information)
    print(user_information)
    quit_program ()
    return None

def create_logfile (log_filepath: str):
    
    try:
        logging.basicConfig(
            filename   =    log_filepath,
            filemode   =    'a',
            format     =    '%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s',
            datefmt    =    '%Y-%m-%d %H:%M:%S',
            level      =    logging.DEBUG
            )
        
    except ImportError as e:
        print(
            f"Failed to write log file to location \"{log_filepath}\".\n\n Error: {e}"
        )
        quit_program ()
        
    except Exception as e:
        print(f"Unknown error: \n\n {e}")
        quit_program ()
        
    return None