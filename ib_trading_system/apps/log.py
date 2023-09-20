import os
import logging
import datetime
import pytz

def create_logger(path:str, fileName:str, log_mode:str="save_and_print", root_logger:bool=True) -> logging.Logger:
    """
    Creates a logger to write messages to a file and optionally to the console.
    
    Args:
        path (str): Directory where the log file will be stored.
        fileName (str): Name of the log file (without the extension).
        log_mode (str, optional): Mode to determine logging behavior. Can be one of ["save_and_print", "save_only", "print_only"]. Defaults to "save_and_print".
        root_logger (bool, optional): Whether to use the root logger or a custom named logger. Defaults to True.
       
    Returns:
        logging.Logger: Configured logger instance.
    """
    
    if log_mode not in ["save_and_print", "save_only", "print_only"]:
        raise ValueError("Invalid log_mode. Choose from ['save_and_print', 'save_only', 'print_only']")

    now = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    fullFileName = os.path.join(path, f'{now}_{fileName}.log')

    FORMAT = '%(asctime)s %(levelname)s: %(message)s'
    formatter = logging.Formatter(FORMAT)
    formatter.converter = lambda *args: datetime.datetime.now().timetuple()

    logger = logging.getLogger() if root_logger else logging.getLogger(fullFileName)
    logger.setLevel(logging.INFO)
    
    if log_mode in ["save_and_print", "save_only"]:
        file_handler = logging.FileHandler(fullFileName, mode='a', encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    if log_mode in ["save_and_print", "print_only"]:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


def close_logger(logger: logging.Logger):
    """
    Closes and saves the log file associated with the provided logger.

    Args:
        logger (logging.Logger): The logger object to close and save.

    Returns:
        None
    """
    # Close and remove all handlers associated with the logger
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)


if __name__=='__main__':

    # Example usage
    # Create a logger named 'my_log_file' and log messages to a file in the specified path
    my_logger = create_logger("/path/to/log", "my_log_file")

    # Log a message
    my_logger.info("This is a log message")

    # Close and save the log file
    close_logger(my_logger)

