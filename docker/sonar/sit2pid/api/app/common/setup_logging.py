import logging

def setup_logger(log_level=logging.INFO):

    logger = logging.getLogger()
    logger.setLevel(log_level)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(module)s : %(lineno)d - %(message)s'
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
