import logging
# standardize use of logging module in fs-drift

def start_log(prefix):
    log = logging.getLogger(prefix)
    h = logging.StreamHandler()
    log_format = prefix + ' %(asctime)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_format)
    h.setFormatter(formatter)
    log.addHandler(h)
    log.setLevel(logging.DEBUG)
    return log

    #with open('/tmp/weights.csv', 'w') as w_f:
