import os
import logging

# standardize use of logging module in fs-drift

def start_log(prefix):
    log = logging.getLogger(prefix)
    log_format = prefix + ' %(asctime)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_format)

    h = logging.StreamHandler()
    h.setFormatter(formatter)
    h.setLevel(logging.INFO)
    log.addHandler(h)

    h2 = logging.FileHandler('/var/tmp/fsd.%s.log' % prefix)
    h2.setFormatter(formatter)
    h2.setLevel(logging.DEBUG)
    if os.getenv('LOGLEVEL_DEBUG'):
        h2.setLevel(logging.DEBUG)
    log.addHandler(h2)

    return log
