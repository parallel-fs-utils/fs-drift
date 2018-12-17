import os
import logging

# standardize use of logging module in fs-drift

def start_log(prefix, verbosity=0):
    log = logging.getLogger(prefix)
    if os.getenv('LOGLEVEL_DEBUG') != None or verbosity != 0:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)
    log_format = prefix + ' %(asctime)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_format)

    h = logging.StreamHandler()
    h.setFormatter(formatter)
    h.setLevel(logging.INFO)
    log.addHandler(h)

    h2 = logging.FileHandler('/var/tmp/fsd.%s.log' % prefix)
    h2.setFormatter(formatter)
    log.addHandler(h2)

    log.info('starting log')
    return log

if __name__ == '__main__':
    log = start_log('fsd_log_test')
    log.error('level %s', 'error')
    log.warn('level %s', 'warn')
    log.info('level %s', 'info')
    log.debug('level %s', 'debug')

