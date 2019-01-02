import argparse
import os
from common import FileSizeDistr
 
TypeExc = argparse.ArgumentTypeError

# if we throw exceptions, do it with this
# so caller can specifically catch them

class SmfParseException(Exception):
    pass

# the next few routines implement data types
# of smallfile parameters

def boolean(boolstr):
    if boolstr == True:
        return True
    elif boolstr == False:
        return False
    b = boolstr.lower()
    if b == 'y' or b == 'yes' or b == 't' or b == 'true':
        bval = True
    elif b == 'n' or b == 'no' or b == 'f' or b == 'false':
        bval = False
    else:
        raise TypeExc('boolean value must be y|yes|t|true|n|no|f|false')
    return bval

def positive_integer(posint_str):
    i = int(posint_str)
    if i <= 0:
        raise TypeExc( 'integer value greater than zero expected')
    return i

def non_negative_integer(nonneg_str):
    i = int(nonneg_str)
    if i < 0:
        raise TypeExc( 'non-negative integer value expected')
    return i

def bitmask(int_or_hex_str):
    try:
        i = int(int_or_hex_str)
    except ValueError:
        try:
            i = int(int_or_hex_str, 16)
        except ValueError:
            raise TypeExc('positive integer or hex string expected')
    return i

def positive_float(pos_float_str):
    f = float(pos_float_str)
    if f <= 0.0:
        raise TypeExc( 'floating-point value greater than zero expected')
    return f

def non_negative_float(nonneg_float_str):
    f = float(nonneg_float_str)
    if f < 0.0:
        raise TypeExc( 'floating-point value less than zero not expected')
    return f

def positive_percentage(pos_float_str):
    f = positive_float(pos_float_str)
    if f > 100.0:
        raise TypeExc( 'percentages must be no greater than 100')
    return f

def host_set(hostname_list_str):
    if os.path.isfile(hostname_list_str):
        with open(hostname_list_str, 'r') as f:
            hostname_list = [ record.strip() for record in f.readlines() ]
    else:
        hostname_list = hostname_list_str.strip().split(',')
        if len(hostname_list) < 2:
            hostname_list = hostname_list_str.strip().split()
        if len(hostname_list) == 0:
            raise TypeExc('host list must be non-empty')
    return hostname_list

def directory_list(directory_list_str):
    directory_list = directory_list_str.strip().split(',')
    if len(directory_list) == 1:
        directory_list = directory_list_str.strip().split()
    if len(directory_list) == 0:
        raise TypeExc('directory list must be non-empty')
    return directory_list

def file_size_distrib(fsdistrib_str):
    # FIXME: should be a data type
    if fsdistrib_str == 'exponential':
        return FileSizeDistr.random_exponential
    elif fsdistrib_str == 'fixed':
        return FileSizeDistr.fixed
    else:
        # should never get here
        raise TypeExc(
            'file size distribution must be either "exponential" or "fixed"')

def file_access_distrib(distrib_str):
    # FIXME: should be a data type
    if distrib_str == 'gaussian':
        return FileAccessDistr.gaussian
    elif distrib_str == 'uniform':
        return FileAccessDistr.uniform
    else:
        # should never get here
        raise TypeExc(
            'file access distribution must be either "gaussian" or "uniform"')

