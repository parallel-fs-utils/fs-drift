import argparse
import os
from common import FileSizeDistr, FileAccessDistr
from common import BYTES_PER_KiB

TypeExc = argparse.ArgumentTypeError

# if we throw exceptions, do it with this
# so caller can specifically catch them

class FsDriftParseException(Exception):
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

def positive_integer_or_None(posintornone_str):
    if isinstance (posintornone_str, str):    
        if 'none' in posintornone_str.lower():
            return None
        else:
            i = int(posintornone_str)
            if i <= 0:
                raise TypeExc( 'integer value greater than zero expected')
        return i
    if isinstance (posintornone_str, int):    
        if posintornone_str <= 0:
            raise TypeExc( 'integer value greater than zero expected')
        return posintornone_str
    
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
        
#If the input is g or G, multiply by 1024*1024*1024
#If the input is m or M, multiply by 1024*1024
#If the input is k or K multiply by 1024
#If the input is b or B, return as is
def size_unit_to_bytes(v):
    try:
        if 'g' in v.lower():
            return int(v[:-1]) * BYTES_PER_KiB * BYTES_PER_KiB * BYTES_PER_KiB
        elif 'm' in v.lower():
            return int(v[:-1]) * BYTES_PER_KiB * BYTES_PER_KiB
        elif 'k' in v.lower():
            return int(v[:-1]) * BYTES_PER_KiB
        elif 'b' in v.lower():
            return int(v[:-1])
        else:
            return int(v)
    except ValueError:
        raise TypeExc('valid size expected: [b, k, m, g], not ', v)            

def size_or_range(size_input):
    if isinstance(size_input, int):
        return size_input
    if ':' not in size_input:
        return size_unit_to_bytes(size_input)
    else:
        low_bound, high_bound = size_input.split(':')
        low_bound = size_unit_to_bytes(low_bound)
        high_bound = size_unit_to_bytes(high_bound)
        if low_bound > high_bound:
            raise TypeExc('low bound (left) should be larger than high bound (right), got %s' % size_input)
        return (low_bound, high_bound)
