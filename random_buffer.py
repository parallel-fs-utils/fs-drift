# random_buffer.py - generate a random but printable text string

import string
import array
import os, mmap
from numpy import append
from common import myassert, BYTES_PER_KiB

#Generate buffer of deduplicable and compressible data
#*Step 1: generate some compressible blocks by fillng a portion
#with random data and pad the rest with zeroes
#*Step 2: repeat these blocks in the buffer to achieve deduplicability
#Note, I know this looks bad, but I'm trying to squeeze in as much performance
#as possible
def gen_compressible_buffer(size_bytes, dedupe, compression_ratio):
    compress = 1/compression_ratio
    to_dedupe = (dedupe/100)
    if not to_dedupe:
        to_dedupe = 1
        repeat_buf = 1
    else:
        repeat_buf = int(1 / (1 - to_dedupe))
    number_of_blocks = int((size_bytes / (4 * BYTES_PER_KiB)) * to_dedupe)

    blocks = bytearray()
    for i in range(number_of_blocks):
        blocks.extend(bytearray(os.urandom(int(compress * 4 * BYTES_PER_KiB))) + bytearray(((4 * BYTES_PER_KiB)-int(compress * 4 * BYTES_PER_KiB))*b'\0'))            
    
    return repeat_buf * blocks
    
starter_array_len = 1024

def gen_init_buffer( size_bytes ):
  b = array.array('B')
  for k in range(0, size_bytes):
    index = k % len(string.printable)
    printable_char = string.printable[index]
    b.append(ord(printable_char))
  return b

starter_buffer = gen_init_buffer(starter_array_len)

# use doubling of buffer size to reduce time spent in python interpreter
# the loop in gen_init_buffer is very expensive per byte

def gen_buffer( size_bytes ):
  b = array.array('B')
  if size_bytes < starter_array_len:
    b = starter_buffer[0:size_bytes] 
  else:
    b = starter_buffer[:]
    while len(b) < size_bytes / 2:
      b = append(b, b)
    remainder = size_bytes - len(b)
    b = append(b, b[0:remainder])
    myassert(len(b) == size_bytes)
  return b

if __name__ == '__main__':
    import time
    start_time = time.perf_counter()
    buf = gen_buffer(1000)
    print('Time elapsed:', (time.perf_counter() - start_time)*1000)
    start_time = time.perf_counter()
    buf = gen_buffer(1000000)
    print('Time elapsed:', (time.perf_counter() - start_time)*1000)
    start_time = time.perf_counter()
    buf = gen_compressible_buffer(4096*245, 50, 4.0)
    print('Time elapsed:', (time.perf_counter() - start_time)*1000)
    
    
