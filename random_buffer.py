# random_buffer.py - generate a random but printable text string

import string
import array
import os
from numpy import append
from common import myassert, BYTES_PER_KiB

def gen_block(compress):
    bytes_per_block = 4 * BYTES_PER_KiB
    random_bytes = int((1/compress) * bytes_per_block)
    return bytearray(os.urandom(random_bytes)) + bytearray((bytes_per_block-random_bytes)*b'\0')

def gen_compressible_buffer(size, dedupe, compress):
    to_dedupe = (dedupe/100)
    if not to_dedupe:
        to_dedupe = 1
        repeat_buf = 1
    else:
        repeat_buf = int(1 / (1 - to_dedupe))
    number_of_blocks = int((size / 4096) * to_dedupe)

    blocks = bytearray()
    for i in range(number_of_blocks):
        blocks += gen_block(compress)
    
    buf = bytearray()
    for i in range(repeat_buf):
        buf += blocks
    return buf
    
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
    buf = gen_compressible_buffer(4096*245, 50, 75)
    print('Time elapsed:', (time.perf_counter() - start_time)*1000)
