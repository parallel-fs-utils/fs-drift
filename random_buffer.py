# random_buffer.py - generate a random but printable text string

import string
import array
from numpy import append
from common import myassert

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
    start_time = time.time()
    buf = gen_buffer(1000)
    print(time.time() - start_time)
    start_time = time.time()
    buf = gen_buffer(1000000)
    print(time.time() - start_time)
