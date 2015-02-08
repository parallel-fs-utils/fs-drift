# random_buffer.py - generate a random but printable text string

import string
import array

def gen_buffer( size_bytes ):
	b = array.array('c')
	for k in range(0, size_bytes):
		index = k % len(string.printable)
		printable_char = string.printable[index]
		b.append(printable_char)
	return b

if __name__ == '__main__':
	print gen_buffer(100)
