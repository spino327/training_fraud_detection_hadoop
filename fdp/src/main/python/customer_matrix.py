#/usr/bin/env python
# -*- coding: utf -*-

import string
import random
import numpy as np

STATE_NUM = 18

def id_generator(length=10, chars=(string.ascii_uppercase + string.digits)):
	return ''.join(random.choice(chars) for _ in range(length))

def customer_id_list_generator(size=100):
	'''
	generate a list with #size customer_ids
	'''
	customer_ids = []
	for i in range(size):
		cid = id_generator()
		print i
		customer_ids.append(cid)
	return customer_ids

def customer_matrix_generator():
	'''
	generate a random transition matrix for customer_id
	'''
	# random a 18*18 matrix
	mat = np.random.randint(100, size=(STATE_NUM, STATE_NUM))
	# add 1 to every item to ensure no item equal zero
	mat = mat + np.ones((STATE_NUM, STATE_NUM))
	# get sum of each row
	row_sum = np.sum(mat, axis=0, dtype=float)
	# each item in a row divide sum
	prob_mat = np.divide(mat, row_sum)
	return prob_mat

if __name__ == "__main__":
	PREFIX = 'customers/'
	customer_ids = customer_id_list_generator(1000)
	for cid in customer_ids:
		filename = PREFIX + cid
		# save the matrix to a file named with customer_id
		np.save(filename, customer_matrix_generator())
