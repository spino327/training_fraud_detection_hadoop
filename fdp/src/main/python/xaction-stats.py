#/usr/bin/env python
# -*- coding: utf -*-

import os
import sys
import string
import random
import glob
import numpy as np

STATES = ['LNL', 'MNL', 'HNL', 
		'LHL', 'MHL', 'HHL', 
		'LNN', 'MNN', 'HNN', 
		'LHN', 'MHN', 'HHN', 
		'LNS', 'MNS', 'HNS',
		'LHS', 'MHS', 'HHS']


def id_generator(length=10, chars=(string.ascii_uppercase + string.digits)):
	# return random string
	return ''.join(random.choice(chars) for _ in range(length))

def seq_gen(customer_id, mat, seq_length):
	''' 
	generate sequence, given:
		customer_id: customer id
		mat: transtion matrix for this customer
		seq_length: how many STATES in this sequence
	'''

	state_num = len(STATES)

	# start with random state, get its index in state array
	index = random.randrange(state_num)
	seq = [STATES[index],]

	# get a cumulated sum matrix 
	cumsum_mat = np.cumsum(mat, axis=1)


	for i in range(seq_length):
		# get a random float between [0, 1)
		rand = random.random()

		# for the given state index,
		# if rand > cumulated_sum[j-1] and rand < cumulated_sum[j-1]
		# then next state index should be 'j'
		# if j == 0
		if (rand < cumsum_mat[index][0]):
			# next state index
			index = 0
			seq.append(STATES[0])

			# transaction_id is random digits only, can be sorted
			transaction_id = id_generator(14, string.digits)
			print "%s, %s, %s" % (customer_id, transaction_id, STATES[0])
		
		else: # if j >= 1
			for j in range(1, state_num):
				if (rand > cumsum_mat[index][j - 1]) and (rand < cumsum_mat[index][j]):
					index = j
					transaction_id = id_generator(14, string.digits)

					print "%s,%s,%s" % (customer_id, transaction_id, STATES[j])
					break

if __name__ == "__main__":


	# how many customer we need
	selected_customer_num = 10
	# around how many transaction we need
	transaction_num = 100
	# the directory where the matrixes saved
	matrix_dir = 'customers/'
	
	if (len(sys.argv) > 1):
		selected_customer_num = int(sys.argv[1])
		transaction_num = int(sys.argv[2])
	else:
		print "usage: argv[0] customer_num transaction_num"
		sys.exit(2)
	# customer array
	customer_ids = []
	# get all generated customer_ids
	# note all the matrix using customer_id as filename
	os.chdir(matrix_dir)
	for filename in glob.glob('*.npy'):
		customer_ids.append(filename[:-4])
	

	customer_num = len(customer_ids)
	# get random selected customer
	selected_customer_ids = random.sample(customer_ids, selected_customer_num)

	for customer_id in selected_customer_ids:		
		# get customer's matrix
		mat = np.load(customer_id + '.npy')
		# set seq_num with random normal distribution
		seq_num = int(random.normalvariate(transaction_num, transaction_num * 0.25))

		seq_gen(customer_id, mat, seq_num)
		