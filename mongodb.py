#coding=utf-8
import os, ast
from pymongo import *

def get_raw_data():
	conn=MongoClient('mongodb://localhost:27017/')

	db=conn.bottos

	accounts = db.Accounts.find({})
	blocks   = db.Blocks.find({'merkle_root':{'$ne':'0000000000000000000000000000000000000000000000000000000000000000'}})
	trxs     = db.Transactions.find({}).sort('create_time')
	
	#queryArgs =  {'_id': {'$gt': 0x5bc4ac9b4580a729e89f6de1L }}
	queryArgs =  {}
	projectionFields = ['_id']
	total_blocks = db.Blocks.find(queryArgs, projection = projectionFields).sort('_id')
	
	if os.path.exists('ALL_DATA.log'):
                os.remove('ALL_DATA.log')

	with open('ALL_DATA.log', 'w') as f:
		print '\n===== ACCOUNT =====\n'
		f.write('===== ACCOUNT =====\n')
		for item in accounts:
			#print item
			f.write(str(item) + '\n')
		print
		print '\n===== BLOCK =====\n'
		f.write('===== BLOCK =====\n')
		for item in blocks:
			#print item
			f.write(str(item) + '\n')
		print

		print '\n===== TRXS =====\n'
		f.write('===== TRXS =====\n')
		for item in trxs:
			#print item
			f.write(str(item) + '\n')
	
	cnttmp = 0L
	item_prev = 0L
	startblock_num = 0x5bc4ac9b4580a729e89f6de1L
	for item in total_blocks:
		if long(str(item['_id']), 16) < startblock_num:
			continue
		
		if cnttmp == 0:
			#print long(str(item['_id']), 16)
			item_prev = long(str(item['_id']), 16)
			cnttmp += 1
		if long(str(item['_id']), 16) > (long(item_prev) + 1):

			start = long(item_prev) + 1
			end   = long(str(item['_id']), 16)
			gap = end - start
			print gap
			newstart = start
			while gap > 0:
				print 'start:', hex(start), ', end: ', hex(end), ', Missing block:', hex(newstart)
				newstart += 1
				gap -=1
			
			cnttmp += 1

file_dict_lines = list()
def analyse_file(filename, dstfilename):
	global file_dict_lines
	file_dict = dict()
	valid = False

	if os.path.exists(dstfilename):
                os.remove(dstfilename)
	cnt = 0
	
	with open(filename, 'r') as f:
		for line in f.readlines():
			if '===== TRXS =====' in line:
				valid = True
			
			if not valid:
				continue
			else:
				if '===== TRXS =====' in line:
					continue
				if not 'transfer' in line:
					continue
				if not 'superang' in line:
					continue
			start = line.index('datetime.datetime(')
			stop = start + line[start:].find(')') + 1
			
			linetmp = line[start:stop]
			
			linetmp_replace = linetmp.replace(',', '_')
			linetmp_replace = '\'' + linetmp_replace + '\''
			line = line.replace(linetmp, linetmp_replace)

			start = line.index('ObjectId(')
			stop = start + len ('ObjectId(') + len('\'5bc16d534580a729e89e3b65\')')

			linetmp = line[start:stop]

			linetmp_replace = linetmp.replace('ObjectId(', 'ObjectId_')
			linetmp_replace = linetmp_replace.replace(')', '_')
			linetmp_replace = linetmp_replace.replace('\'','')
			linetmp_replace = '\'' + linetmp_replace + '\''

			line = line.replace(linetmp, linetmp_replace)
			
			start = line.index('cursor_num') + len('cursor_num')
			stop =  line.index('u\'transaction_id')
			
			linetmp = line[start:stop]
			
			while(linetmp in line and 'L' in linetmp):
				line = line.replace(linetmp, linetmp.split('L')[0] + ', ')
			

			start = line.index('lifetime') + len('lifetime')
			stop =  line.index('u\'cursor_label')
			
			linetmp = line[start:stop]
			
			while(linetmp in line and 'L' in linetmp):
				line = line.replace(linetmp, linetmp.split('L')[0] + ', ')
			
			start = line.index('block_number') + len('block_number')
			stop =  line.index('u\'sig_alg')
			
			linetmp = line[start:stop]
			
			while(linetmp in line and 'L' in linetmp):
				line = line.replace(linetmp, linetmp.split('L')[0] + ', ')
			
			start = line.index('cursor_label') + len('cursor_label')
			stop =  line.index('u\'_id')
			
			linetmp = line[start:stop]
			
			while(linetmp in line and 'L' in linetmp):
				line = line.replace(linetmp, linetmp.split('L')[0] + ', ')
			
			file_dict = ast.literal_eval(line)
			
			file_dict_lines.append(file_dict)
		
	
	#file_dict_lines.sort(key=lambda listinfo: listinfo['create_time'])
	with open(dstfilename, 'w') as f:
		for item in file_dict_lines:
			f.write(item['create_time'] + ', ' + str(item['param']) + ', ' + str(item['transaction_id']) + '\n')


def get_account_bto(dstfile):
	if not file_dict_lines:
		return
	dict_account = dict()
	dict_account['bottos'] = 10000000000000000000000L
	for item in file_dict_lines:
		if not 'superang' in item['param']['from'] and not 'superang' in item['param']['to']:
			continue
		userfrom = item['param']['from']
		userto   = item['param']['to']
		amount   = long(item['param']['value'])
		
		dict_account[userfrom] -= amount
		
		if not dict_account.has_key(userto):
			dict_account[userto]   = amount
		else:
			dict_account[userto]   += amount
	
	if os.path.exists(dstfile):
		os.remove(dstfile)
	
	with open(dstflile, 'w') as f:
		for key, item in dict_account:
			print key, ' : ', item
			f.write(dstfile)
get_raw_data()
#analyse_file('ALL_DATA.log', 'RESULT_ALL_DATA.log')
#get_account_bto('RESULT_ALL_BTO.log')
