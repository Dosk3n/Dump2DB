# LOAD DATA LOW_PRIORITY LOCAL INFILE 'C:\\csvs\\dump1.csv.csv' REPLACE INTO TABLE collection1_1 CHARACTER SET latin1 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n';

# Script to add DB dumps to DB
# This script takes all the .txt files in to a list and loops
# through each file cleaning up the strings to a user:pass where
# some of the strings used a ; instead.
# It then creates a 3 column CSV files with the first colum empty
# as this is an auto inc column in the DB I use.
# Next it uses the above command to import the files and then deletes
# both the text and csv to clean up.

from os import listdir
from os.path import isfile, join
import pymysql
import os
import sys

def GetArgs():
	if(len(sys.argv) == 1):
		sys.exit(HowToUse())
	else:
		return sys.argv[1]
		
def HowToUse():
	print()
	print("Usage: script.py TABLE_NAME")
	print("You must run this script with the argument of the table name")
	print("that you wish to send the data to.")
	

def GetTxtFiles():
	""" Get list of files in current dir """
	files = [f for f in listdir(".") if isfile(join(".", f))]
	fileList = []
	for file in files:
		try:
			if(file[-4:] == ".txt"):
				fileList.append(file)
		except:
			continue
			
	fileList.sort()
	return fileList
	
def FileToCleanArray(filename):
	""" Clean up the list to replace semi colon with colon on first occurrence """
	txtFile = open(filename, "r", errors='ignore')
	lineList = txtFile.readlines()
	txtFile.close()
	
	# This section works by looking at the entire line of text and looking for the first occurrence
	# of either a ; or a : and once it finds it it then replaces with a :/
	# Since an or statement works by ending as soon as the first condition is true we dont have to
	# worry about it running the next replace in the query
	creds = []
	for line in lineList:
		try:
			if(line.replace(";", ":", 1) or line.replace(":", ":", 1)):
				creds.append((line.replace(";", ":", 1) or line.replace(":", ":", 1)).strip()) #strip is to remove newline char
		except:
			continue
	
	return creds
	
def SplitCreds(rawcreds, creds):
	for rcred in rawcreds:
		creds.append(rcred.split(":", 1))
	
	return creds
	
def creds2csv(creds, filename):
	csvFile = open(filename + ".csv", "w")
	index = 1
	for cred in creds:
		try:
			#print(cred)
			csvFile.write("" + "," + str(cred[0]) + "," + str(cred[1]) + "\n")
			index = index + 1
		except:
			continue
	csvFile.close()
	
def RunInsertQuery(filename, table_name):
	curdir = os.getcwd()
	querydir = curdir.replace("\\", "\\\\")

	query = """LOAD DATA LOW_PRIORITY LOCAL INFILE '""" + querydir + """\\\\""" + filename + """' REPLACE INTO TABLE """ + table_name + """ CHARACTER SET latin1 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\r\\n';"""
	
	command = """C:\\xampp\\mysql\\bin\\mysql database_dump -uroot -e \"""" + query + """\""""
	
	os.system(command)
	
def DeleteFiles(file):
	txtfile = file
	csvfile = file+".csv"
	os.remove(txtfile)
	os.remove(csvfile)
	
	print(txtfile)
	print(csvfile)
def Main():
	# Get list of files in current dir
	fileList = GetTxtFiles()
	
	csvname = "dump.csv"
	
	table_name = GetArgs()

	# Loop through each file
	for file in fileList:
		creds = []
		print("\nStarting to work on file: " + str(file))
		print("Replacing ; with : ...")
		cleandata = FileToCleanArray(file)
		print("Creating array data by splitting on : ...")
		creds = SplitCreds(cleandata, creds)
		print("Creating csv file...")
		creds2csv(creds, file)
		csvfiletosend = file + ".csv"
		print("Running SQL insert...")
		RunInsertQuery(csvfiletosend, table_name)
		print("Deleting Files...")
		DeleteFiles(file)
		print("Completed file: " + str(file))








Main()