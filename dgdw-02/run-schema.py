#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from datetime import datetime

database = ["m3bg1"]
queries = ["q01","q02","q03", "q04","q05","q06"] # .js extension
NUM_EXEC = 5
SUBDIR = ""

def atualDate():
   now = datetime.now()
   return "{:4d}{:02d}{:02d}-{:02d}{:02d}{:02d}".format(now.year,now.month,now.day,now.hour,now.minute,now.second)

def saveResultFile(directory, database, query, numExec, start, end, time):
    file = directory + "/" + database + "-" + query + ".csv"
    f = open(file, 'a')
    f.write(database + ";" + query + ";" + str(numExec) + ";" + str(time) + ";\n" )
    f.close()
    return True
                
def directory():
    d = os.getcwd() +  "/result-time" + SUBDIR
    if not os.path.exists(d):
        os.makedirs(d)    
    d = os.getcwd()  +  "/result-query" + SUBDIR
    if not os.path.exists(d):
        os.makedirs(d)    

os.system("clear")

print("=========================================================")	
print("RUNNING")	
print("=========================================================")	

directory()
print("make dir - ok")
        
# mongo        
for numExec in range (NUM_EXEC):
    for q in queries:   
        for db in database:
            file = os.getcwd() +  "/result-query" + SUBDIR + "/" + db + "-" + q + "-" + atualDate()              
            print (db + " > " + q + " > " + str(numExec))
            timeStart = datetime.now()          
            mongo = "mongo " + db + " " + db + "/" + q + ".js > " +  file 
            print("   " + mongo)
            os.system(mongo) 
            timeEnd = datetime.now()
            saveResultFile(os.getcwd()+"/result-time" + SUBDIR, db, q, numExec, timeStart, timeEnd, timeEnd-timeStart )
            print("   START: " + str(timeStart) + "  END: " + str(timeEnd) + "  TOTAL: " + str(timeEnd - timeStart))
print("\nDone!")



