'''
Created on Oct 14, 2016

@author: Vasan
'''
import os
import jobInfo.getStatus
import time
lfNM="C:/junk/junk.lock"
dfNM="C:/junk/report.txt"
#os.remove(lfNM)
ji=jobInfo.getStatus.getStatus(lfNM,dfNM)
def add(a,b):
    return a+b
if __name__ == "__main__":
    # execute only if run as a script
    ji.getStatus()


    