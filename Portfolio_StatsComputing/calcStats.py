#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 12 18:27:03 2019

@author: tinghaoli
"""
from __future__ import print_function

import sys

from collections import Counter


import bisect 
class medianFillsize(object):

    def __init__(self):
        """
        index: list. It is the location of the median values and will at most have 2 length.
        m: float. The running median
        que: list.  the fill size array.
        """
        self.mid = None
        self.sort_array = []
        self.l_arr=0
    def updateFillsize(self, fill):
        """
        :type fill: int
        """
        
        # Initial populating
        if self.l_arr == 0:
            self.mid = fill
            self.sort_array.append(fill)

        # Update the mid according to the new fill size
        else:
            if fill<=self.mid:
                bisect.insort_left(self.sort_array,fill)
            else:
                bisect.insort_right(self.sort_array,fill)
    
        self.l_arr +=1

    def computeMedian(self):
        """
        :rtype: float
        """
        if self.l_arr&1==0:
            return (self.sort_array[self.l_arr>>1] + self.sort_array[(self.l_arr>>1)-1])/2.0
        else:
            return self.sort_array[self.l_arr>>1]



def DataClean(Id,line):
    
    ## Remove the trailing newline if any
    line = line.rstrip("\n\r")
    
    temp = line.split(",")
    ## Make sure every variable have values
    if len(temp)==7 and min([len(t) for t in temp])>0:
        LocalTime,Symbol,EventType,Side,FillSize,FillPrice,FillExchange = temp
    else:
        print("Warning: possible errors and all the information of that row will be skipped, its row ID: {}".format(Id))
        
        return []
        
    FillSize = int(FillSize)
    FillPrice = float(FillPrice)
    
    return LocalTime,Symbol,EventType,Side,FillSize,FillPrice,FillExchange

class TradeRecords():
    def __init__(self, inputFile,outputFile):
        self.records = Counter()
        self.exchangeDict = {}
        
        self.totalBought = 0
        self.totalSold = 0
        self.notionalBought = 0.0
        self.notionalSold = 0.0
        
        self.meanFileSize = 0.0
        self.medianFillSize = medianFillsize()
        
        self.numTrades=0
        
        # No id column in this dataset, so I just created one
        self.ID=0
        
        with open(inputFile, 'r') as inFile, open(outputFile, 'w+') as outFile:
            next(inFile) # skip header
            
            ### For the enriched csv file header
            HEADERS = "LocalTime,Symbol,EventType,Side,FillSize,FillPrice,FillExchange,SymbolBought,SymbolSold,SymbolPosition,SymbolNotional,ExchangeBought,ExchangeSold,TotalBought,TotalSold,TotalBoughtNotional,TotalSoldNotional \n"
            outFile.write(HEADERS)
            for record in inFile:
                self.ID +=1
                try:
                    LocalTime,Symbol,EventType,Side,FillSize,FillPrice,FillExchange = DataClean(self.ID,record)  # last two fields
                except ValueError:
                    continue
                
        
                self.numTrades +=1

                symbolNotional= FillSize*FillPrice
                
                # Update trade ticker records dictionary 
                if Side=='b':
                    
                    self.records.setdefault(Symbol,[0,0,0,0])
                    self.records[Symbol][0] += FillSize
                    self.records[Symbol][1] += FillSize
                    self.records[Symbol][3] = self.records[Symbol][1]-self.records[Symbol][2]
                    
                    
                    # Update the total values related with Buying
                    self.totalBought += FillSize
                    self.notionalBought += symbolNotional
                    
                    # Update exchange ticker records dictionary 
                    self.exchangeDict.setdefault(FillExchange,[0,0])
                    self.exchangeDict[FillExchange][0] +=FillSize
                    
                else:
                    
                    self.records.setdefault(Symbol,[0,0,0,0])
                    self.records[Symbol][0] += FillSize
                    self.records[Symbol][2] += FillSize
                    self.records[Symbol][3] = self.records[Symbol][1]-self.records[Symbol][2]

                    
                    # Update the total values related with Selling
                    self.totalSold += FillSize
                    self.notionalSold += symbolNotional
                    
                    # Update exchange ticker records dictionary 
                    self.exchangeDict.setdefault(FillExchange,[0,0])
                    self.exchangeDict[FillExchange][1] +=FillSize
                    
                
                
                # Compute the running mean fill size to avoid overflow from direct sum
                self.meanFileSize = self.meanFileSize*(self.numTrades-1)/self.numTrades+float(FillSize)/self.numTrades
                
                # Compute the running median fill size 
                self.medianFillSize.updateFillsize(FillSize)
                
                ### Print the current line results:
                record = LocalTime+","+Symbol+ "," + EventType + "," + Side + "," + str(FillSize) \
                +"," + format(FillPrice,".2f") +"," + FillExchange\
                +"," + str(self.records[Symbol][1]) +", " + str(self.records[Symbol][2]) \
                +"," + str(self.records[Symbol][3]) + "," + format(symbolNotional,".2f") \
                +"," + str(self.exchangeDict[FillExchange][0])+ "," + str(self.exchangeDict[FillExchange][1]) \
                +"," + str(self.totalBought) + "," + str(self.totalSold) + "," + format(self.notionalBought,".2f") \
                +"," + format(self.notionalSold,".2f") + "\n"
                
                #print(record)
                outFile.write(record)
                

        inFile.close()
        outFile.close()
        
    def summaryStats(self):
        print ("Processed Trades: ", self.numTrades)
        print ("\n")
        
        print ("Shares Bought: {}".format(self.totalBought))
        print ("Shares Sold: {}".format(self.totalSold))
        print ("Total Volume: {}".format(self.totalBought + self.totalSold))
        
        print ("Notional Bought: $ {0:.2f}".format(self.notionalBought))
        print ("Notional Sold: $ {0:.2f}".format(self.notionalSold))
        print ("\n")

        print("Per Exchange Volumes:")
        
        for e in sorted(self.exchangeDict):
            print(e," Bought: {}".format(self.exchangeDict[e][0]))
            print(e," Sold: {}".format(self.exchangeDict[e][1]))
            
        print ("\n")
        print ("Average Trade Size: {0:.2f}".format(self.meanFileSize))
        print ("Median Trade Size: {0:.2f}".format(self.medianFillSize.computeMedian()))
        print ("\n")
        
        print("10 Most Active Symbols", end=": ")
        
        for aS in self.records.most_common(10):
            print("{}({})".format(aS[0],aS[1][0]), end=' ') 
        
        print("\n")
        
            
          
def main(argv):
    
    # first input of the function will be input file and second is the output file
    Trade_Analysis = TradeRecords(argv[1],argv[2])      
    Trade_Analysis.summaryStats()

def calcTradeStats(inputfile,outputfile):
    
 
    Trade_Analysis = TradeRecords(inputfile,outputfile)      
    Trade_Analysis.summaryStats()
     
     
if __name__ == "__main__":
    main(sys.argv)
    
    
    
    
    
    