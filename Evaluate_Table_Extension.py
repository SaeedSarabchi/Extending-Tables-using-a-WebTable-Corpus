'''
This file include all the experiments executed for evaluating our proposed solution
as well as comparing our solution with the other baselines.
'''

import xml.etree.ElementTree as ET
import pickle
import json
import urllib
import re
from CommonUtilities import * 
from Dataset_Gathering import * 
from Octopus import * 
import time
import xml.etree.ElementTree as ET
import csv
import logging
from DBPedia import *
from Proposed_Solution import * 
from Mannheim_SearchJoin import * 
from Proposed_Sol_ColumnSelectionTestings import *
from InfoGather import *
import sys
from shutil import copyfile, copy2
import os

global dbPedia
evaluation=""
lookup=""
predicate=""
dbPediaEval = ""



Eval_Dataset="Country"
#Country
#Company
#VideoGame
#Film

#Logger code From https://stackoverflow.com/questions/17035077/python-logging-to-multiple-log-files-from-different-classes       
def setup_logger(logger_name, log_file, level=logging.INFO):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(asctime)s : %(message)s')
    fileHandler = logging.FileHandler(log_file, mode='w')
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)  

    l.setLevel(level)
    l.addHandler(fileHandler)
    l.addHandler(streamHandler) 
    

DBPedia_Lookup_Dict={}  #Works as Lookup Cache
'''def DBPedia_Lookup(query):
    
    if(query in DBPedia_Lookup_Dict):
        return DBPedia_Lookup_Dict[query]
    

    service_url = 'http://lookup.dbpedia.org/api/search/KeywordSearch'
    params = {
        'QueryString': query,
        'MaxHits' : 1
    }
    url = service_url + '?' + urllib.parse.urlencode(params)
    #try:
    response =(str(urllib.request.urlopen(url).read().decode("utf-8")))
    
    response=response.split("<URI>")
    try:
        response=response[1].split("</URI>")
        DBPedia_Lookup_Dict[query] = "<"+response[0]+">"
        lookup.info(query+" : "+str(response[0]))
        return ("<"+response[0]+">")
    except:
        lookup.info(query+" : ERROR")
        return("")'''

def DBPedia_Lookup(query):
    global dbPedia
    Result_set = dbPedia.Search_in_Mappings(query)
    #lookup.info(query+" : "+str(Result_set))
    return Result_set
   
'''def DBPedia_GetPredicate(Subject, Object):

    query='select distinct ?Rel where {'+Subject+' ?Rel '+Object+'}'
    print(query)
    service_url = 'http://dbpedia.org/sparql'
    params = {
        'default-graph-uri':'http://dbpedia.org',
        'query': query,
        'format':'application/sparql-results+json',
        'timeout': '30000',
        'debug':'on',
        'run':' Run Query '
    }
    
    try:
        url = service_url + '?' + urllib.parse.urlencode(params)
        #print(url)
        #try:
        #response =(urllib.request.urlopen(url).read())
        response = json.loads(urllib.request.urlopen(url).read())
        #response =(urllib.request.urlopen(url).read())
        #print(response)
        JsonResults = response["results"]["bindings"]
        #print(JsonResults)
        results=set()
        for JsonRes in JsonResults:
            results.add(JsonRes["Rel"]["value"])
            
        predicate.info(query+" : "+str(results))
        return(results)
    except:
        predicate.info(query+" : ERROR")
        return (set())'''
        
def DBPedia_GetPredicate(Subject, Object):
    global dbPedia
    results = dbPedia.Extract_Predicates(Subject, Object)
    '''if(len(results)>0):
        predicate.info("Subj : "+str(Subject)+", Obj: "+str(Object)+", Result: "+str(results))
    else:
        predicate.info("Subj : "+str(Subject)+", Obj: "+str(Object)+", Result: ERROR!")'''
        
    return results
    
    #print(results)   
    #except:
        #print("Error!")

#-------------------Evaluate_Table_Extension---------------------------
        
#First Reads the output Excel file which is the result of the Table Extension
#Then labels EACH COLUMN based on Max common "Predicate"of all (Subjct,Object) Combination of 'k-v's.
#Then based on the col label, every k-v existing in DBPedia is considered as True, -3 Precision +  Recall of Content Per Column, Overal: Pre+Recall+F-Measure of Top N Mapped cols: N=Number of Cols in Ground-Truth
#Then based on the col lables, Precision+Recall of 'Related Cols' are calculated., of Top N Mapped cols based on content: N=Number of Cols in Ground-Truth
#Then, based on the col labesl, The Grouping Co-efficient is calculated: #Unique/All cols of Top N Mapped cols based on content: N=Number of Cols in Ground-Truth
    
class Aug_Cell:
    """Aug_Cell is the representation of each value in a column
    with the related attributes for evaluation: Predicate of k-v, True or False in terms of content
    """
    def __init__(self, val):
        self.value = val
        self.predicates=set()
        self.ifKVExists=""
    
    def setKVPredidates(self, predicates):
        self.predicates=predicates
        
    def setIfKVExists(self, res):
        self.ifKVExists=res
        
    def setDBPedia_URL(self):
        self.dbPedia_URL = DBPedia_Lookup(self.value)
    
class Aug_Col:
    """Aug_Col is the representation of a column
    with the related attributes for evaluation: Content Prec+Recall, ...
    """
    
    def __init__(self, vals, label):
        self.values= vals
        self.label = label
        self.dbPedia_Label=""
        
        
        
    #Set that Predicate which is most reffered to.
    def setDBPediaLabel(self):
        PredicateCnt_Dict={}
        for val in self.values:
            for predicate in val.predicates:
                if(predicate in PredicateCnt_Dict):
                    PredicateCnt_Dict[predicate] +=1
                else:
                    PredicateCnt_Dict[predicate] =1
        
        max=0
        for predicate in PredicateCnt_Dict:
            if(PredicateCnt_Dict[predicate]>max):
                self.dbPedia_Label=predicate
                max=PredicateCnt_Dict[predicate]
                
        
            
                    
    def setContent_Precision_Recall_FMeasure(self):
        for val in self.values:
            if(len(val.predicates)>0):
                if self.dbPedia_Label in val.predicates:
                    val.setIfKVExists('True')
                else:
                    val.setIfKVExists('False')
                    
        true=0
        selected=0
        for val in self.values:
            if(val.ifKVExists!=""):
                selected+=1
                if(val.ifKVExists=="True"):
                    true+=1
                    
        if(selected!=0):
            self.content_Precision = true/selected
        else:
            self.content_Precision = 0
        self.content_Recall = selected/len(self.values)
        
        if((self.content_Precision+self.content_Recall)!=0):
            self.content_fMeasure = 2*self.content_Precision*self.content_Recall/(self.content_Precision+self.content_Recall)
        else:
            self.content_fMeasure = 0
                
                
            
    def log_col_eval_result(self):
        
        evaluation.info("###################")
        evaluation.info("Col Label: "+self.label)
        evaluation.info("Col DBPedia Label: "+self.dbPedia_Label)
        evaluation.info("Col Precision: "+str(self.content_Precision)+", "+"Col Recall: "+str(self.content_Recall)+", Col F-Measure: "+str(self.content_fMeasure))
        evaluation.info("###################")
    
    
    def eval_col(self, Keys):
        for i in range(len(Keys.values)):
            if(self.values[i].value!="" and Keys.values[i].value!=""):
                print(self.values[i].value)
                Subject = Keys.values[i].dbPedia_URL
                Object= DBPedia_Lookup(self.values[i].value)
                if(Subject!="" and Object!=""):
                    self.values[i].setKVPredidates(DBPedia_GetPredicate(Subject, Object))
            
        self.setDBPediaLabel()
        self.setContent_Precision_Recall_FMeasure()
        
        self.log_col_eval_result()

             
    #Sets the DBPedia URL beforehand to speedup the process of Searching for Predicates.
    def eval_KeyCol(self):
        for val in self.values:
            val.setDBPedia_URL()

class Aug_Table:
    """Aug_Table is the representation of an output Extended Table
        With a list of Cols and attributes used for evaluating the Extension.
    """
    
    def __init__(self, CsvTable):
        self.init_Cols_From_Csv(CsvTable)
        self.Related_Cols_Ground_Truth=T2D_GoldStandard_Related_Columns(Eval_Dataset)
        self.evaluation = logging.getLogger('evaluation')
        
    
    def init_Cols_From_Csv(self, CsvTable):
        self.cols=[]
        with open(CsvTable) as CsvTableFile:
            csv_Content = csv.reader(CsvTableFile)
            CsvMAtrix=[]
            for row in csv_Content:
                CsvRow=[]
                for val in (row):
                    CsvRow.append(val)
                CsvMAtrix.append(CsvRow)
            #print(CsvMAtrix)
            for colIdx in range(len(CsvMAtrix[0])):
                #if(colIdx>=7):
                    #break
                values=[]
                for rowIdx in range(len(CsvMAtrix)):
                    if (rowIdx==0):
                        Col_Label=CsvMAtrix[rowIdx][colIdx]
                    else:
                        values.append(Aug_Cell(CsvMAtrix[rowIdx][colIdx]))
                Augmented_Col=Aug_Col(values, Col_Label)
                if(colIdx==0):
                    self.keyCol = Augmented_Col
                else:
                    self.cols.append(Augmented_Col)
                
                
                
    def eval_Table(self):
        ######Content Eval############:
        self.keyCol.eval_KeyCol()
        cntr=0
        for col in self.cols:
            col.eval_col(self.keyCol)
        
            
        #########Related Col Eval+ Grouping Eval########:
        
        #Sort the Cols based on Content F-MEasure (Evaluating all Extened cols which has label)
        self.cols = sorted(self.cols, key=lambda col:col.content_fMeasure, reverse=True)
        
        #TopNCols
        TopNCols=[]
        UniqueColLabels=set()
        cntr=0
        #while (len(UniqueColLabels)<len(self.Related_Cols_Ground_Truth) and cntr<len(self.cols)):
        while (cntr<len(self.cols)):
            if(self.cols[cntr].dbPedia_Label!=""):
                TopNCols.append(self.cols[cntr])
                UniqueColLabels.add(self.cols[cntr].dbPedia_Label)
            
            cntr+=1
        
        #Evaluating Total Avg Content Prec, Recall, F-Measure:
        Sum_Content_Precision=0
        Sum_Content_Recall=0
        Sum_Content_FMeasure=0
        for col in TopNCols:
            Sum_Content_Precision+=col.content_Precision
            Sum_Content_Recall+=col.content_Recall
            Sum_Content_FMeasure+=col.content_fMeasure
            
        self.avg_TopN_Precision=Sum_Content_Precision/len(TopNCols)
        self.avg_TopN_Recall=Sum_Content_Recall/len(TopNCols)
        self.avg_TopN_FMeasure=Sum_Content_FMeasure/len(TopNCols)
        
        #Evaluating Related Col Precision and Recall
        true=0
        for label in UniqueColLabels:
            if label in self.Related_Cols_Ground_Truth:
                true+=1
                
        if(len(UniqueColLabels)!=0):
            self.relatedCol_Precision = true/len(UniqueColLabels)
        else:
            self.relatedCol_Precision = 0
            
        if(len(self.Related_Cols_Ground_Truth)!=0):
            self.relatedCol_Recall = true/len(self.Related_Cols_Ground_Truth)
        else:
             self.relatedCol_Recall=0
             
        if((self.relatedCol_Precision+self.relatedCol_Recall)!=0):
            
            self.relatedCol_fMeasure = 2*self.relatedCol_Precision*self.relatedCol_Recall/(self.relatedCol_Precision+self.relatedCol_Recall)
        else:
            self.relatedCol_fMeasure=0
                
        
        
        #Evaluating Grouping Factor:
        self.groupingFactor=len(UniqueColLabels)/len(TopNCols)
        
        
        #Log Evaluation Results:
        evaluation = logging.getLogger('evaluation')
        evaluation.info("\n----------------Final Result------------------\n")
        for col in TopNCols:
            col.log_col_eval_result()
            
        evaluation.info("\n----------------Summary------------------\n")
        evaluation.info("Number of Top N Augmented Cols: "+str(len(TopNCols))+", Number of Unique Cols: "+str(len(UniqueColLabels)))
        evaluation.info("\n----------------Evaluating on Content Total Avgs:------------------- ")
        evaluation.info("avg_TopN_Precision: "+str(self.avg_TopN_Precision)+ ", avg_TopN_Recall: "+str(self.avg_TopN_Recall)+", avg_TopN_FMeasure: "+str(self.avg_TopN_FMeasure)+"\n")
        evaluation.info("\n-----------------Evaluating on Related Columns:------------------------")
        evaluation.info("relatedCol_Precision: "+str(self.relatedCol_Precision)+" , relatedCol_Recall: "+str(self.relatedCol_Recall)+", relatedCol_fMeasure: "+str(self.relatedCol_fMeasure)+"\n")
        evaluation.info("groupingFactor: "+str(self.groupingFactor)+"\n")
        
        
        
        
    
    def print_Table(self):
        for col in self.cols:
            print("\n####"+col.label+" : "+ col.dbPedia_Label+"####\n")
            #for val in col.values:
                #print(val.value)
Redirects={}
DBPedia_TotalRedirects_Pickle_Loc = r"C:\Saeed_Local_Files\DBPedia\total_redirects_en_2014.pickle" 
#Check if 2 strings approximately match or not                  
def equalityCheck(GT_val, ET_Val):
    global Redirects

    if(Edit_Distance_Ratio((GT_val.replace("_"," ")), (ET_Val.replace("_"," ")))<0.3 ):
        return True
    else:
        if GT_val in Redirects:
            for elem in Redirects[GT_val]:
                if(Edit_Distance_Ratio((elem.replace("_"," ")), (ET_Val.replace("_"," ")))<0.3 ):
                    return True
        return False
        
#Evaluation based on Ground Truth extracted from DBPedia.
def Eval_DBPedia_As_GT(ExtendedTable_Path, ET_DBPedia_Mapping_path, GT_Path, Equi_Props_Path):
    
    global Redirects
    with open(DBPedia_TotalRedirects_Pickle_Loc, 'rb') as f1:
        Redirects = pickle.load(f1)
     
    
    ExtendedTable=""
    ET_DBPedia_Mapping=""
    GT = ""
    
    #Initializing Extended Tabke, DBPedia Mapping and the Ground Truth:
    
    with open(ExtendedTable_Path) as ExtendedTable_File:
        ExtendedTable_reader = csv.reader(ExtendedTable_File)
        #Initilize Extended Table:
        ExtendedTable=[]
        for row in ExtendedTable_reader :
            ExtendedTable.append(row)
        
        
        with open(ET_DBPedia_Mapping_path) as ET_DBPedia_Mapping_File:
            ET_DBPedia_Mapping_reader = csv.reader(ET_DBPedia_Mapping_File)
            
            #Initialize ET_DBPedia_Mapping:
            ET_DBPedia_Mapping = []
            for row in ET_DBPedia_Mapping_reader :
                ET_DBPedia_Mapping.append(row)
            
            
            with open(GT_Path) as GTFile:
                GT_reader = csv.reader(GTFile)
                #Initialize GT:
                GT = []
                for row in GT_reader :
                    GT.append(row)
                
                #Reading Equivalent Properties File:
                Equi_Props=[]
                with open(Equi_Props_Path, 'r') as Equi_Props_Path_File:
                    for line in Equi_Props_Path_File:
                        elems = line.replace("\n","").split("\t")
                        #print(elems)
                        equi_set=set()
                        for elem in elems:
                            #print(elem)
                            equi_set.add(string_Cleanse(elem.split("/")[-1]))
                        
                        #print(equi_set)
                        Equi_Props.append(equi_set)
                
                
                #print(Equi_Props)
                
                ET_FirstLine = (ExtendedTable[0])
                GT_FirstLine = (GT[0])
                #print(GT_FirstLine)
                
                
                
                #Calculating Content and Related Columns Precision & Recall:
                AVG_Content_Precision = 0
                AVG_Content_Recall = 0
                Related_Cols_Precision = 0
                Related_Cols_Recall = 0
                TP_num_RelatedCol = 0
                FP_num_RelatedCol = 0
                FN_num_RelatedCol = 0
                relCol_Collection = set()
                relCol_Cnt = len(GT_FirstLine)-1
                Unique_Mapped_Cols = set()
                Mapped_Cols = []
                
                Col_cnt = 0     
                for row in ET_DBPedia_Mapping:
                    
                    if("http://dbpedia.org/"  in  row[1]):
                        
                        mapped_prop = string_Cleanse(row[1].split("/")[-1])
                        Unique_Mapped_Cols.add(mapped_prop)
                        Mapped_Cols.append(mapped_prop)
                        
                        
                        #print(mapped_prop)
                        ET_indx = -1
                        GT_indx = -1
                        
                        all_mapped_props=set()
                        all_mapped_props.add(mapped_prop)
                        
                        for equivSet in Equi_Props:
                            if(mapped_prop in equivSet):
                                for elem in equivSet:
                                    all_mapped_props.add(elem)
                                
                        #print(all_mapped_props)
                        #print(ET_FirstLine)
                        for i in range(len(ET_FirstLine)):
                            if(string_Cleanse(ET_FirstLine[i]) == string_Cleanse(row[0])):
                                ET_indx=i
                                #print(ET_FirstLine[i])
                                
                        for j in range(len(GT_FirstLine)):
                            if(string_Cleanse(GT_FirstLine[j]) in  all_mapped_props):
                                GT_indx=j
                                #print(GT_FirstLine[j])
                                
                        if(ET_indx==-1 or GT_indx==-1):
                            dbPediaEval.info((GT_indx, ET_indx))
                            dbPediaEval.info("Fail!")
                            if(GT_indx==-1):
                                FP_num_RelatedCol += 1
                                
                            continue
                        relCol_Collection.add(mapped_prop)
                        #Since the Column is Matched from Ground Truth with one of the oclumns of the extended table:
                        #TP_num_RelatedCol += 1
                        
                        #Calculating Column Precision:
                        
                        
                        TP_num_Content = 0
                        FP_num_Content = 0
                        FN_num_Content = 0
                        
                        
                        for gt_row_indx in range(len(GT)):
                            if(gt_row_indx==0): #ignore first line
                                continue
                            gt_key = GT[gt_row_indx][0] 
                            gt_value = GT[gt_row_indx][GT_indx]
                            
                            #IF the value in the Ground truth is empty, then just continue
                            if(gt_value.strip()==""):
                                dbPediaEval.info("the value in the Ground truth is empty for: "+gt_key)
                                continue;
                            
                            #print("gt: "+str((gt_key, gt_value)))
                            min_editDistance=1
                            et_matchedKey=""
                            et_matchedVal=""
                            for et_row_indx in range(len(ExtendedTable)):
                                if(et_row_indx==0):
                                    continue
                                #print("umad")
                                et_key = ExtendedTable[et_row_indx][0] 
                                et_value = ExtendedTable[et_row_indx][ET_indx]
                                
                                edit_distance = Edit_Distance_Ratio((gt_key), (et_key))
                                #print("et: "+str(edit_distance))
                                
                                if(edit_distance<min_editDistance):
                                    min_editDistance = edit_distance 
                                    et_matchedKey = et_key
                                    et_matchedVal = et_value
                                    #print("et: "+str((et_key, et_value)))
                                    
                            if(et_matchedVal.strip()!=""):
                                if("|" in gt_value):
                                    values = gt_value.split("|")
                                    breaked=False
                                    for val in values:
                                        if(equalityCheck(val, et_matchedVal)):
                                            TP_num_Content+=1
                                            dbPediaEval.info("macthed et: "+str((et_matchedKey, et_matchedVal,(val) )))
                                            breaked = True
                                            break
                                    
                                    if(breaked==False):
                                        FP_num_Content+=1
                                        dbPediaEval.info("!!!NOT macthed et: "+str((et_matchedKey, et_matchedVal, (gt_value))))
                                        
                                    
                                elif(equalityCheck((gt_value), (et_matchedVal))):
                                    dbPediaEval.info("macthed et: "+str((et_matchedKey, et_matchedVal,(gt_value) )))
                                    TP_num_Content+=1
                                else:
                                  
                                    dbPediaEval.info("!!!NOT macthed et: "+str((et_matchedKey, et_matchedVal, (gt_value))))
                                    FP_num_Content+=1
                            else:
                                dbPediaEval.info("!!!et_matchedVal Empty: "+str((gt_key, (et_matchedKey))))
                                FN_num_Content+=1
                                
                            #print("----")
                                
                                
                                    
                                 
                        Col_Content_Precision=0
                        Col_Content_Recall = 0
                        
                        if(TP_num_Content+FP_num_Content!=0):
                            Col_Content_Precision = TP_num_Content/(TP_num_Content+FP_num_Content)
                        if(TP_num_Content+FN_num_Content!=0):
                            Col_Content_Recall  = TP_num_Content/(TP_num_Content+FN_num_Content)
                            
                        Col_Content_FMeasure = 0
                        if(Col_Content_Precision+Col_Content_Recall!=0):
                            Col_Content_FMeasure = 2* Col_Content_Precision*Col_Content_Recall/(Col_Content_Precision+Col_Content_Recall)
                                
                            
                        
                        dbPediaEval.info("")
                        dbPediaEval.info("")
                        dbPediaEval.info("---------------------------------------------")
                        dbPediaEval.info("Col: "+str(ET_FirstLine[ET_indx])+" From Extended Table is Matched with Col: "+str(GT_FirstLine[GT_indx])+" From Ground Truth with Precision: "+str(Col_Content_Precision)+" and Recall: "+str(Col_Content_Recall)+" and FMeasure: "+str(Col_Content_FMeasure))
                        dbPediaEval.info("---------------------------------------------")
                        dbPediaEval.info("")
                        dbPediaEval.info("")
                        
                        
                        
                        AVG_Content_Precision = (AVG_Content_Precision*Col_cnt+Col_Content_Precision) / (Col_cnt+1)
                        AVG_Content_Recall = (AVG_Content_Recall*Col_cnt+Col_Content_Recall) / (Col_cnt+1)
                        
                        Col_cnt += 1     
                        
                        
                        
            AVG_Content_FMeasure = 0
            if(AVG_Content_Precision+AVG_Content_Recall!=0):           
                AVG_Content_FMeasure = 2* AVG_Content_Precision*AVG_Content_Recall/(AVG_Content_Precision+AVG_Content_Recall)
            
            dbPediaEval.info("")
            dbPediaEval.info("")
            dbPediaEval.info("---------------------------------------------")
            dbPediaEval.info("---------------------------------------------")
            dbPediaEval.info("AVG_Content_Precision: "+str(round(AVG_Content_Precision,2))+", AVG_Content_Recall:"+str(round(AVG_Content_Recall,2))+" ,AVG_Content_FMeasure: "+str(round(AVG_Content_FMeasure,2)))
            dbPediaEval.info("---------------------------------------------")
            dbPediaEval.info("---------------------------------------------")
            dbPediaEval.info("")
            dbPediaEval.info("")
            

            FN_num_RelatedCol = relCol_Cnt - len(relCol_Collection) 
            TP_num_RelatedCol = len(relCol_Collection)  #Ignoring Duplicates
            num_of_Retrieved_Cols = len(ET_FirstLine)-1
            num_of_Mapped_cols = len(ET_DBPedia_Mapping)-1
            FP_num_RelatedCol+= (num_of_Retrieved_Cols - num_of_Mapped_cols)
                
            if(TP_num_RelatedCol+FP_num_RelatedCol!=0):
                Related_Cols_Precision = TP_num_RelatedCol/(TP_num_RelatedCol+FP_num_RelatedCol)
            if(TP_num_RelatedCol+FN_num_RelatedCol!=0):
                Related_Cols_Recall  = TP_num_RelatedCol/(TP_num_RelatedCol+FN_num_RelatedCol)
            
            Related_Cols_Fmeasure = 0
            if(Related_Cols_Precision+Col_Content_Recall!=0):           
                Related_Cols_Fmeasure = 2* Related_Cols_Precision*Related_Cols_Recall/(Related_Cols_Precision+Related_Cols_Recall)
                
            dbPediaEval.info("")
            dbPediaEval.info("")
            dbPediaEval.info("---------------------------------------------")
            dbPediaEval.info("---------------------------------------------")
            dbPediaEval.info("Related_Cols_Precision: "+str(round(Related_Cols_Precision,2))+", Related_Cols_Recall:"+str(round(Related_Cols_Recall,2))+" ,Related_Cols_Fmeasure: "+str(round(Related_Cols_Fmeasure,2)))
            dbPediaEval.info("---------------------------------------------")
            dbPediaEval.info("---------------------------------------------")
            dbPediaEval.info("")
            dbPediaEval.info("")
                        
 
            grouping_Factor = len(Unique_Mapped_Cols)/len(Mapped_Cols)
            
            dbPediaEval.info("")
            dbPediaEval.info("")
            dbPediaEval.info("---------------------------------------------")
            dbPediaEval.info("---------------------------------------------")
            dbPediaEval.info("grouping_Factor: "+str(round(grouping_Factor,2)))
            dbPediaEval.info("---------------------------------------------")
            dbPediaEval.info("---------------------------------------------")
            dbPediaEval.info("")
            dbPediaEval.info("")
       
def Create_Manual_GroundTruth(QueryKeysMapping_Path):
    URIQueryKeys_Mapping=[]
    
    with open(QueryKeysMapping_Path) as QueryKeysMapping_File:
        QueryKeysMapping = csv.reader(QueryKeysMapping_File)
        next(QueryKeysMapping)
        for row in QueryKeysMapping:
            if(row[1].strip()!=""):
                URIQueryKeys_Mapping.append((row[0], row[1]))
                

   # URIQueryKeys = Populate_QueryKeys_From_ExcelFile(QueryKeysExcelFile)
    
    QueryKeys = []
    for q in URIQueryKeys_Mapping:
        QueryKeys.append(q[0])
        
    FilteredTables = FilterTables(QueryKeys)
    cluster = Cluster(FilteredTables)
    ExtendedTable = TableExtension_TableStyle(QueryKeys, cluster)    
    Write_Table_To_CSV(ExtendedTable, "C:/Saeed_Local_Files/GroundTruth/Manual_GroundTruth.csv")  
    
def Cleaning_DBPediaAsTable_GT_By_Query(queryKeys, DBPediaAsTableFile, Null_Value_Threhold, FinalGTFile):
    ####
    #Filtering queryKeys rows from DBPediaAsTable file,
    #Excluding those columns with null values higher than a threshold
    ####
    
    print(queryKeys)
    
    #Reading the ground truth
    ground_truth = Convert_CSVFile_To_Table(groundTruth_csv)
    #print(len(ground_truth))
    
    #Finding queryKeys rows from ground truth
    filtered_gt=[]
    
    #First, adding the header row to the filtered ground truth
    first_row=[]
    for i in range(1, len(ground_truth)):
        filtered_gt.append([ground_truth[i][0]])
    
    #Finding the best matching row in the ground truth
    for q_key in queryKeys[0]:
        min_dist = 1
        min_dist_row_idx = -1
        hit_list=[] #Those with apporximate match = 1
        for i in range(4, len(ground_truth[1])):#labels start at row 4
            dist = Edit_Distance_Ratio(q_key, ground_truth[1][i])
            if(dist<=min_dist):
                min_dist = dist
                min_dist_row_idx = i
                
                if(dist==0):
                    #each element of hit list: (key,row_idx)
                    hit_list.append((ground_truth[1][i],i))
                    
        #when approximate match finds more than one hit, we select the most similar one
        if(len(hit_list)>1):
            min_dist = 1
            for elem in hit_list:
                dist = Char_Level_Edit_Distance(elem[0],q_key)
                if(dist<min_dist):
                    min_dist_row_idx = elem[1]
                    min_dist = dist
                    
        print(str(q_key)+" matched with "+str(ground_truth[1][min_dist_row_idx]))
        #adding the matched row to the filtered gt table
        filtered_gt[0].append(q_key)
        for i in range(2, len(ground_truth)):
            filtered_gt[i-1].append(str(ground_truth[i][min_dist_row_idx]))

            
    
    #excluding those columns with null values higher than a threshold 
    col_idx=0
    while(col_idx<len(filtered_gt)):
        null_value_cnt = 0
        for val in filtered_gt[col_idx]:
            if val == "NULL":
                null_value_cnt += 1
                
        to_be_deleted = False
        
        #Exclude next columns of those that have "_label" in their header name
        if("_label" in filtered_gt[col_idx][0]):
            print("popped column idx "+str(filtered_gt[col_idx+1][0]))
            filtered_gt.pop(col_idx+1)
            
            
        #Exclude the column if null_value_cnt ratio is  higher than Null_Value_Threhold
        if((null_value_cnt/(len(filtered_gt[0])-1)>Null_Value_Threhold)):
            print("popped column idx "+str(filtered_gt[col_idx][0]))
            to_be_deleted = True
            


        if(filtered_gt[col_idx][0]=="22-rdf-syntax-ns#type_label"):
           to_be_deleted = True
        if(filtered_gt[col_idx][0]=="rdf-schema#seeAlso_label"):
           to_be_deleted = True
        if(filtered_gt[col_idx][0]=="owl#differentFrom_label"):
           to_be_deleted = True
           
            
            
        #sync the offset for the col_idx if the column is deleted,
        #And pop the column if it is deleted
        if(to_be_deleted==False):
            col_idx += 1
        else:
            filtered_gt.pop(col_idx)
            
            
    #writing the filtered gt table into csv file
    Write_Table_To_CSV(filtered_gt, FinalGTFile)
  
def Evaluate_By_PreDefined_GroundTruth(extendedTable_csv, groundTruth_csv):
    
    ground_truth = Convert_CSVFile_To_Table(groundTruth_csv)
    result = Convert_CSVFile_To_Table(extendedTable_csv)
    #print(ground_truth)
    #print(result)
    
    value_match_distance_threshold = 0.35
    
    
    #mapping the keys of the result table to ground truth
    result_gt_key_map = {}
    for i in range(1,len(result[0])):
        min_distance = 1
        key_match_idx = -1
        for j in range(1,len(ground_truth[0])):
            dist = Edit_Distance_Ratio(result[0][i],ground_truth[0][j])
            if(dist<min_distance):
                min_distance = dist
                key_match_idx = j

        
        result_gt_key_map[i]=key_match_idx
    
    #print("result_gt_key_map: "+str(result_gt_key_map))
    
    #mapping cols of the result table to ground truth based on value match:
    result_gt_col_map = {}
    col_map_threshold = 0.2
    for i in range(1,len(result)):
        max_match_percentage = 0
        match_idx = -1
        for j in range(1,len(ground_truth)):
            num_of_matches=0
            number_of_vals_in_gt = 0
            #print(len(ground_truth[j]))
            for k in range(1,len(ground_truth[j])):
                #print(k)
                if(ground_truth[j][result_gt_key_map[k]]!="NULL"):
                    number_of_vals_in_gt += 1
                    if(Edit_Distance_Ratio(result[i][k],ground_truth[j][result_gt_key_map[k]])<value_match_distance_threshold):
                        num_of_matches+=1
                    #break
            
            match_percentage =  num_of_matches/number_of_vals_in_gt
            if (match_percentage>col_map_threshold):
                if(match_percentage>max_match_percentage):
                    max_match_percentage = match_percentage
                    match_idx = j
        
        result_gt_col_map[i]=match_idx
    
    #print(result_gt_col_map)
    for result_idx in result_gt_col_map:
        if result_gt_col_map[result_idx]!=-1:
            print(str(result_idx)+": "+str(result[result_idx][0])+" in result table matched to "+str(result_gt_col_map[result_idx])+": "+str(ground_truth[result_gt_col_map[result_idx]][0])+" in ground truth ")
    
    #calculating extended_column_cover
    extended_column_cover_set=set()
    for col_match in result_gt_col_map:
        extended_column_cover_set.add(result_gt_col_map[col_match])
    
    extended_column_cover_set.remove(-1)
    #print(extended_column_cover_set)
    column_extension_coverage = len(extended_column_cover_set)/(len(ground_truth)-1)
    
    
    
    #calculating grouping factor
    num_of_mapped_cols = 0
    for col_match in result_gt_col_map:
        if(result_gt_col_map[col_match]!=-1):
            num_of_mapped_cols+=1
    
    grouping_factor = len(extended_column_cover_set)/num_of_mapped_cols
    
    
    
    #calculating average column precision and recall for the mapped columns
    avg_col_precision = 0 
    avg_col_recall = 0
    avg_f_measure = 0 
    total_number_of_col_match = 0
    for col_match in result_gt_col_map:
        if(result_gt_col_map[col_match]!=-1):
            total_number_of_col_match += 1
            i= col_match
            j= result_gt_col_map[col_match]
            value_match = 0
            number_of_vals_in_gt = 0
            number_of_vals_in_res = 0
            for k in range(1,len(result[i])):
                if(ground_truth[j][result_gt_key_map[k]]!="NULL"):
                    number_of_vals_in_gt += 1
                    if(result[i][k]!=""):
                        number_of_vals_in_res+=1
                        
                        if(Edit_Distance_Ratio(result[i][k],ground_truth[j][result_gt_key_map[k]])<value_match_distance_threshold):
                            value_match +=1
                            
            col_precision = value_match/number_of_vals_in_gt
            col_recall = number_of_vals_in_res/number_of_vals_in_gt
            f_measure = 2*col_precision*col_recall/(col_precision+col_recall)
            
            avg_col_precision+=col_precision
            avg_col_recall+=col_recall
            avg_f_measure+=f_measure
            
    print("total_number_of_col_matches with ground truth: "+str(total_number_of_col_match))
    print("column_extension_coverage: "+str(column_extension_coverage))
    print("grouping_factor: "+str(grouping_factor))
    
    
    avg_col_precision = avg_col_precision/total_number_of_col_match
    avg_col_recall = avg_col_recall/total_number_of_col_match
    avg_f_measure = avg_f_measure/total_number_of_col_match
    
    print("avg_content_precision: "+str(avg_col_precision))
    print("avg_content_recall: "+str(avg_col_recall))
    print("avg_content_f_measure: "+str(avg_f_measure))
            
            #print(col_precision, col_recall)
                 
def Table_Extension_Quality_Comparison_with_DBPedia():
    global dbPedia
    input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/All-Countries.xlsx"
    proposed_outwards_extension, proposed_inwards_extension = TableExtension(Populate_QueryTable_From_ExcelFile(input_query_file),0, False)
    #TableExtension(Populate_QueryTable_From_ExcelFile(input_query_file),0, False)
    #TableExtension(Populate_QueryTable_From_ExcelFile(input_query_file),0, False)
    #integrated_extension, integrated_inwards_outwards_extension = Integrate_Inwards_Outwards(Col_Overlap_TH, Input_Table) 
    dbPedia = DBPedia()
    
    eval_parent_path = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/proposed_"
    proposed_outwards_eval = Evaluate_Extended_Table(True, proposed_outwards_extension, eval_parent_path+"outwards.txt")
    proposed_inwards_eval = Evaluate_Extended_Table(False, proposed_inwards_extension, eval_parent_path+"inwards.txt")

    del dbPedia
    
def Mannheim_Extension_Quality_Comparison_with_DBPedia():
    global dbPedia
    input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/All-Countries.xlsx"
    integrated_extension, integrated_inwards_outwards_extension = Mannheim_TableExtension(Populate_QueryTable_From_ExcelFile(input_query_file),0, False)
    dbPedia = DBPedia()
    Evaluate_Outwards_Extended_Table(integrated_extension)
    #Evaluate_Inwards_Extended_Table([])
    
    
def Evaluate_Outwards_Extension(Extended_Table, evaluate_logfile, content_prec_threshold):
    ###
    # The Evaluation works in this way that each extended column is first mapped to an EAB where the first column is the key column and the second column is the extended column.
    # Then, each (Key,Value) is verified over dbPedia, in this way that, each (K,V) is mapped to a set of predicates P either in the form of DBPedia triple <K,P,V> or <V,P,K>
    # each column is tagged based on the most majority Predicate vote of its rows.
    # a column will be tagged as 'relevant' if the percentage of the tagged predicate in that column is higher than a Threshold (Column_Tag_Threhsold)
    ###
    
    print("\n\n\n\n################ Evaluate_Outwards_Extended_Table #################\n\n\n\n")

    #Traversibg each colunm
    #Assume the key column in the first column
    col_evals=[]
    for col_idx in range(1,len(Extended_Table)):
        #Traversing the values of column
        row_predicates=[]
        header = ""
        null_cnt = 0
        for row_idx in range(1,len(Extended_Table[0])):
            
            Subject = set()
            Object = set()
            
            if(Extended_Table[0][row_idx].strip()!=""):
                Subject = DBPedia_Lookup(Extended_Table[0][row_idx])
            
            if(Extended_Table[col_idx][row_idx] != ""):
                Object= DBPedia_Lookup(Extended_Table[col_idx][row_idx])
                
            if(Extended_Table[col_idx][row_idx]==""):
                null_cnt+=1
                
            if(len(Subject)!=0 and len(Object)!=0):
                row_predicates.append(DBPedia_GetPredicate(Subject, Object))
            else:
                row_predicates.append([])
        
        #print("\n\n################ row_predicates results #################\n\n")
        #print("column: "+str(Extended_Table[col_idx]))
        #print(row_predicates)
        #Finding the majority of tags in a column
        header = str(col_idx)+"-"+Extended_Table[col_idx][0]
        TagCountMap = {}
        for pred_list in row_predicates:
            for pred in pred_list:
                if(pred in TagCountMap):
                    TagCountMap[pred]+=1
                else:
                    TagCountMap[pred]=1
        sorted_Counts = sorted(TagCountMap.items(), key=operator.itemgetter(1), reverse=True)
        if(len(sorted_Counts)>0):
            column_tag=[]
            for elem in sorted_Counts:
                elem_prec = elem[1]/len(row_predicates)
                if (elem_prec>content_prec_threshold):
                    column_tag.append((elem[0],elem_prec))
            tag_cnt = sorted_Counts[0][1]
            prec = tag_cnt / len(row_predicates)
            recall = 1- (null_cnt/len(row_predicates))
            eval_res = (prec, recall, column_tag, header)
            
        else:
            eval_res = (0, 0, "", header)
            
        col_evals.append(eval_res)
        
        evaluate_logfile.write(str(header)+", Eval Result: "+str(eval_res)+"\n")
        
    return(col_evals)

def Evaluate_Inwards_Extension(Extended_Table, evaluate_logfile, content_prec_threshold):
    ###
    #-- depricated --> "For Inwards tables, a column is tagged as the majority predicate of the (Key,Value) pairs."
    # in case of a set-valued cell in column $c$, we tag the cell's predicate as the most frequent matched predicate in that set
    #precision is  calculated in this way that if at least one value is correct for a key, then that key is tagged as correct
    #recall is calculated in this way that if at least one value is outputted for a key, then that key is tagged as 'has value'
    #then, the recall is the average of keys that have value for that column!
    ###

    #Traversibg each colunm
    #Assume the key column in the first column
    col_evals=[]
    inwards_col = False
    for col_idx in range(1,len(Extended_Table)):
        if(inwards_col==False):
            if(Extended_Table[col_idx][0]=="###"):
               inwards_col = True
            continue
                   
        
        #Checking the (Key,Value) Pairs
        #Traversing the values of column
        row_predicates=[]
        header = ""
        Subject = []
        Object = []
        key_has_inwards_val = False
        key_size = 0
        inwards_keyval_cnt=0
        for row_idx in range(1,len(Extended_Table[0])):
            if(str(Extended_Table[0][row_idx]).strip()!=""):
                Subject = DBPedia_Lookup(Extended_Table[0][row_idx])
                key_size+=1
                
                #since the inwards vals are changed for the new key, recall should reset:
                key_has_inwards_val = False
                continue
            
            if(str(Extended_Table[col_idx][row_idx]).strip()==""):
                continue
            
            Object= DBPedia_Lookup(Extended_Table[col_idx][row_idx])
            
            #means that for that key, there is at least one inwards value
            if(key_has_inwards_val==False):
                inwards_keyval_cnt+=1
                
            key_has_inwards_val=True
                
            if(len(Subject)!=0 and len(Object)!=0):
                row_predicates.append(DBPedia_GetPredicate(Subject, Object))
            else:
                row_predicates.append([])
        
        inwards_col = False
        
        #print("\n\n################ row_predicates results #################\n\n")
        #print("column: "+str(Extended_Table[col_idx]))
        #print(row_predicates)
        #Finding the majority of tags in a column
        header = str(col_idx)+"-"+Extended_Table[col_idx][1]
        TagCountMap = {}
        for pred_list in row_predicates:
            for pred in pred_list:
                if(pred in TagCountMap):
                    TagCountMap[pred]+=1
                else:
                    TagCountMap[pred]=1
        sorted_Counts = sorted(TagCountMap.items(), key=operator.itemgetter(1), reverse=True)
        if(len(sorted_Counts)>0):
            column_tag=[]
            for elem in sorted_Counts:
                elem_prec = elem[1]/len(row_predicates)
                if (elem_prec>content_prec_threshold):
                    column_tag.append((elem[0],elem_prec))
            tag_cnt = sorted_Counts[0][1]
            prec = tag_cnt / len(row_predicates)
            recall = inwards_keyval_cnt/len(row_predicates)
            eval_res = (prec, recall, column_tag, header)
            
        else:
            eval_res = (0, 0, "", header)
            
        col_evals.append(eval_res)
        
        evaluate_logfile.write(str(header)+", Eval Result: "+str(eval_res)+"\n")
        
    return col_evals

def Evaluate_Extended_Table(Is_Outwards, Extended_Table, evaluate_logfile_path, stat=None):
    
    print("\n\n\n\n################ Evaluate_Extended_Table #################\n\n\n\n")

    evaluate_logfile = open(evaluate_logfile_path, "w", encoding="utf-8")
    
    col_evals=[]
    content_prec_threshold = 0 
    
    if(Is_Outwards):
        content_prec_threshold = 0.2
        col_evals = Evaluate_Outwards_Extension(Extended_Table, evaluate_logfile, content_prec_threshold)
    else:
        content_prec_threshold = 0.2
        col_evals = Evaluate_Inwards_Extension(Extended_Table, evaluate_logfile, content_prec_threshold)

    grouping_factor=0
    
    
    related_columns = []
    related_column_set = set()
    grouping_factor = 0
    
    for col_eval in col_evals:
        content_prec = col_eval[0]
        if(content_prec> content_prec_threshold):
            related_columns.append(col_eval)
            related_column_set.add(col_eval[2][0][0])
            #average_column_content_precision +=col_eval[0]
            #average_column_content_recall +=col_eval[1]
        
    
            
    #average_column_content_precision = average_column_content_precision/len(col_evals)
    #average_column_content_recall = average_column_content_recall / len(col_evals)
    

        
    #Calculating Grouping Factor:
    
    #Initilaizing mapped_relatedCols_list [(tag_set,mapped_idx)] in order to find duplicate columns that are subset of each other.
    mapped_relatedCols_list=[]
    for i in range(len(related_columns)):
        col_tag = related_columns[i][2]
        prec_recall = (related_columns[i][0], related_columns[i][1])
        tag_set = set()
        for tag in col_tag:
            tag_set.add(tag[0])
        
        mapped_relatedCols_list.append([tag_set,-1, prec_recall])
        
    #update mapped_idx
    for i in range(len(mapped_relatedCols_list)):
        if(mapped_relatedCols_list[i][1]==-1):
            mapped_relatedCols_list[i][1]=i
            a=mapped_relatedCols_list[i][0]
            for j in range(i+1, len(mapped_relatedCols_list)):
                if(mapped_relatedCols_list[j][1]==-1):
                    b= mapped_relatedCols_list[j][0]
                    if(a.issubset(b) or b.issubset(a)):
                        mapped_relatedCols_list[j][1]=i
                        
                        
    #calculating average content precesion and recall
    #content precision and recall Should not be averaged over duplicated and non-duplicated column together,
    #they should be averaged over duplicate columns first, 
    #then those duplicate columns should considered as collapsed 
    #and the overal average should be over non-duplicate columns
    
    unique_col_quality_dict = {} # {col1:[prec_recall1], col2:[prec_recall2,prec_recall3]}
    for col in mapped_relatedCols_list:
        if(col[1] in unique_col_quality_dict):
            unique_col_quality_dict[col[1]].append(col[2])
        else:
            unique_col_quality_dict[col[1]] = [col[2]]
            
            
    average_column_content_precision=0
    average_column_content_recall=0
    average_column_content_fMeasure=0
    
    for unique_col in unique_col_quality_dict:
        partial_prec = 0
        partial_recall = 0
        partial_fMeasure = 0
        
        for (p,r) in unique_col_quality_dict[unique_col]:
            partial_prec += p
            partial_recall += r
            partial_fMeasure += (2*p*r)/(p+r)
            
        average_column_content_precision += partial_prec/len(unique_col_quality_dict[unique_col])
        average_column_content_recall += partial_recall/len(unique_col_quality_dict[unique_col])
        average_column_content_fMeasure += partial_fMeasure/len(unique_col_quality_dict[unique_col])
        
    if(len(unique_col_quality_dict)>0):
        average_column_content_precision = average_column_content_precision/len(unique_col_quality_dict)
        average_column_content_recall = average_column_content_recall/len(unique_col_quality_dict)
        average_column_content_fMeasure = average_column_content_fMeasure/len(unique_col_quality_dict)
        
                
    grouping_factor = 0
    if(len(related_columns)>0):
        grouping_factor = len(unique_col_quality_dict)/len(related_columns)
    
    
    
    #output related columns:
    evaluate_logfile.write("\n\n\n\n############### Related Columns: ############################\n")
    for col in related_columns:
        evaluate_logfile.write(str(col)+"\n")
        
    
    evaluate_logfile.write("\n\n\n\n###############Final Result: ############################\n")
    evaluate_logfile.write("number_of_unique_related_cols: "+str(len(unique_col_quality_dict))+"\n")
    evaluate_logfile.write("average_column_content_precision: "+str(average_column_content_precision)+"\n")
    evaluate_logfile.write("average_column_content_recall: "+str(average_column_content_recall)+"\n")
    evaluate_logfile.write("average_column_content_fMeasure: "+str(average_column_content_fMeasure)+"\n")
    evaluate_logfile.write("grouping_factor: "+str(grouping_factor)+"\n")
    evaluate_logfile.write("Extended Columns: "+str(len(col_evals))+"\n")
    evaluate_logfile.write("stat: "+str(stat)+"\n")

    
    '''extended_table_size = 0
    if(len(Extended_Table)>0):
        extended_table_size = len(Extended_Table) - 1'''
        
    return (len(unique_col_quality_dict),average_column_content_precision, average_column_content_recall, average_column_content_fMeasure, grouping_factor, len(col_evals),stat, len(related_columns))


def New_ExtendedTable_Evaluation(Extended_Table, evaluate_logfile, content_prec_threshold):
    ##
    # It is assumed that each column in the extended table has set-values, regardless of inwards or outwards.
    ##
    ###
    # The Evaluation works in this way that each extended column is first mapped to an EAB where the first column is the key column and the second column is the extended column.
    # Then, each (Key,Value) is verified over dbPedia, in this way that, each (K,V) is mapped to a set of predicates P either in the form of DBPedia triple <K,P,V> or <V,P,K>
    # each column is tagged based on the most majority Predicate vote of its rows.
    # a column will be tagged as 'relevant' if the percentage of the tagged predicate in that column is higher than a Threshold (Column_Tag_Threhsold)
    # in case of a set-valued cell in column $c$, we tag the cell's predicate as the most frequent matched predicate in that set.
    ###
    
    print("\n\n\n\n################ Evaluate_Outwards_Extended_Table #################\n\n\n\n")
    print("Extended Table:\n")
    print(Extended_Table)

    #Traversibg each colunm
    #Assume the key column in the first column
    col_evals=[]
    for col_idx in range(1,len(Extended_Table)):
        #Traversing the values of column
        row_predicates=[]
        header = ""
        null_cnt = 0
        for row_idx in range(1,len(Extended_Table[0])):

            #traversing through the set of values in each cell
            cell_predicate_list=[]
            for cell_value in Extended_Table[col_idx][row_idx]:

                Subject = set()
                Object = set()
            
                if(Extended_Table[0][row_idx].strip()!=""):
                    Subject = DBPedia_Lookup(Extended_Table[0][row_idx])
            
                if(cell_value != ""):
                    Object= DBPedia_Lookup(cell_value)
                
                #if(Extended_Table[col_idx][row_idx]==""):
                    #null_cnt+=1
                

                #populate the predicates for each set_value in each cell
                if(len(Subject)!=0 and len(Object)!=0):
                    cell_predicate_list.append(DBPedia_GetPredicate(Subject, Object))
                #else:
                    #cell_predicate_list.append([])
                    
                if(Extended_Table[col_idx][0][0]=="atomic-valued"):
                    break
        
            if(len(Extended_Table[col_idx][row_idx])==0):
                null_cnt+=1
            row_predicates.append(cell_predicate_list)

        #print("\n\n################ row_predicates results #################\n\n")
        #print("column: "+str(Extended_Table[col_idx]))
        #print(row_predicates)

        #Finding the majority of tags in a column
        header = str(col_idx)+"-"+str(Extended_Table[col_idx][0])
        row_TagCountMap = {}
        print("row_predicates:")
        for elem in row_predicates:
            print(elem)
        for cell_predicate_list in row_predicates:
            print("cell_predicate_list:")
            print(str(cell_predicate_list))
            cell_TagCountMap = {}
            for pred_list in cell_predicate_list:
                print("pred_list: "+str(pred_list ))

                for pred in pred_list:
                    if(pred in cell_TagCountMap):
                        cell_TagCountMap[pred]+=1
                    else:
                        cell_TagCountMap[pred]=1
                        
            print("cell_TagCountMap:")
            print(cell_TagCountMap)
            cell_sorted_Counts = sorted(cell_TagCountMap.items(), key=operator.itemgetter(1), reverse=True)
            if(len(cell_sorted_Counts )>0):
                #cell_tag is the majority of the tags in the cell_predicate_list
                #in case of tie, the cell will be tagged with all tied majority tags 
                i=0
                while (len(cell_sorted_Counts)>i):
                    if(cell_sorted_Counts [0][1]==cell_sorted_Counts [i][1]):
                        cell_tag = cell_sorted_Counts [i][0]
                        print("cell_tag: "+str(cell_tag))
                        if(cell_tag in row_TagCountMap):
                            row_TagCountMap[cell_tag]+=1
                        else:
                            row_TagCountMap[cell_tag]=1
                        i+=1
                    else:
                        break
        
        
        
        print("row_TagCountMap:")
        print(row_TagCountMap)
        
        row_sorted_Counts = sorted(row_TagCountMap.items(), key=operator.itemgetter(1), reverse=True)
        #if(len(row_sorted_Counts)>0):
        column_tag=[]
        number_of_cells_with_some_values = len(row_predicates) -  null_cnt
        for elem in row_sorted_Counts:
            elem_prec = elem[1]/number_of_cells_with_some_values
            if (elem_prec>=content_prec_threshold):
                column_tag.append((elem[0],elem_prec))
        tag_cnt = 0
        if(len(row_sorted_Counts)>0):
            tag_cnt = row_sorted_Counts[0][1]
        
        prec = 0
        if(number_of_cells_with_some_values>0):
            prec = tag_cnt / number_of_cells_with_some_values
        print("len(row_predicates):"+str(len(row_predicates)))
        print("number_of_cells_with_some_values:"+str(number_of_cells_with_some_values))
        recall = number_of_cells_with_some_values/len(row_predicates)
        print("recall:"+str(recall))
        eval_res = (prec, recall, column_tag, header, col_idx)
        
        #else:
            #eval_res = (0, 0, "", header)
            
        col_evals.append(eval_res)
        
        evaluate_logfile.write(str(header)+", Eval Result: "+str(eval_res)+"\n")
    
    print(col_evals)
    #sys.exit()
    return(col_evals)

def New_Final_Table_Evaluation(Extended_Table, evaluate_logfile_path, stat=None):
    #New Evaluation for extended tables regardless of inwards or outwards
    
    print("\n\n\n\n################ Evaluate_Extended_Table #################\n\n\n\n")

    evaluate_logfile = open(evaluate_logfile_path, "w", encoding="utf-8")
    
    col_evals=[]
    
    content_prec_threshold = 0.2
    col_evals = New_ExtendedTable_Evaluation(Extended_Table, evaluate_logfile, content_prec_threshold)
   
    grouping_factor=0
    
    related_columns = []
    related_column_set = set()
    grouping_factor = 0
    
    for col_eval in col_evals:
        content_prec = col_eval[0]
        if(content_prec>= content_prec_threshold):
            related_columns.append(col_eval)
            related_column_set.add(col_eval[2][0][0])
            #average_column_content_precision +=col_eval[0]
            #average_column_content_recall +=col_eval[1]
            
    #average_column_content_precision = average_column_content_precision/len(col_evals)
    #average_column_content_recall = average_column_content_recall / len(col_evals)
    
    #Calculating Grouping Factor:
    
    #Initilaizing mapped_relatedCols_list [(tag_set,mapped_idx, col_idx)] in order to find duplicate columns that are subset of each other.
    mapped_relatedCols_list=[]
    for i in range(len(related_columns)):
        col_tag = related_columns[i][2]
        prec_recall = (related_columns[i][0], related_columns[i][1])
        tag_set = set()
        for tag in col_tag:
            tag_set.add(tag[0])
        col_idx = related_columns[i][4]
        
        mapped_relatedCols_list.append([tag_set,-1,prec_recall,col_idx])
        
    #update mapped_idx
    for i in range(len(mapped_relatedCols_list)):
        if(mapped_relatedCols_list[i][1]==-1):
            mapped_relatedCols_list[i][1]=i
            a=mapped_relatedCols_list[i][0]
            for j in range(i+1, len(mapped_relatedCols_list)):
                if(mapped_relatedCols_list[j][1]==-1):
                    b= mapped_relatedCols_list[j][0]
                    if(a.issubset(b) or b.issubset(a)):
                        mapped_relatedCols_list[j][1]=i
                        
                        
    #calculating average content precesion and recall
    #content precision and recall Should not be averaged over duplicated and non-duplicated column together,
    #they should be averaged over duplicate columns first, 
    #then those duplicate columns should considered as collapsed 
    #and the overal average should be over non-duplicate columns
    
    col_overlap_threshold = 0.3
    unique_col_quality_dict = {} # {col1:[(prec_recall1,col_idx1)], col2:[(prec_recall2,col_idx2),(prec_recall3,col_idx3)]}
    for col in mapped_relatedCols_list:
        if(col[1] in unique_col_quality_dict):
            #check if the data in Extended_Table[col[3]] overlaps with any of the Extended_Table[i]
            #where i is any of the col_idx already present in unique_col_quality_dict[col[1]]
            set_of_colIdxs = set()
            for elem in unique_col_quality_dict[col[1]]:
                set_of_colIdxs.add(elem[1])
            if highest_overlap_betw_colIdx_and_set_of_colIdxs(Extended_Table,col[3],set_of_colIdxs)>col_overlap_threshold:
                unique_col_quality_dict[col[1]].append((col[2],col[3]))
            else:
                #add "-1" until the entry gets unique
                new_entry = str(col[1])+"-1"
                while(new_entry in unique_col_quality_dict):
                    new_entry+="-1"
                unique_col_quality_dict[new_entry] = [(col[2],col[3])]
        else:
            unique_col_quality_dict[col[1]] = [(col[2],col[3])]
            
            
    average_column_content_precision=0
    average_column_content_recall=0
    average_column_content_fMeasure=0
    
    for unique_col in unique_col_quality_dict:
        partial_prec = 0
        partial_recall = 0
        partial_fMeasure = 0
        
        for (p_r,idx) in unique_col_quality_dict[unique_col]:
            p=p_r[0]
            r=p_r[1]
            partial_prec += p
            partial_recall += r
            partial_fMeasure += (2*p*r)/(p+r)
            
        average_column_content_precision += partial_prec/len(unique_col_quality_dict[unique_col])
        average_column_content_recall += partial_recall/len(unique_col_quality_dict[unique_col])
        average_column_content_fMeasure += partial_fMeasure/len(unique_col_quality_dict[unique_col])
        
    if(len(unique_col_quality_dict)>0):
        average_column_content_precision = average_column_content_precision/len(unique_col_quality_dict)
        average_column_content_recall = average_column_content_recall/len(unique_col_quality_dict)
        average_column_content_fMeasure = average_column_content_fMeasure/len(unique_col_quality_dict)
        
                
    grouping_factor = 0
    if(len(related_columns)>0):
        grouping_factor = len(unique_col_quality_dict)/len(related_columns)
    
    
    
    #output related columns:
    evaluate_logfile.write("\n\n\n\n############### Related Columns: ############################\n")
    for col in related_columns:
        evaluate_logfile.write(str(col)+"\n")
        
    
    evaluate_logfile.write("\n\n\n\n###############Final Result: ############################\n")
    evaluate_logfile.write("number_of_unique_related_cols: "+str(len(unique_col_quality_dict))+"\n")
    evaluate_logfile.write("average_column_content_precision: "+str(average_column_content_precision)+"\n")
    evaluate_logfile.write("average_column_content_recall: "+str(average_column_content_recall)+"\n")
    evaluate_logfile.write("average_column_content_fMeasure: "+str(average_column_content_fMeasure)+"\n")
    evaluate_logfile.write("grouping_factor: "+str(grouping_factor)+"\n")
    evaluate_logfile.write("Extended Columns: "+str(len(col_evals))+"\n")
    evaluate_logfile.write("stat: "+str(stat)+"\n")

    
    '''extended_table_size = 0
    if(len(Extended_Table)>0):
        extended_table_size = len(Extended_Table) - 1'''
        
    return (len(unique_col_quality_dict),average_column_content_precision, average_column_content_recall, average_column_content_fMeasure, grouping_factor, len(col_evals),stat, len(related_columns))

def highest_overlap_betw_colIdx_and_set_of_colIdxs(Extended_Table,col_idx,set_of_colIdxs):
    #first we cleanse the cols
    input_col_data=[]
    for i in range(len(Extended_Table[col_idx])):
        values=[]
        for elem in Extended_Table[col_idx][i]:
            values.append(string_Cleanse(elem))
        input_col_data.append(values)
        
    set_of_cols_data=[]
    for i in set_of_colIdxs:
        single_col_data=[]
        for j in range(len(Extended_Table[i])):
            values=[]
            for elem in (Extended_Table[i][j]):
                values.append(string_Cleanse(elem))
            single_col_data.append(values)
        set_of_cols_data.append(single_col_data)
        
    #now calculate the highest overlap:
    #to calculate the overlap, at least one matched value in a list suffices
    highest_overlap=0
    for single_col in set_of_cols_data:
        number_of_overlaps=0
        for i in range(len(single_col)):
            row_found=False
            #for j in range(len(input_col_data)):
            for elem in single_col[i]:
                #if(elem in input_col_data[j]):
                if(elem in input_col_data[i]):
                    number_of_overlaps+=1
                    row_found=True
                    break
                #if(row_found):
                    #break
        overlap = number_of_overlaps/len(single_col)
        if(overlap>highest_overlap):
            highest_overlap = overlap 
            
    return highest_overlap
                
            
def test():
    dbPedia = DBPedia()

    

def Test_Random_Queries():
    
    #The Goal is to log the Statistics of Random 10% of Test Corpus columns as query column
    WT_Corpus={}
    with open(get_Constant("Deduplicated_Merged_Pickle_URL"), 'rb') as f3:
        WT_Corpus = pickle.load(f3)

    #Retrieve all columns
    corpus_col_list = []
    for tableID in  WT_Corpus:
        table = WT_Corpus[tableID]["relation"]
        units = WT_Corpus[tableID]["units"]
        header_index = -1
        if(WT_Corpus[tableID]["hasHeader"]):
            header_index = WT_Corpus[tableID]["headerRowIndex"]
            
        for idx in range(len(table)):
            corpus_col_list.append((table[idx],units[idx], header_index))
            
    
     
        #sys.exit()

    
    evaluation.info("\n\n\n\n############### Results: ############################")
    
    
    global dbPedia
    dbPedia = DBPedia()
    
    et_proposed_result_list=[]
    vd_proposed_result_list=[]
    cg_proposed_result_list=[]
    
    et_proposed_stat_list=[]
    vd_proposed_stat_list=[]
    cg_proposed_stat_list=[]
    
    #mannheim_result_list=[]
    #infoGather_result_list=[]
    
    cntr = 0
    #while cntr < int(0.01*len(corpus_col_list)):
    
    eval_parent_folder = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries"
    output_source_file_path ="C:/Saeed_Local_Files/TestDataSets/T2D_233/output/integrated_inwards_outwards.xlsx"
    while (cntr <= 20):
        #print(len(corpus_col_list))
        rand = random.randint(0,len(corpus_col_list)-1)
        col = corpus_col_list[rand][0]
        unit = corpus_col_list[rand][1][0]
        header_idx =  corpus_col_list[rand][2]

        if(unit != "text"):
            continue
        
        #Get rid of the header
        header=""
        if(header_idx!=-1):
            header = col[header_idx]
            del col[header_idx]
            
        #Deleting redundant values
        queryKeys = []
        for item in col:
            if (item not in queryKeys):
                queryKeys.append(item)
        
        if("1" in queryKeys and "2" in queryKeys):
            continue
            
        #mannheim_extension_list = []
        #proposed_extension_list = []
        
        if(len(queryKeys)>=5 and len(queryKeys)<=50):
            cntr+=1
            queryTable = [queryKeys]
            
            
            input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/10_Countries.xlsx"
            #Currency-All
            #All-Countries
            #10_Countries
            #queryTable = Populate_QueryTable_From_ExcelFile(input_query_file)
            #test3
            
            '''et_proposed_outwards_extension, et_proposed_inwards_extension, et_outwards_stat, et_inwards_stat = TableExtension(queryTable,0,"Error_Threshold", False)
            #proposed_extension_list.append((proposed_outwards_extension, proposed_inwards_extension))
            exists = os.path.isfile(output_source_file_path)
            if exists:
                copyfile(output_source_file_path, eval_parent_folder+"/ed_proposed_"+str(cntr)+"_output.xlsx")
            
            vd_proposed_outwards_extension, vd_proposed_inwards_extension, vd_outwards_stat, vd_inwards_stat = TableExtension(queryTable,0,"Violation_Detectability", False)
            exists = os.path.isfile(output_source_file_path)
            if exists:
                copyfile(output_source_file_path, eval_parent_folder+"/vd_proposed_"+str(cntr)+"_output.xlsx")
            
            if(et_proposed_outwards_extension==[] and vd_proposed_outwards_extension==[]):
                cntr-=1
                continue'''
            
            cg_proposed_outwards_extension, cg_proposed_inwards_extension, cg_outwards_stat, cg_inwards_stat = TableExtension(queryTable,0,"Column_Grouping", False)
            
            if(cg_proposed_outwards_extension==[] and cg_proposed_inwards_extension==[]):
                cntr-=1
                continue
            
            exists = os.path.isfile(output_source_file_path)
            if exists:
                copyfile(output_source_file_path, eval_parent_folder+"/cg_proposed_"+str(cntr)+"_output.xlsx")
            
            #mannheim_outwards_extension=[]
            #mannheim_outwards_extension = Mannheim_TableExtension(queryTable,0, False)
            #mannheim_extension_list.append(mannheim_outwards_extension)
            
            
            
            
            
            
            
            
            #infoGather_outwards_extension = Extend_Table(queryTable[0])
            
            
            '''eval_parent_path = eval_parent_folder+"/ed_proposed_"
            et_proposed_outwards_eval = Evaluate_Extended_Table(True, et_proposed_outwards_extension, eval_parent_path+str(cntr)+"outwards.txt", et_outwards_stat)
            et_proposed_inwards_eval = Evaluate_Extended_Table(False, et_proposed_inwards_extension, eval_parent_path+str(cntr)+"inwards.txt", et_inwards_stat)
            
            eval_parent_path = eval_parent_folder+"/vd_proposed_"
            vd_proposed_outwards_eval = Evaluate_Extended_Table(True, vd_proposed_outwards_extension, eval_parent_path+str(cntr)+"outwards.txt",vd_outwards_stat)
            vd_proposed_inwards_eval = Evaluate_Extended_Table(False, vd_proposed_inwards_extension, eval_parent_path+str(cntr)+"inwards.txt", vd_inwards_stat)'''
            
            eval_parent_path = eval_parent_folder+"/cg_proposed_"
            cg_proposed_outwards_eval = Evaluate_Extended_Table(True, cg_proposed_outwards_extension, eval_parent_path+str(cntr)+"outwards.txt", cg_outwards_stat)
            cg_proposed_inwards_eval = Evaluate_Extended_Table(False, cg_proposed_inwards_extension, eval_parent_path+str(cntr)+"inwards.txt", cg_inwards_stat)
            
            #mannheim_outwards_eval =(0,0)
            '''eval_parent_path = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/mannheim_"
            mannheim_outwards_eval = Evaluate_Extended_Table(True, mannheim_outwards_extension, eval_parent_path+str(cntr)+"outwards.txt")
            mannheim_inwards_eval = Evaluate_Extended_Table(False, [], eval_parent_path+str(cntr)+"inwards.txt")
            
            
            eval_parent_path = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/infoGather_"
            infoGather_outwards_eval = Evaluate_Extended_Table(True, infoGather_outwards_extension, eval_parent_path+str(cntr)+"outwards.txt")
            infoGather_inwards_eval = Evaluate_Extended_Table(False, [], eval_parent_path+str(cntr)+"inwards.txt")'''
    
            #if(et_proposed_outwards_eval[0]>0 or et_proposed_inwards_eval[0]>0 or vd_proposed_outwards_eval[0]>0 or vd_proposed_inwards_eval[0]>0):
            if(cg_proposed_outwards_eval[0]>0 or cg_proposed_inwards_eval[0]>0 ):
                '''et_proposed_result_list.append((et_proposed_outwards_eval, et_proposed_inwards_eval, queryTable))
                #et_proposed_stat_list.append((et_outwards_stat,et_inwards_stat))
                vd_proposed_result_list.append((vd_proposed_outwards_eval, vd_proposed_inwards_eval, queryTable))
                #vd_proposed_stat_list.append((vd_outwards_stat,vd_inwards_stat))'''
                cg_proposed_result_list.append((cg_proposed_outwards_eval, cg_proposed_inwards_eval, queryTable))
                #cg_proposed_stat_list.append((cg_outwards_stat,cg_inwards_stat))
                #mannheim_result_list.append((mannheim_outwards_eval, mannheim_inwards_eval, queryTable))
                #infoGather_result_list.append((infoGather_outwards_eval, infoGather_inwards_eval, queryTable))
            else:
                cntr-=1
                continue
                
            
            

    
    '''for i in range(len(proposed_extension_list)):
            
        (proposed_outwards_extension, proposed_inwards_extension) = proposed_extension_list[i]
        mannheim_outwards_extension = mannheim_extension_list[i]'''
        


                

    '''eval_filePath = eval_parent_folder+"/et_proposed_table-eval.txt"
    total_extension_eval(et_proposed_result_list, eval_filePath)
    
    eval_filePath = eval_parent_folder+"/vd_proposed_table-eval.txt"
    total_extension_eval(vd_proposed_result_list, eval_filePath)'''
    
    eval_filePath = eval_parent_folder+"/cg_proposed_table-eval.txt"
    total_extension_eval(cg_proposed_result_list, eval_filePath)
    
    '''eval_filePath = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/mannheim_table-eval.txt"
    total_extension_eval(mannheim_result_list, eval_filePath)
    
    eval_filePath = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/infoGather_table-eval.txt"
    total_extension_eval(infoGather_result_list, eval_filePath)'''

def Evaluate_Comparisons():
    
    #The Goal is to log the Statistics of Comparisons between table extension methods    
    evaluation.info("\n\n\n\n############### Results: ############################")
    
                    
    #proposed_and_mannheim_source_file_path ="C:/Saeed_Local_Files/TestDataSets/T2D_233/output/0_outward.csv"
    #proposed_inwards_source_file_path ="C:/Saeed_Local_Files/TestDataSets/T2D_233/output/integrated_inwards_outwards.xlsx"
    proposed_and_mannheim_source_file_path = "C:/Saeed_Local_Files/TestDataSets/T2D_233/output/integrated_inwards_outwards.xlsx"
    infoGather_source_file_path= "C:/Saeed_Local_Files/InfoGather/" + Dataset +"_TableExtension_Result.csv"

    input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/countries-rand10.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/companies-rand10.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/films-rand10.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/games-rand10.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/languages-rand10.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/Languages.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/cities-rand10.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/currencies-rand10.xlsx"
    
    
    
    evalFiles_folder_path = "C:/Users/new/Google Drive/1-Uni/Thesis/Reports/Eval-Experiments/Table-Extension/"+input_query_file.split("/")[-1][:-5]+"/"
    exists = (os.path.isdir(evalFiles_folder_path ))
    if(not exists):
        os.mkdir(evalFiles_folder_path)
    
    eval_proposed_1_2_folder_path = evalFiles_folder_path+"/proposed_1_2/"
    exists = (os.path.isdir(eval_proposed_1_2_folder_path ))
    if(not exists):
        os.mkdir(eval_proposed_1_2_folder_path)
        
    eval_proposed_1_folder_path = evalFiles_folder_path+"/proposed_1/"
    exists = (os.path.isdir(eval_proposed_1_folder_path ))
    if(not exists):
        os.mkdir(eval_proposed_1_folder_path)
        
    eval_proposed_2_folder_path = evalFiles_folder_path+"/proposed_2/"
    exists = (os.path.isdir(eval_proposed_2_folder_path ))
    if(not exists):
        os.mkdir(eval_proposed_2_folder_path)
    
    eval_mannheim_folder_path = evalFiles_folder_path+"/mannheim/"
    exists = (os.path.isdir(eval_mannheim_folder_path ))
    if(not exists):
        os.mkdir(eval_mannheim_folder_path)
    
    eval_infoGather_folder_path = evalFiles_folder_path+"/infoGather/"
    exists = (os.path.isdir(eval_infoGather_folder_path ))
    if(not exists):
        os.mkdir(eval_infoGather_folder_path)

    queryKeys = Populate_QueryTable_From_ExcelFile(input_query_file)[0]
    
    #executing table exension algorithms: 
    print("proposed_et_1_2_proposed_outwards_extension Start ...")
    proposed_extension_1_2, et_outwards_stat_1_2, et_inwards_stat_1_2 = TableExtension([queryKeys],0,"Error_Threshold","1+2", False)
    print("proposed_et_1_2_proposed_outwards_extension End.")
    exists = os.path.isfile(proposed_and_mannheim_source_file_path)
    if(exists):
        copy2(proposed_and_mannheim_source_file_path, eval_proposed_1_2_folder_path)
    #exists = os.path.isfile(proposed_inwards_source_file_path)
    #if(exists):
        #copy2(proposed_inwards_source_file_path, eval_proposed_folder_path)
        
    '''    #executing table exension algorithms: 
    print("proposed_et_1_proposed_outwards_extension Start ...")
    proposed_extension_1, et_outwards_stat_1, et_inwards_stat_1 = TableExtension([queryKeys],0,"Error_Threshold","1", False)
    print("proposed_et_1_proposed_outwards_extension End.")
    exists = os.path.isfile(proposed_and_mannheim_source_file_path)
    if(exists):
        copy2(proposed_and_mannheim_source_file_path, eval_proposed_1_folder_path)
    #exists = os.path.isfile(proposed_inwards_source_file_path)
    #if(exists):
        #copy2(proposed_inwards_source_file_path, eval_proposed_folder_path)
        
        
        #executing table exension algorithms: 
    print("proposed_et_2_proposed_outwards_extension Start ...")
    proposed_extension_2, et_outwards_stat_2, et_inwards_stat_2 = TableExtension([queryKeys],0,"Error_Threshold","2", False)
    print("proposed_et_2_proposed_outwards_extension End.")
    exists = os.path.isfile(proposed_and_mannheim_source_file_path)
    if(exists):
        copy2(proposed_and_mannheim_source_file_path, eval_proposed_2_folder_path)
    #exists = os.path.isfile(proposed_inwards_source_file_path)
    #if(exists):
        #copy2(proposed_inwards_source_file_path, eval_proposed_folder_path)'''
    
        
    '''print("manheim_extension Start ...")
    manheim_extension = Mannheim_TableExtension([queryKeys],0)
    print("manheim_extension End.")
    exists = os.path.isfile(proposed_and_mannheim_source_file_path)
    if(exists):
        copy2(proposed_and_mannheim_source_file_path, eval_mannheim_folder_path )
    
    print("infoGather_extension Start ...")
    infoGather_extension = Extend_Table(queryKeys)
    #infoGather_extension = ""
    print("infoGather_extension End")
    exists = os.path.isfile(infoGather_source_file_path)
    if(exists):
        copy2(infoGather_source_file_path, eval_infoGather_folder_path)'''
    
    global dbPedia
    dbPedia = DBPedia()
    
    et_proposed_outwards_eval_1_2 = New_Final_Table_Evaluation(proposed_extension_1_2, eval_proposed_1_2_folder_path+"proposed_1_2.txt", {"outwards":et_outwards_stat_1_2,"inwards":et_inwards_stat_1_2})
    '''et_proposed_outwards_eval_1 = New_Final_Table_Evaluation(proposed_extension_1, eval_proposed_1_folder_path+"proposed_1.txt", {"outwards":et_outwards_stat_1,"inwards":et_inwards_stat_1})
    et_proposed_outwards_eval_2 = New_Final_Table_Evaluation(proposed_extension_2, eval_proposed_2_folder_path+"proposed_2.txt", {"outwards":et_outwards_stat_2,"inwards":et_inwards_stat_2})'''
    #et_mannheim_outwards_eval = New_Final_Table_Evaluation(manheim_extension, eval_mannheim_folder_path+"mannheim.txt")
    #et_infoGather_outwards_eval = New_Final_Table_Evaluation(infoGather_extension, eval_infoGather_folder_path+"infoGather.txt")
    
def Evaluate_FD_Detection_Methods_Separate_Queries():
    #The Goal is to log the Statistics of Comparisons between the FD Detection methods
    evaluation.info("\n\n\n\n############### Results: ############################")
    
                    
    #proposed_outwards_source_file_path ="C:/Saeed_Local_Files/TestDataSets/T2D_233/output/0_outward.csv"
    proposed_source_file_path ="C:/Saeed_Local_Files/TestDataSets/T2D_233/output/integrated_inwards_outwards.xlsx"

    input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/countries-rand10.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/companies-rand10.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/films-rand10.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/games-rand10.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/languages-rand10.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/Languages.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/cities-rand10.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/currencies-rand10.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/city-test.xlsx"
    
    
    
    evalFiles_folder_path = "C:/Users/new/Google Drive/1-Uni/Thesis/Reports/Eval-Experiments/FD_Detection/"+input_query_file.split("/")[-1][:-5]+"/"
    exists = (os.path.isdir(evalFiles_folder_path ))
    if(not exists):
        os.makedirs(evalFiles_folder_path)
    
    et_eval_folder_path = evalFiles_folder_path+"/et/"
    exists = (os.path.isdir(et_eval_folder_path ))
    if(not exists):
        os.mkdir(et_eval_folder_path)
        
    vd_eval_folder_path = evalFiles_folder_path+"/vd/"
    exists = (os.path.isdir(vd_eval_folder_path ))
    if(not exists):
        os.mkdir(vd_eval_folder_path)
        
    cg_eval_folder_path = evalFiles_folder_path+"/cg/"
    exists = (os.path.isdir(cg_eval_folder_path ))
    if(not exists):
        os.mkdir(cg_eval_folder_path)  
        
    coOccur_eval_folder_path = evalFiles_folder_path+"/coOccur/"
    exists = (os.path.isdir(coOccur_eval_folder_path ))
    if(not exists):
        os.mkdir(coOccur_eval_folder_path)  
        
    colSelTest_eval_folder_path = evalFiles_folder_path+"/colSelTest_/"
    exists = (os.path.isdir(colSelTest_eval_folder_path ))
    if(not exists):
        os.mkdir(colSelTest_eval_folder_path)
        
    
        
    
        
    queryKeys = Populate_QueryTable_From_ExcelFile(input_query_file)[0]
    
    rejected_cols_file_path = "Column_Sel_Logs/rejected_cols.txt"
    inwards_key_cols_file_path = "Column_Sel_Logs/inwards_key_cols.txt"
    inwards_nonKey_cols_file_path = "Column_Sel_Logs/inwards_nonKey_cols.txt"
    outwards_nonKey_cols_file_path = "Column_Sel_Logs/outwards_nonKey_cols.txt"
    
    #executing table exension algorithms: 
    print("proposed_et_proposed_outwards_extension Start ...")
    et_outwards_extension, et_outwards_stat, et_inwards_stat = TableExtension([queryKeys],0,"Error_Threshold", False)
    print("proposed_et_proposed_outwards_extension End.")
    exists = os.path.isfile(proposed_source_file_path)
    if(exists):
        copy2(proposed_source_file_path, et_eval_folder_path)
    exists = os.path.isfile(proposed_source_file_path)
    if(exists):
        copy2(proposed_source_file_path, et_eval_folder_path)
        
    copyfile(rejected_cols_file_path, et_eval_folder_path+"rejected_cols_file.txt")
    copyfile(inwards_key_cols_file_path, et_eval_folder_path+"inwards_key_cols_file.txt")
    copyfile(inwards_nonKey_cols_file_path, et_eval_folder_path+"inwards_nonKey_cols_file.txt")
    copyfile(outwards_nonKey_cols_file_path, et_eval_folder_path+"outwards_nonKey_cols_file.txt")
        
    print("proposed_vd_proposed_outwards_extension Start ...")
    vd_outwards_extension, vd_outwards_stat, vd_inwards_stat = TableExtension([queryKeys],0,"Violation_Detectability", False)
    print("proposed_vd_proposed_outwards_extension End.")
    exists = os.path.isfile(proposed_source_file_path)
    if(exists):
        copy2(proposed_source_file_path, vd_eval_folder_path)
    exists = os.path.isfile(proposed_source_file_path)
    if(exists):
        copy2(proposed_source_file_path, vd_eval_folder_path)
        
    copyfile(rejected_cols_file_path, vd_eval_folder_path+"rejected_cols_file.txt")
    copyfile(inwards_key_cols_file_path, vd_eval_folder_path+"inwards_key_cols_file.txt")
    copyfile(inwards_nonKey_cols_file_path, vd_eval_folder_path+"inwards_nonKey_cols_file.txt")
    copyfile(outwards_nonKey_cols_file_path, vd_eval_folder_path+"outwards_nonKey_cols_file.txt")
        
    print("proposed_cg_proposed_outwards_extension Start ...")
    cg_outwards_extension, cg_outwards_stat, cg_inwards_stat = TableExtension([queryKeys],0,"Column_Grouping", False)
    print("proposed_cg_proposed_outwards_extension End.")
    exists = os.path.isfile(proposed_source_file_path)
    if(exists):
        copy2(proposed_source_file_path, cg_eval_folder_path)
    exists = os.path.isfile(proposed_source_file_path)
    if(exists):
        copy2(proposed_source_file_path, cg_eval_folder_path)
        
    copyfile(rejected_cols_file_path, cg_eval_folder_path+"rejected_cols_file.txt")
    copyfile(inwards_key_cols_file_path, cg_eval_folder_path+"inwards_key_cols_file.txt")
    copyfile(inwards_nonKey_cols_file_path, cg_eval_folder_path+"inwards_nonKey_cols_file.txt")
    copyfile(outwards_nonKey_cols_file_path, cg_eval_folder_path+"outwards_nonKey_cols_file.txt")
        
    print("proposed_coOccur_proposed_outwards_extension Start ...")
    coOccur_outwards_extension, coOccur_outwards_stat, coOccur_inwards_stat = TableExtension([queryKeys],0,"Co-occurence_pruning", False)
    print("proposed_coOccur_proposed_outwards_extension End.")
    exists = os.path.isfile(proposed_source_file_path)
    if(exists):
        copy2(proposed_source_file_path, coOccur_eval_folder_path)
    exists = os.path.isfile(proposed_source_file_path)
    if(exists):
        copy2(proposed_source_file_path, coOccur_eval_folder_path)
        
    copyfile(rejected_cols_file_path, coOccur_eval_folder_path+"rejected_cols_file.txt")
    copyfile(inwards_key_cols_file_path, coOccur_eval_folder_path+"inwards_key_cols_file.txt")
    copyfile(inwards_nonKey_cols_file_path, coOccur_eval_folder_path+"inwards_nonKey_cols_file.txt")
    copyfile(outwards_nonKey_cols_file_path, coOccur_eval_folder_path+"outwards_nonKey_cols_file.txt")
    
    '''print("TableExtension_ColumnSelectionTest Start ...")
    colSelTest_outwards_extension, colSelTest_inwards_extension, colSelTest_outwards_stat, colSelTest_inwards_stat = TableExtension_ColumnSelectionTest([queryKeys],0,"", False)
    print("TableExtension_ColumnSelectionTest End.")
    exists = os.path.isfile(proposed_outwards_source_file_path)
    if(exists):
        copy2(proposed_outwards_source_file_path, colSelTest_eval_folder_path)
    exists = os.path.isfile(proposed_inwards_source_file_path)
    if(exists):
        copy2(proposed_inwards_source_file_path, colSelTest_eval_folder_path)
        

    
    copyfile(rejected_cols_file_path, colSelTest_eval_folder_path+"rejected_cols_file.txt")
    copyfile(inwards_key_cols_file_path, colSelTest_eval_folder_path+"inwards_key_cols_file.txt")
    copyfile(inwards_nonKey_cols_file_path, colSelTest_eval_folder_path+"inwards_nonKey_cols_file.txt")
    copyfile(outwards_nonKey_cols_file_path, colSelTest_eval_folder_path+"outwards_nonKey_cols_file.txt")'''
        
        
    
    global dbPedia
    dbPedia = DBPedia()
    
    et_outwards_eval = New_Final_Table_Evaluation( et_outwards_extension, et_eval_folder_path+"et_outwards.txt",{"outwards":et_outwards_stat, "inwards":et_inwards_stat})
    #et_inwards_eval = Evaluate_Extended_Table(False, et_inwards_extension, et_eval_folder_path+"et_inwards.txt",et_inwards_stat)
    
    vd_outwards_eval = New_Final_Table_Evaluation( vd_outwards_extension, vd_eval_folder_path+"vd_outwards.txt",{"outwards":vd_outwards_stat, "inwards":vd_inwards_stat})
    #vd_inwards_eval = Evaluate_Extended_Table(False, vd_inwards_extension, vd_eval_folder_path+"vd_inwards.txt",vd_inwards_stat)
    
    cg_outwards_eval = New_Final_Table_Evaluation( cg_outwards_extension, cg_eval_folder_path+"cg_outwards.txt",{"outwards":cg_outwards_stat, "inwards":cg_inwards_stat})
    #cg_inwards_eval = Evaluate_Extended_Table(False, cg_inwards_extension, cg_eval_folder_path+"cg_inwards.txt",cg_inwards_stat)
    
    coOccur_outwards_eval = New_Final_Table_Evaluation(coOccur_outwards_extension, coOccur_eval_folder_path+"coOccur_outwards.txt",{"outwards":coOccur_outwards_stat, "inwards":coOccur_inwards_stat})
    #coOccur_inwards_eval = Evaluate_Extended_Table(False, coOccur_inwards_extension, coOccur_eval_folder_path+"coOccur_inwards.txt",coOccur_inwards_stat)
    
    #colSelTest_outwards_eval = Evaluate_Extended_Table(True, colSelTest_outwards_extension, colSelTest_eval_folder_path+"colSelTest_outwards.txt",colSelTest_outwards_stat)
    #colSelTest_inwards_eval = Evaluate_Extended_Table(False, colSelTest_inwards_extension, colSelTest_eval_folder_path+"colSelTest_inwards.txt",colSelTest_inwards_stat)
    
def Random_Evaluate_Comparisons():
    
        
    #The Goal is to log the Statistics of Random 10% of Test Corpus columns as query column
    WT_Corpus={}
    with open(get_Constant("Deduplicated_Merged_Pickle_URL"), 'rb') as f3:
        WT_Corpus = pickle.load(f3)

    #Retrieve all columns
    corpus_col_list = []
    for tableID in  WT_Corpus:
        table = WT_Corpus[tableID]["relation"]
        units = WT_Corpus[tableID]["units"]
        header_index = -1
        if(WT_Corpus[tableID]["hasHeader"]):
            header_index = WT_Corpus[tableID]["headerRowIndex"]
            
        for idx in range(len(table)):
            corpus_col_list.append((table[idx],units[idx], header_index))
            
    
     
        #sys.exit()

    
    evaluation.info("\n\n\n\n############### Results: ############################")
    
    
    global dbPedia
    dbPedia = DBPedia()
    
    et_proposed_1_2_result_list=[]
    et_proposed_1_result_list=[]
    et_proposed_2_result_list=[]
    #et_proposed_stat_list=[]
    
    mannheim_result_list=[]
    infoGather_result_list=[]
    
    cntr = 0
    #while cntr < int(0.01*len(corpus_col_list)):
    
    eval_parent_folder = "C:/Users/new/Google Drive/1-Uni/Thesis/Reports/Eval-Experiments/Table-Extension/random_queries"
    #mannheim_source_file_path ="C:/Saeed_Local_Files/TestDataSets/T2D_233/output/0_outward.csv"
    proposed_and_mannheim_source_file_path ="C:/Saeed_Local_Files/TestDataSets/T2D_233/output/integrated_inwards_outwards.xlsx"
    infoGather_source_file_path= "C:/Saeed_Local_Files/InfoGather/" + Dataset +"_TableExtension_Result.csv"
    
    cumulative_query_keys = []
    
    print("print(len(corpus_col_list)):")
    print(len(corpus_col_list))
    while cntr < int(0.05*len(corpus_col_list)):
    #while (cntr < 2):
        #print(len(corpus_col_list))
        rand = random.randint(0,len(corpus_col_list)-1)
        col = corpus_col_list[rand][0]
        unit = corpus_col_list[rand][1][0]
        header_idx =  corpus_col_list[rand][2]

        if(unit != "text"):
            continue
        
        #Get rid of the header
        header=""
        if(header_idx!=-1):
            header = col[header_idx]
            del col[header_idx]
            
        #Deleting redundant values
        queryKeys = []
        for item in col:
            if (item not in queryKeys):
                queryKeys.append(item)
        
        if("1" in queryKeys and "2" in queryKeys):
            continue
            
        #mannheim_extension_list = []
        #proposed_extension_list = []
        
        if(len(queryKeys)>=5 and len(queryKeys)<=50 and '1' not in queryKeys):
            
            query_element_used = 0
            for elem in queryKeys:
                if elem in cumulative_query_keys:
                    query_element_used = 1
                    break
            
            if (query_element_used):
                continue
            
            cntr+=1
            queryTable = [queryKeys]
            cumulative_query_keys += [queryKeys]
            
            
            
            print("proposed_1_2: round "+str(cntr)+" starts ...")
            et_proposed_extension_1_2, et_outwards_stat_1_2, et_inwards_stat_1_2 = TableExtension(queryTable,0,"Error_Threshold","1+2", False)
            
            if(et_proposed_extension_1_2==[]):
                cntr-=1
                continue
            
            exists = os.path.isfile(proposed_and_mannheim_source_file_path)
            if exists:
                copyfile(proposed_and_mannheim_source_file_path, eval_parent_folder+"/et_proposed_1_2_"+str(cntr)+"_output.xlsx")
            

            '''print("proposed_1: round "+str(cntr)+" starts ...")
            et_proposed_extension_1, et_outwards_stat_1, et_inwards_stat_1 = TableExtension(queryTable,0,"Error_Threshold","1", False)
            
            exists = os.path.isfile(proposed_and_mannheim_source_file_path)
            if exists:
                copyfile(proposed_and_mannheim_source_file_path, eval_parent_folder+"/et_proposed_1_"+str(cntr)+"_output.xlsx")
                
                
    
            

            print("proposed_2: round "+str(cntr)+" starts ...")
            et_proposed_extension_2, et_outwards_stat_2, et_inwards_stat_2 = TableExtension(queryTable,0,"Error_Threshold","2", False)
            
            exists = os.path.isfile(proposed_and_mannheim_source_file_path)
            if exists:
                copyfile(proposed_and_mannheim_source_file_path, eval_parent_folder+"/et_proposed_2_"+str(cntr)+"_output.xlsx")'''
                
                
            
            
            print("mannheim: round "+str(cntr)+" starts ...")
            mannheim_outwards_extension = Mannheim_TableExtension(queryTable,0, False)
            exists = os.path.isfile(proposed_and_mannheim_source_file_path)
            if exists:
                copyfile(proposed_and_mannheim_source_file_path, eval_parent_folder+"/mannheim_"+str(cntr)+"_output.csv")
            #mannheim_extension_list.append(mannheim_outwards_extension)
            
            
            
            '''print("infoGather: round "+str(cntr)+" starts ...")
            infoGather_outwards_extension = Extend_Table(queryTable[0].copy())
            exists = os.path.isfile(infoGather_source_file_path)
            if exists:
                copyfile(infoGather_source_file_path, eval_parent_folder+"/infoGather_"+str(cntr)+"_output.csv")'''
            
            
            eval_parent_path = eval_parent_folder+"/et_proposed_1_2_"
            et_proposed_1_2_eval = New_Final_Table_Evaluation(et_proposed_extension_1_2, eval_parent_path+str(cntr)+"outwards.txt", {"outwards":et_outwards_stat_1_2,"inwards":et_inwards_stat_1_2})
            
            '''eval_parent_path = eval_parent_folder+"/et_proposed_1_"
            et_proposed_1_eval = New_Final_Table_Evaluation(et_proposed_extension_1, eval_parent_path+str(cntr)+"outwards.txt", {"outwards":et_outwards_stat_1,"inwards":et_inwards_stat_1})
            
            eval_parent_path = eval_parent_folder+"/et_proposed_2_"
            et_proposed_2_eval = New_Final_Table_Evaluation(et_proposed_extension_2, eval_parent_path+str(cntr)+"outwards.txt", {"outwards":et_outwards_stat_2,"inwards":et_inwards_stat_2})'''
            
            #New_Final_Table_Evaluation(proposed_extension, eval_proposed_folder_path+"proposed.txt", {"outwards":et_outwards_stat,"inwards":et_inwards_stat})
            #et_proposed_inwards_eval = Evaluate_Extended_Table(False, et_proposed_inwards_extension, eval_parent_path+str(cntr)+"inwards.txt", et_inwards_stat)
            
            '''eval_parent_path = eval_parent_folder+"/vd_proposed_"
            vd_proposed_outwards_eval = Evaluate_Extended_Table(True, vd_proposed_outwards_extension, eval_parent_path+str(cntr)+"outwards.txt",vd_outwards_stat)
            vd_proposed_inwards_eval = Evaluate_Extended_Table(False, vd_proposed_inwards_extension, eval_parent_path+str(cntr)+"inwards.txt", vd_inwards_stat)
            
            eval_parent_path = eval_parent_folder+"/cg_proposed_"
            cg_proposed_outwards_eval = Evaluate_Extended_Table(True, cg_proposed_outwards_extension, eval_parent_path+str(cntr)+"outwards.txt", cg_outwards_stat)
            cg_proposed_inwards_eval = Evaluate_Extended_Table(False, cg_proposed_inwards_extension, eval_parent_path+str(cntr)+"inwards.txt", cg_inwards_stat)'''
            
            #mannheim_outwards_eval =(0,0)
            eval_parent_path = eval_parent_folder+"/mannheim_"
            mannheim_eval = New_Final_Table_Evaluation(mannheim_outwards_extension, eval_parent_path+str(cntr)+"outwards.txt")
            #mannheim_inwards_eval = Evaluate_Extended_Table(False, [], eval_parent_path+str(cntr)+"inwards.txt")
            
            
            '''eval_parent_path = eval_parent_folder+"/infoGather_"
            infoGather_eval = New_Final_Table_Evaluation(infoGather_outwards_extension, eval_parent_path+str(cntr)+"outwards.txt")'''
            #infoGather_inwards_eval = Evaluate_Extended_Table(False, [], eval_parent_path+str(cntr)+"inwards.txt")
    
            #if(et_proposed_outwards_eval[0]>0 or et_proposed_inwards_eval[0]>0 or vd_proposed_outwards_eval[0]>0 or vd_proposed_inwards_eval[0]>0):
            if(et_proposed_1_2_eval[0]>0):
                et_proposed_1_2_result_list.append((et_proposed_1_2_eval, queryTable))
                #et_proposed_1_result_list.append((et_proposed_1_eval, queryTable))
                #et_proposed_2_result_list.append((et_proposed_2_eval, queryTable))
                #et_proposed_stat_list.append((et_outwards_stat,et_inwards_stat))
                #vd_proposed_result_list.append((vd_proposed_outwards_eval, vd_proposed_inwards_eval, queryTable))
                #vd_proposed_stat_list.append((vd_outwards_stat,vd_inwards_stat))
                #cg_proposed_result_list.append((cg_proposed_outwards_eval, cg_proposed_inwards_eval, queryTable))
                #cg_proposed_stat_list.append((cg_outwards_stat,cg_inwards_stat))
                mannheim_result_list.append((mannheim_eval, queryTable))
                #infoGather_result_list.append((infoGather_eval, queryTable))
            else:
                cntr-=1
                continue
                
            
            

    
    '''for i in range(len(proposed_extension_list)):
            
        (proposed_outwards_extension, proposed_inwards_extension) = proposed_extension_list[i]
        mannheim_outwards_extension = mannheim_extension_list[i]'''
        


                

    total_extension_eval_filePath = eval_parent_folder+"/et_proposed_1_2_table-eval.txt"
    total_extension_eval(et_proposed_1_2_result_list, total_extension_eval_filePath )
    
    '''total_extension_eval_filePath = eval_parent_folder+"/et_proposed_1_table-eval.txt"
    total_extension_eval(et_proposed_1_result_list, total_extension_eval_filePath )
    
    total_extension_eval_filePath = eval_parent_folder+"/et_proposed_2_table-eval.txt"
    total_extension_eval(et_proposed_2_result_list, total_extension_eval_filePath )'''
    
    '''eval_filePath = eval_parent_folder+"/vd_proposed_table-eval.txt"
    total_extension_eval(vd_proposed_result_list, eval_filePath)
    
    eval_filePath = eval_parent_folder+"/cg_proposed_table-eval.txt"
    total_extension_eval(cg_proposed_result_list, eval_filePath)'''
    
    total_extension_eval_filePath  = eval_parent_folder+"/mannheim_table-eval.txt"
    total_extension_eval(mannheim_result_list, total_extension_eval_filePath )
    
    #total_extension_eval_filePath  = eval_parent_folder+"/infoGather_table-eval.txt"
    #total_extension_eval(infoGather_result_list, total_extension_eval_filePath )
    
    
def total_extension_eval(result_list, eval_filePath):
    #Logging the results:
    #First, let's sort the result based on the contributed_percentage:
    #sorted_result_list = sorted(result_list, key=lambda item:(item[0][3]+item[1][3]), reverse=True)
    
    average_number_of_outwards_related_cols = 0
    average_outwards_column_content_precision = 0
    average_outwards_column_content_recall = 0
    average_outwards_column_content_fMeasure = 0
    average_outwards_grouping_factor = 0
    total_num_of_outwards_related_column = 0
    total_num_of_outwards_extended_columns = 0
    total_num_of_outwards_nonKey_FDs = 0
    total_num_of_outwards_Key_FDs = 0
    total_num_of_outwards_No_FDs = 0
    total_num_of_outwards_relCol_including_dup=0
    
    #average_number_of_inwards_related_cols = 0
    #average_inwards_column_content_precision = 0
    #average_inwards_column_content_recall = 0
    #average_inwards_column_content_fMeasure = 0
    #average_inwards_grouping_factor = 0
    #total_num_of_inwards_related_column = 0
    #total_num_of_inwards_extended_columns = 0
    total_num_of_inwards_nonKey_FDs = 0
    total_num_of_inwards_Key_FDs = 0
    total_num_of_inwards_No_FDs = 0
    total_num_of_inwards_rejected_No_FDs = 0
    #total_num_of_inwards_relCol_including_dup=0

    outwards_nonZero=0
    #inwards_nonZero=0

    random_query_eval_logger = open(eval_filePath, "w", encoding="utf-8")

    cntr=0
    for i in range(len(result_list)):
        cntr+=1
        
        outwards_eval = result_list[i][0]
        #inwards_eval = result_list[i][1]
        input_query = result_list[i][1]
        
        random_query_eval_logger.write("\n"+str(cntr)+"- Query:"+str(input_query))
        #random_query_eval_logger.write("\n"+str(outwards_eval)+" - "+str(inwards_eval))
        random_query_eval_logger.write("\n"+str(outwards_eval))
        random_query_eval_logger.write("\n--\n")
        
        total_num_of_outwards_related_column += outwards_eval[0]
        total_num_of_outwards_extended_columns += outwards_eval[5]
        total_num_of_outwards_relCol_including_dup += outwards_eval[7]
        
        if(outwards_eval[6]!=None):
            total_num_of_outwards_nonKey_FDs += outwards_eval[6]["outwards"]["num_of_nonKey_FD"]
            total_num_of_outwards_Key_FDs +=  outwards_eval[6]["outwards"]["num_of_Key_FD"]
            total_num_of_outwards_No_FDs +=  outwards_eval[6]["outwards"]["num_of_No_FD"]
            
            total_num_of_inwards_nonKey_FDs += outwards_eval[6]["inwards"]["num_of_nonKey_FD"]
            total_num_of_inwards_Key_FDs += outwards_eval[6]["inwards"]["num_of_Key_FD"]
            total_num_of_inwards_No_FDs += outwards_eval[6]["inwards"]["num_of_No_FD"]
            total_num_of_inwards_rejected_No_FDs += outwards_eval[6]["inwards"]["num_of_rejected_No_FD"]
            
        
        #total_num_of_inwards_related_column += inwards_eval[0]
        #total_num_of_inwards_extended_columns += inwards_eval[5]
        #total_num_of_inwards_relCol_including_dup += inwards_eval[7]
        
        #if(inwards_eval[6]!=None):
            #total_num_of_inwards_nonKey_FDs += inwards_eval[6]["num_of_nonKey_FD"]
            #total_num_of_inwards_Key_FDs += inwards_eval[6]["num_of_Key_FD"]
            #total_num_of_inwards_No_FDs += inwards_eval[6]["num_of_No_FD"]
            #total_num_of_inwards_rejected_No_FDs += inwards_eval[6]["num_of_rejected_No_FD"]

        
        if(outwards_eval[0]>0):
            outwards_nonZero+=1
            average_number_of_outwards_related_cols += outwards_eval[0]
            average_outwards_column_content_precision += outwards_eval[1]
            average_outwards_column_content_recall += outwards_eval[2]
            average_outwards_column_content_fMeasure += outwards_eval[3]
            average_outwards_grouping_factor += outwards_eval[4]
        
        #if(inwards_eval[0]>0):
            #inwards_nonZero+=1
            #average_number_of_inwards_related_cols += inwards_eval[0]
            #average_inwards_column_content_precision += inwards_eval[1]
            #average_inwards_column_content_recall += inwards_eval[2]
            #average_inwards_column_content_fMeasure += inwards_eval[3]
            #average_inwards_grouping_factor += inwards_eval[4]
            

    portion_of_outwards_tagged_cols = 0    
    portion_of_outwards_tagged_cols_including_dup = 0    
    if(total_num_of_outwards_extended_columns>0):
        portion_of_outwards_tagged_cols = total_num_of_outwards_related_column/total_num_of_outwards_extended_columns
        portion_of_outwards_tagged_cols_including_dup =   total_num_of_outwards_relCol_including_dup/total_num_of_outwards_extended_columns
        
    #portion_of_inwards_tagged_cols = 0
    #portion_of_inwards_tagged_cols_including_dup = 0 
    #if(total_num_of_inwards_extended_columns>0):
        #portion_of_inwards_tagged_cols = total_num_of_inwards_related_column/total_num_of_inwards_extended_columns
        #portion_of_inwards_tagged_cols_including_dup =   total_num_of_inwards_relCol_including_dup/total_num_of_inwards_extended_columns
    
    avg_outwards_table_size = 0
    #avg_inwards_table_size = 0
    avg_outwards_relCol_including_dup = 0
    #avg_inwards_relCol_including_dup = 0
    
    if(len(result_list)!=0):
        average_number_of_outwards_related_cols = average_number_of_outwards_related_cols/len(result_list)
        avg_outwards_table_size = total_num_of_outwards_extended_columns/len(result_list)
        #avg_inwards_table_size = total_num_of_inwards_extended_columns/len(result_list)
        avg_outwards_relCol_including_dup = total_num_of_outwards_relCol_including_dup/len(result_list)
        #avg_inwards_relCol_including_dup = total_num_of_inwards_relCol_including_dup/len(result_list)
    
    
    if(outwards_nonZero!=0):
        average_outwards_column_content_precision = average_outwards_column_content_precision /outwards_nonZero
        average_outwards_column_content_recall = average_outwards_column_content_recall /outwards_nonZero
        average_outwards_column_content_fMeasure = average_outwards_column_content_fMeasure /outwards_nonZero
        average_outwards_grouping_factor = average_outwards_grouping_factor /outwards_nonZero
        

    avg_num_of_outwards_nonKey_FDs=0
    avg_num_of_outwards_Key_FDs=0
    avg_num_of_outwards_No_FDs=0
    avg_num_of_inwards_nonKey_FDs=0
    avg_num_of_inwards_Key_FDs=0
    avg_num_of_inwards_No_FDs=0
    avg_num_of_inwards_rejected_No_FDs=0
    
    if(len(result_list)!=0):
        #average_number_of_inwards_related_cols = average_number_of_inwards_related_cols/len(result_list)
        avg_num_of_outwards_nonKey_FDs = total_num_of_outwards_nonKey_FDs/len(result_list)
        avg_num_of_outwards_Key_FDs = total_num_of_outwards_Key_FDs/len(result_list)
        avg_num_of_outwards_No_FDs = total_num_of_outwards_No_FDs/len(result_list)
        
        avg_num_of_inwards_nonKey_FDs = total_num_of_inwards_nonKey_FDs/len(result_list)
        avg_num_of_inwards_Key_FDs = total_num_of_inwards_Key_FDs/len(result_list)
        avg_num_of_inwards_No_FDs = total_num_of_inwards_No_FDs/len(result_list)
        avg_num_of_inwards_rejected_No_FDs = total_num_of_inwards_rejected_No_FDs/len(result_list)
        
    
    #if(inwards_nonZero!=0):
        #average_inwards_column_content_precision = average_inwards_column_content_precision /inwards_nonZero
        #average_inwards_column_content_recall = average_inwards_column_content_recall /inwards_nonZero
        #average_inwards_column_content_fMeasure = average_inwards_column_content_fMeasure /inwards_nonZero
        #average_inwards_grouping_factor = average_inwards_grouping_factor /inwards_nonZero
        
        
    outwards_avg_total_FDs = avg_num_of_outwards_nonKey_FDs+ avg_num_of_outwards_Key_FDs
    portion_of_avg_outwards_nonKey_FDs = 0
    if(outwards_avg_total_FDs>0):
        portion_of_avg_outwards_nonKey_FDs = avg_num_of_outwards_nonKey_FDs/outwards_avg_total_FDs
    
    inwards_avg_total_FDs = avg_num_of_inwards_nonKey_FDs+ avg_num_of_inwards_Key_FDs
    portion_of_avg_inwards_nonKey_FDs = 0
    if(inwards_avg_total_FDs>0):
        portion_of_avg_inwards_nonKey_FDs = avg_num_of_inwards_nonKey_FDs/inwards_avg_total_FDs                                                 
    
    random_query_eval_logger.write("------- Final Result --------")
    
    random_query_eval_logger.write("\n Outwards Result: \n")
    random_query_eval_logger.write("\n average_number_of_unique_outwards_related_cols:"+str(average_number_of_outwards_related_cols))
    random_query_eval_logger.write("\n avg_outwards_relCol_including_dup:"+str(avg_outwards_relCol_including_dup))
    random_query_eval_logger.write("\n average_outwards_column_content_precision:"+str(average_outwards_column_content_precision))
    random_query_eval_logger.write("\n average_outwards_column_content_recall:"+str(average_outwards_column_content_recall))
    random_query_eval_logger.write("\n average_outwards_column_content_fMeasure:"+str(average_outwards_column_content_fMeasure))
    fMeasure_of_average_outwards_column_contents=0
    if(average_outwards_column_content_precision+average_outwards_column_content_recall!=0):
        fMeasure_of_average_outwards_column_contents = (2*average_outwards_column_content_precision*average_outwards_column_content_recall)/(average_outwards_column_content_precision+average_outwards_column_content_recall)
    random_query_eval_logger.write("\n fMeasure_of_average_outwards_column_contents:"+str(fMeasure_of_average_outwards_column_contents))
    random_query_eval_logger.write("\n average_outwards_grouping_factor:"+str(average_outwards_grouping_factor))
    random_query_eval_logger.write("\n portion_of_outwards_tagged_cols:"+str(portion_of_outwards_tagged_cols))
    random_query_eval_logger.write("\n portion_of_outwards_tagged_cols_including_dup:"+str(portion_of_outwards_tagged_cols_including_dup))
    random_query_eval_logger.write("\n avg_outwards_table_size:"+str(avg_outwards_table_size))
    random_query_eval_logger.write("\n avg_num_of_outwards_nonKey_FDs:"+str(avg_num_of_outwards_nonKey_FDs))
    random_query_eval_logger.write("\n avg_num_of_outwards_Key_FDs:"+str(avg_num_of_outwards_Key_FDs))
    random_query_eval_logger.write("\n portion_of_avg_outwards_nonKey_FDs:"+str(portion_of_avg_outwards_nonKey_FDs))
    random_query_eval_logger.write("\n avg_num_of_outwards_No_FDs:"+str(avg_num_of_outwards_No_FDs))
    
    random_query_eval_logger.write("\n\n")
    
    random_query_eval_logger.write("\n Inwards Result: \n") 
    #random_query_eval_logger.write("\n average_number_of_unique_inwards_related_cols:"+str(average_number_of_inwards_related_cols))
    #random_query_eval_logger.write("\n avg_inwards_relCol_including_dup:"+str(avg_inwards_relCol_including_dup))
    #random_query_eval_logger.write("\n average_inwards_column_content_precision:"+str(average_inwards_column_content_precision))
    #random_query_eval_logger.write("\n average_inwards_column_content_recall:"+str(average_inwards_column_content_recall))
    #random_query_eval_logger.write("\n average_inwards_column_content_fMeasure:"+str(average_inwards_column_content_fMeasure))
    #fMeasure_of_average_inwards_column_contents=0
    #if(average_inwards_column_content_precision+average_inwards_column_content_recall!=0):
        #fMeasure_of_average_inwards_column_contents= (2*average_inwards_column_content_precision*average_inwards_column_content_recall)/(average_inwards_column_content_precision+average_inwards_column_content_recall)
    #random_query_eval_logger.write("\n fMeasure_of_average_inwards_column_contents:"+str(fMeasure_of_average_inwards_column_contents))
    #random_query_eval_logger.write("\n average_inwards_grouping_factor:"+str(average_inwards_grouping_factor))
    
    #random_query_eval_logger.write("\n portion_of_inwards_tagged_cols:"+str(portion_of_inwards_tagged_cols))
    #random_query_eval_logger.write("\n portion_of_inwards_tagged_cols_including_dup:"+str(portion_of_inwards_tagged_cols_including_dup))
    
    #random_query_eval_logger.write("\n avg_inwards_table_size:"+str(avg_inwards_table_size))
    random_query_eval_logger.write("\n avg_num_of_inwards_nonKey_FDs:"+str(avg_num_of_inwards_nonKey_FDs))
    random_query_eval_logger.write("\n avg_num_of_inwards_Key_FDs:"+str(avg_num_of_inwards_Key_FDs))
    random_query_eval_logger.write("\n portion_of_avg_inwards_nonKey_FDs:"+str(portion_of_avg_inwards_nonKey_FDs))
    random_query_eval_logger.write("\n avg_num_of_inwards_No_FDs:"+str(avg_num_of_inwards_No_FDs))
    random_query_eval_logger.write("\n avg_num_of_inwards_rejected_No_FDs:"+str(avg_num_of_inwards_rejected_No_FDs))
    
def corpus_dbPedia_tagging_prob():
    '''
    #In this method, we want to find the probability of a column being tagged by dbPedia in the coprus
    '''
    global dbPedia
    dbPedia = DBPedia()
    
    WT_Corpus={}
    
    with open(get_Constant("Deduplicated_Merged_Pickle_URL"), 'rb') as f3:
        WT_Corpus = pickle.load(f3)
        
    table_evals_list=[]
    num_of_total_attribs = 0
    num_of_total_taggings = 0
    total_prec=0
    total_recall=0
    total_fMeasure=0
    avg_portion_of_tagging = 0
    avg_grouping_factor = 0
    num_of_tables_with_tagging = 0

    for tableID in WT_Corpus:
        
        if("keyColumnIndex" not in WT_Corpus[tableID] ):
            continue
        
        key_idx = WT_Corpus[tableID]["keyColumnIndex"]
        corpus_table = WT_Corpus[tableID]["relation"]
        extended_style_table = [] #which is in this format that the key column is in the first column and the other columns after that.
        
        #add the key column
        extended_style_table.append(WT_Corpus[tableID]["relation"][key_idx])
        for idx in range(len(corpus_table)):
            if(idx != key_idx):
                extended_style_table.append(WT_Corpus[tableID]["relation"][idx])
                
        #evaluate over dbPedia
        eval_parent_path = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/corpus_"
        table_eval = Evaluate_Extended_Table(True, extended_style_table, eval_parent_path+str(tableID)+".txt")
        
        table_evals_list.append(table_eval)
        num_of_total_attribs+=table_eval[5]
        num_of_total_taggings+=table_eval[0]
        avg_portion_of_tagging+= table_eval[0]/table_eval[5]
        
        total_prec += table_eval[1]
        total_recall += table_eval[2]
        total_fMeasure += table_eval[3]
        
        if(table_eval[0]>0):
            avg_grouping_factor += table_eval[4]
            num_of_tables_with_tagging+=1
        #if(len(table_evals_list)>5):
            #break
    
    print(len(WT_Corpus))
    print(num_of_total_taggings)
    print(num_of_total_attribs)
    corpus_column_tagging_probability = num_of_total_taggings/num_of_total_attribs
    print("probability of column tagging in the corpus is :"+str(corpus_column_tagging_probability))
    print("avg_portion_of_tagging: "+str(avg_portion_of_tagging/len(WT_Corpus)))
    print("avg tagged col: "+str(num_of_total_taggings/len(WT_Corpus)))
    print("avg table size: "+str(num_of_total_attribs/len(WT_Corpus)))
    avg_precision = total_prec/len(WT_Corpus)
    print("average precision: "+str(avg_precision))
    avg_recall  = total_recall/len(WT_Corpus)
    print("average recall: "+str(avg_recall))
    print("average fMeasure: "+str(total_fMeasure/len(WT_Corpus)))
    
    print("avg_grouping_factor: "+str(avg_grouping_factor/num_of_tables_with_tagging))
    
def test_Column_Ranking():
    
    global dbPedia
    dbPedia = DBPedia()
    
    input_query_files=[]
    
    input_query_files.append("C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/companies-rand10.xlsx")
    input_query_files.append("C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/films-rand10.xlsx")
    input_query_files.append("C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/games-rand10.xlsx")
    input_query_files.append("C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/languages-rand10.xlsx")
    #input_query_files.append("C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/Languages.xlsx")
    input_query_files.append("C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/cities-rand10.xlsx")
    input_query_files.append("C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/currencies-rand10.xlsx")
    input_query_files.append("C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/countries-rand10.xlsx")
    
    for query in input_query_files:
        
        query_name = query.replace("C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/","").replace("-rand10.xlsx","")
        if(query_name[-3:]=="ies"):
            query_name = query_name[:-3]+"y"
        if(query_name[-1:]=="s"):
            query_name = query_name[:-1]
            
            
        queryKeys = Populate_QueryTable_From_ExcelFile(query)[0]
        proposed_extension_1_2, et_outwards_stat, et_inwards_stat = TableExtension([queryKeys],0,"Error_Threshold", "1+2")
        
        
        column_ranking_eval_path = "C:/Users/new/Google Drive/1-Uni/Thesis/Reports/Eval-Experiments/column_ranking/"
        

        #extendedTable_eval = New_Final_Table_Evaluation(proposed_extension_1_2, column_ranking_eval_path+query_name+"_eval.txt", {"outwards":et_outwards_stat,"inwards":et_inwards_stat})
        content_prec_threshold = 0.2
        evaluate_logfile = open(column_ranking_eval_path+query_name+"_eval.txt", "w", encoding="utf-8")
        
        extendedTable_eval = New_ExtendedTable_Evaluation(proposed_extension_1_2,evaluate_logfile , content_prec_threshold)
        final_eval = New_Final_Table_Evaluation(proposed_extension_1_2, column_ranking_eval_path+query_name+"_finalEval.txt", stat=None)
        
        for i in (0,1,2):
            New_Extended_Columns_Ranking(proposed_extension_1_2, query_name,i, column_ranking_eval_path+query_name+"_column_ranking_"+str(i)+".txt",extendedTable_eval, final_eval)
    
    
def Random_FD_Detection_Comparisons():
 #The Goal is to log the Statistics of Random 5% of Test Corpus columns as query column
    WT_Corpus={}
    with open(get_Constant("Deduplicated_Merged_Pickle_URL"), 'rb') as f3:
        WT_Corpus = pickle.load(f3)

    #Retrieve all columns
    corpus_col_list = []
    for tableID in  WT_Corpus:
        table = WT_Corpus[tableID]["relation"]
        units = WT_Corpus[tableID]["units"]
        header_index = -1
        if(WT_Corpus[tableID]["hasHeader"]):
            header_index = WT_Corpus[tableID]["headerRowIndex"]
            
        for idx in range(len(table)):
            corpus_col_list.append((table[idx],units[idx], header_index))
            
    evaluation.info("\n\n\n\n############### Results: ############################")

    global dbPedia
    dbPedia = DBPedia()
    
    et_proposed_result_list=[]
    cst_proposed_result_list=[]
    vm_proposed_result_list=[]
    vd_proposed_result_list=[]
    cg_proposed_result_list=[]
    coOccur_proposed_result_list=[]
    
    
    cntr = 0
    
    all_3_equal = 0
    
    eval_parent_folder = "C:/Users/new/Google Drive/1-Uni/Thesis/Reports/Eval-Experiments/FD_Detection/random_queries"
    exists = (os.path.isdir(eval_parent_folder ))
    if(not exists):
        os.makedirs(eval_parent_folder)
    

    proposed_inwards_outwards_source_file_path ="C:/Saeed_Local_Files/TestDataSets/T2D_233/output/integrated_inwards_outwards.xlsx"
    
    rejected_cols_file_path = "Column_Sel_Logs/rejected_cols.txt"
    inwards_key_cols_file_path = "Column_Sel_Logs/inwards_key_cols.txt"
    inwards_nonKey_cols_file_path = "Column_Sel_Logs/inwards_nonKey_cols.txt"
    outwards_nonKey_cols_file_path = "Column_Sel_Logs/outwards_nonKey_cols.txt"

    cumulative_query_keys = []
    
    #while cntr < int(0.05*len(corpus_col_list)):
    #while cntr < 20:
    while cntr < 57:
        #print(len(corpus_col_list))
        rand = random.randint(0,len(corpus_col_list)-1)
        col = corpus_col_list[rand][0]
        unit = corpus_col_list[rand][1][0]
        header_idx =  corpus_col_list[rand][2]

        if(unit != "text"):
            continue
        
        #Get rid of the header
        header=""
        if(header_idx!=-1):
            header = col[header_idx]
            del col[header_idx]
            
        #Deleting redundant values
        queryKeys = []
        for item in col:
            if (item not in queryKeys):
                queryKeys.append(item)
        
        if("1" in queryKeys and "2" in queryKeys):
            continue
            
        #mannheim_extension_list = []
        #proposed_extension_list = []
        
        if(len(queryKeys)>=5 and len(queryKeys)<=50 and '1' not in queryKeys):
            
            query_element_used = 0
            for elem in queryKeys:
                if elem in cumulative_query_keys:
                    query_element_used = 1
                    break
            
            if (query_element_used):
                continue
            
            cntr+=1
            queryTable = [queryKeys]
            cumulative_query_keys += [queryKeys]
            
            
            

            
            
            '''print("cst: round "+str(cntr)+" starts ...")
            cst_proposed_outwards_extension, cst_proposed_inwards_extension, cst_outwards_stat, cst_inwards_stat = TableExtension_ColumnSelectionTest(queryTable,0,"", False)            
            exists = os.path.isfile(proposed_inwards_outwards_source_file_path)
            if exists:
                copyfile(proposed_inwards_outwards_source_file_path, eval_parent_folder+"/cst_proposed_"+str(cntr)+"_output.xlsx")
            
            
            
            
            copyfile(rejected_cols_file_path, eval_parent_folder+"/cst_proposed_"+str(cntr)+"_rejected_cols_file.txt")
            copyfile(inwards_key_cols_file_path, eval_parent_folder+"/cst_proposed_"+str(cntr)+"_inwards_key_cols_file.txt")
            copyfile(inwards_nonKey_cols_file_path, eval_parent_folder+"/cst_proposed_"+str(cntr)+"_inwards_nonKey_cols_file.txt")
            copyfile(outwards_nonKey_cols_file_path, eval_parent_folder+"/cst_proposed_"+str(cntr)+"_outwards_nonKey_cols_file.txt")'''
            
            '''print("VM: round "+str(cntr)+" starts ...")
            vm_proposed_outwards_extension, vm_proposed_inwards_extension, vm_outwards_stat, vm_inwards_stat = TableExtension(queryTable,0,"Violation_Merging", False)            
            exists = os.path.isfile(proposed_inwards_outwards_source_file_path)
            if exists:
                copyfile(proposed_inwards_outwards_source_file_path, eval_parent_folder+"/vm_proposed_"+str(cntr)+"_output.xlsx")'''
                
            
            
            print("CG: round "+str(cntr)+" starts ...")
            cg_proposed_outwards_extension, cg_outwards_stat, cg_inwards_stat = TableExtension(queryTable,0,"Column_Grouping", False)            
            exists = os.path.isfile(proposed_inwards_outwards_source_file_path)
            if exists:
                copyfile(proposed_inwards_outwards_source_file_path, eval_parent_folder+"/cg_proposed_"+str(cntr)+"_output.xlsx")
            
            copyfile(inwards_key_cols_file_path, eval_parent_folder+"/cg_proposed_"+str(cntr)+"_inwards_key_cols_file.txt")
            copyfile(inwards_nonKey_cols_file_path, eval_parent_folder+"/cg_proposed_"+str(cntr)+"_inwards_nonKey_cols_file.txt")
            copyfile(outwards_nonKey_cols_file_path, eval_parent_folder+"/cg_proposed_"+str(cntr)+"_outwards_nonKey_cols_file.txt")
            
            
            eval_parent_path = eval_parent_folder+"/cg_proposed_"
            cg_proposed_outwards_eval = New_Final_Table_Evaluation(cg_proposed_outwards_extension, eval_parent_path+str(cntr)+"outwards.txt", {"outwards":cg_outwards_stat,"inwards":cg_inwards_stat})
            #cg_proposed_inwards_eval = Evaluate_Extended_Table(False, cg_proposed_inwards_extension, eval_parent_path+str(cntr)+"inwards.txt", cg_inwards_stat)
            
            if(cg_proposed_outwards_eval[0]==0):
                cntr-=1
                continue
            
            print("VD: round "+str(cntr)+" starts ...")
            vd_proposed_outwards_extension, vd_outwards_stat, vd_inwards_stat = TableExtension(queryTable,0,"Violation_Detectability", False)            
            exists = os.path.isfile(proposed_inwards_outwards_source_file_path)
            if exists:
                copyfile(proposed_inwards_outwards_source_file_path, eval_parent_folder+"/vd_proposed_"+str(cntr)+"_output.xlsx")
            #copyfile(rejected_cols_file_path, eval_parent_folder+"/vd_proposed_"+str(cntr)+"_rejected_cols_file.txt")
            copyfile(inwards_key_cols_file_path, eval_parent_folder+"/vd_proposed_"+str(cntr)+"_inwards_key_cols_file.txt")
            copyfile(inwards_nonKey_cols_file_path, eval_parent_folder+"/vd_proposed_"+str(cntr)+"_inwards_nonKey_cols_file.txt")
            copyfile(outwards_nonKey_cols_file_path, eval_parent_folder+"/vd_proposed_"+str(cntr)+"_outwards_nonKey_cols_file.txt")
            
            
            
            
                        
            eval_parent_path = eval_parent_folder+"/vd_proposed_"
            vd_proposed_outwards_eval = New_Final_Table_Evaluation(vd_proposed_outwards_extension, eval_parent_path+str(cntr)+"outwards.txt",{"outwards":vd_outwards_stat,"inwards":vd_inwards_stat})
            #vd_proposed_inwards_eval = Evaluate_Extended_Table(False, vd_proposed_inwards_extension, eval_parent_path+str(cntr)+"inwards.txt", vd_inwards_stat)
            
            
                
          
            
            #if(et_proposed_outwards_eval[0]>0 or et_proposed_inwards_eval[0]>0 or vd_proposed_outwards_eval[0]>0 or vd_proposed_inwards_eval[0]>0):
            #if((et_proposed_outwards_eval[0])==0 or et_proposed_outwards_eval[6]["outwards"]["num_of_nonKey_FD"]==0):
            
            #TEST ONLY: Calculate the percentage of the cases where the 3 methods yield the same result
            if(cg_proposed_outwards_eval[6]["outwards"]["num_of_nonKey_FD"] == vd_proposed_outwards_eval[6]["outwards"]["num_of_nonKey_FD"]):
                #cntr-=1
                #continue
                a=1

                    
            
            print("ET: round "+str(cntr)+" starts ...")
            et_proposed_outwards_extension, et_outwards_stat, et_inwards_stat = TableExtension(queryTable,0,"Error_Threshold", False)
            
            
            
            
            if(et_proposed_outwards_extension==[]):
                cntr-=1
                continue
            
            
            
            exists = os.path.isfile(proposed_inwards_outwards_source_file_path)
            if exists:
                copyfile(proposed_inwards_outwards_source_file_path, eval_parent_folder+"/et_proposed_"+str(cntr)+"_output.xlsx")
                
            #copyfile(rejected_cols_file_path, eval_parent_folder+"/vd_proposed_"+str(cntr)+"_rejected_cols_file.txt")
            copyfile(inwards_key_cols_file_path, eval_parent_folder+"/et_proposed_"+str(cntr)+"_inwards_key_cols_file.txt")
            copyfile(inwards_nonKey_cols_file_path, eval_parent_folder+"/et_proposed_"+str(cntr)+"_inwards_nonKey_cols_file.txt")
            copyfile(outwards_nonKey_cols_file_path, eval_parent_folder+"/et_proposed_"+str(cntr)+"_outwards_nonKey_cols_file.txt")
            
            eval_parent_path = eval_parent_folder+"/et_proposed_"
            et_proposed_outwards_eval = New_Final_Table_Evaluation(et_proposed_outwards_extension, eval_parent_path+str(cntr)+"outwards.txt", {"outwards":et_outwards_stat,"inwards":et_inwards_stat})
            #et_proposed_inwards_eval = New_Final_Table_Evaluation( et_proposed_inwards_extension, eval_parent_path+str(cntr)+"inwards.txt", et_inwards_stat)
            
                
            if(cg_proposed_outwards_eval[6]["outwards"]["num_of_nonKey_FD"] == vd_proposed_outwards_eval[6]["outwards"]["num_of_nonKey_FD"] and cg_proposed_outwards_eval[6]["outwards"]["num_of_nonKey_FD"] == et_proposed_outwards_eval[6]["outwards"]["num_of_nonKey_FD"]):
                all_3_equal+=1
                cntr-=1
                continue
            
            #print("Co-Occur: round "+str(cntr)+" starts ...")
            #coOccur_proposed_outwards_extension, coOccur_outwards_stat, coOccur_inwards_stat = TableExtension(queryTable,0,"Co-occurence_pruning", False)            
            #exists = os.path.isfile(proposed_inwards_outwards_source_file_path)
            #if exists:
            #    copyfile(proposed_inwards_outwards_source_file_path, eval_parent_folder+"/coOccur_proposed_"+str(cntr)+"_output.xlsx")
            
            #copyfile(inwards_key_cols_file_path, eval_parent_folder+"/coOccur_proposed_"+str(cntr)+"_inwards_key_cols_file.txt")
            #copyfile(inwards_nonKey_cols_file_path, eval_parent_folder+"/coOccur_proposed_"+str(cntr)+"_inwards_nonKey_cols_file.txt")
            #copyfile(outwards_nonKey_cols_file_path, eval_parent_folder+"/coOccur_proposed_"+str(cntr)+"_outwards_nonKey_cols_file.txt")



            

            '''eval_parent_path = eval_parent_folder+"/cst_proposed_"
            cst_proposed_outwards_eval = Evaluate_Extended_Table(True, cst_proposed_outwards_extension, eval_parent_path+str(cntr)+"outwards.txt",cst_outwards_stat)
            cst_proposed_inwards_eval = Evaluate_Extended_Table(False, cst_proposed_inwards_extension, eval_parent_path+str(cntr)+"inwards.txt", cst_inwards_stat)
            
            eval_parent_path = eval_parent_folder+"/vm_proposed_"
            vm_proposed_outwards_eval = Evaluate_Extended_Table(True, vm_proposed_outwards_extension, eval_parent_path+str(cntr)+"outwards.txt",vm_outwards_stat)
            vm_proposed_inwards_eval = Evaluate_Extended_Table(False, vm_proposed_inwards_extension, eval_parent_path+str(cntr)+"inwards.txt", vm_inwards_stat)'''
            

            #eval_parent_path = eval_parent_folder+"/coOccur_proposed_"
            #coOccur_proposed_outwards_eval = New_Final_Table_Evaluation(coOccur_proposed_outwards_extension, eval_parent_path+str(cntr)+"outwards.txt", {"outwards":coOccur_outwards_stat,"inwards":coOccur_inwards_stat})
            #coOccur_proposed_inwards_eval = Evaluate_Extended_Table(False, coOccur_proposed_inwards_extension, eval_parent_path+str(cntr)+"inwards.txt", coOccur_inwards_stat)
            
            et_proposed_result_list.append((et_proposed_outwards_eval, queryTable))
            #et_proposed_stat_list.append((et_outwards_stat,et_inwards_stat))
            #cst_proposed_result_list.append((cst_proposed_outwards_eval, cst_proposed_inwards_eval, queryTable))
            #vm_proposed_result_list.append((vm_proposed_outwards_eval, vm_proposed_inwards_eval, queryTable))
            vd_proposed_result_list.append((vd_proposed_outwards_eval, queryTable))
            #vd_proposed_stat_list.append((vd_outwards_stat,vd_inwards_stat))
            cg_proposed_result_list.append((cg_proposed_outwards_eval, queryTable))
            #cg_proposed_stat_list.append((cg_outwards_stat,cg_inwards_stat))
            #coOccur_proposed_result_list.append((coOccur_proposed_outwards_eval, queryTable))
            

    
    print("\n\n---------\n")
    print("cnt: "+str(cntr))
    print("all_3_equal: "+str(all_3_equal))
    #sys.exit()
    
    total_extension_eval_filePath = eval_parent_folder+"/et_proposed_table-eval.txt"
    total_extension_eval(et_proposed_result_list, total_extension_eval_filePath )
    
    #eval_filePath = eval_parent_folder+"/cst_proposed_table-eval.txt"
    #total_extension_eval(cst_proposed_result_list, eval_filePath)
    
    #eval_filePath = eval_parent_folder+"/vm_proposed_table-eval.txt"
    #total_extension_eval(vm_proposed_result_list, eval_filePath)
    
    eval_filePath = eval_parent_folder+"/vd_proposed_table-eval.txt"
    total_extension_eval(vd_proposed_result_list, eval_filePath)
    
    eval_filePath = eval_parent_folder+"/cg_proposed_table-eval.txt"
    total_extension_eval(cg_proposed_result_list, eval_filePath)
    
    #eval_filePath = eval_parent_folder+"/coOccur_proposed_table-eval.txt"
    #total_extension_eval(coOccur_proposed_result_list, eval_filePath)
    

#http://dbpedia.org/sparql?default-graph-uri=http%3A%2F%2Fdbpedia.org&query=select+distinct+%3FRel+where+%7B%3Chttp%3A%2F%2Fdbpedia.org%2Fresource%2FGermany%3E+%3FRel+%3Chttp%3A%2F%2Fdbpedia.org%2Fresource%2FBerlin%3E%7D+&format=text%2Fhtml&CXML_redir_for_subjs=121&CXML_redir_for_hrefs=&timeout=30000&debug=on&run=+Run+Query+
if  (__name__ == "__main__"):
    sys.stdout = open("C:/Users/new/Desktop/python_print_logs.txt", "w")
    #Create_Manual_GroundTruth("C:/Saeed_Local_Files/Logs/Mapped_to_DBPedia/Queries/Film_Queries.csv")
    
    
    queryTableFolder = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries"
    #csvFileName="Country_From_Table.csv_result.csv"
    #csvFileName="T2D_233_Octopus_Extend.csv"

    start = time.time()
    
    setup_logger('lookup', queryTableFolder+"/lookup.log")
    setup_logger('predicate',queryTableFolder+"/predicate.log")
    setup_logger('evaluation', queryTableFolder+"/evaluation.log")
    
    lookup = logging.getLogger('lookup')
    predicate = logging.getLogger('predicate')
    evaluation = logging.getLogger('evaluation')
    
    
    #Table_Extension_Quality_Comparison_with_DBPedia()
    #Mannheim_Extension_Quality_Comparison_with_DBPedia()
    #Test_Random_Queries()
    #corpus_dbPedia_tagging_prob()
    #Table_Extension_Quality_Comparison_with_DBPedia()
    Evaluate_Comparisons()
    #Random_Evaluate_Comparisons()
    #test_Column_Ranking()
    #Evaluate_FD_Detection_Methods_Separate_Queries()
    #Random_FD_Detection_Comparisons()

    '''Object = DBPedia_Lookup("18")
    Object="18"
    Subject = DBPedia_Lookup("Iran")
    DBPedia_GetPredicate(Subject, Object)
    #DBPedia_SPARQL(i)'''
    

    
    #predicate = logging.getLogger('predicate')
    
    
    '''Extended_Table = Aug_Table(queryTableFolder+"/"+csvFileName)
    #C:/Saeed_Local_Files/Logs/Game_Mannheim_Orig/AugmentedTable.csv
    #C:/Saeed_Local_Files/Logs/Film_Mannheim_Simplified/Film_Director.csv_result.csv
    Extended_Table.eval_Table()
    #Extended_Table.print_Table()'''
    
    
    #source_folder = "C:/Saeed_Local_Files/Proposed_Sol/eval"
    #setup_logger('dbPediaEval', source_folder +"/dbPediaEval.log")
    #dbPediaEval = logging.getLogger('dbPediaEval')
    
    #ExtendedTable_Path =source_folder + "/Company_Queries_TableExtension_Result.csv" 
    #ET_DBPedia_Mapping_path = source_folder + "/Company_Queries_TableExtension_Result_Mapping.csv"
    
    #GT_Path = "C:/Saeed_Local_Files/GroundTruth/DBPedia/Company_GT_DBPedia.csv"
    #Equi_Props_Path = "C:/Saeed_Local_Files/matcher/equivalentProperties.tsv"
    
    #Eval_DBPedia_As_GT(ExtendedTable_Path, ET_DBPedia_Mapping_path, GT_Path, Equi_Props_Path)
    
    
    #cleaned_groundTruth_csv = "C:/Users/new/Dropbox/1-Uni/Thesis/Meetings/4-Spring-Summer-2018/2018-05-28/2-GroundTruth-CleanedCompanyAsTable.csv" 
    #cleaned_groundTruth_csv = "C:/Saeed_Local_Files/Evaluations/DBPediaAsTables/Country/2-Automatic-Cleaned-GroundTruth-Country.csv"
    #extendedTable_csv = "C:/Saeed_Local_Files/Evaluations/DBPediaAsTables/Country/integrated_inwards_outwards.csv" 
    
    #Evaluate_By_PreDefined_GroundTruth(extendedTable_csv, cleaned_groundTruth_csv)
    
    #groundTruth_csv = "C:/Saeed_Local_Files/Evaluations/DBPediaAsTables/Country/1-GroundTruth-Country.csv" 
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/Country2.xlsx"
    #Cleaning_DBPediaAsTable_GT_By_Query(Populate_QueryTable_From_ExcelFile(input_query_file), groundTruth_csv, 0.8, "C:/Saeed_Local_Files/Evaluations/DBPediaAsTables/Country/2-Automatic-Cleaned-GroundTruth-Country.csv" )
    
    end = time.time()
    elapsed = end - start
    print("elapsed time: "+str(elapsed))
    #evaluation.info("elapsed time: "+str(elapsed)+" Seconds, in other words, "+str(elapsed/60)+" Minutes, or "+str(elapsed/(60*60))+" Hours")


    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/countries-rand10.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/countries-rand10.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/companies-rand10.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/films-rand10.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/games-rand10.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/languages-rand10.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/Languages.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/cities-rand10.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/currencies-rand10.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/test1.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/All-Countries.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/Currency-All.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/Languages.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/Cities.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/city-test.xlsx"

    
    #et_proposed_extension, et_outwards_stat, et_inwards_stat = TableExtension(Populate_QueryTable_From_ExcelFile(input_query_file),0,"Error_Threshold", False)
    #infoGather_extension = Extend_Table(Populate_QueryTable_From_ExcelFile(input_query_file)[0])
    #global dbPedia
    #dbPedia = DBPedia()
    #eval_proposed_folder_path = "C:/Users/new/Desktop/"
    #et_proposed_outwards_eval = New_Final_Table_Evaluation(et_proposed_extension, eval_proposed_folder_path+"proposed.txt", {"outwards":et_outwards_stat,"inwards":et_inwards_stat})
    #et_proposed_outwards_eval = New_Final_Table_Evaluation(infoGather_extension, eval_proposed_folder_path+"proposed.txt")
    
    '''lookup.handlers = []
    predicate.handlers = []
    evaluation.handlers = []'''

    
    