'''
This file contains the functionalities used to work with DBpedia.
DBpedia is used as a ground truth in our evaluations.
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
from rdflib import Graph


DBPedia_Ont_Loc = "C:\Saeed_Local_Files\DBPedia\dbpedia_2014.owl"
DBPedia_Mappings_Loc = "C:\Saeed_Local_Files\DBPedia\mappingbased_properties_cleaned_en_2014.nt" 
DBPedia_Mappings_Pickle_Loc = "C:\Saeed_Local_Files\DBPedia\mappingbased_properties_cleaned_en_2014.picke" 
DBPedia_Redirects_Loc = r"C:\Saeed_Local_Files\DBPedia\redirects_transitive_en_2014.nt" 
DBPedia_TotalRedirects_Pickle_Loc = r"C:\Saeed_Local_Files\DBPedia\total_redirects_en_2014.pickle" 
DBPedia_SameAS_Loc = r"C:\Saeed_Local_Files\DBPedia\disambiguations_en.nt" 
DBPedia_Disambig_Loc = r"C:\Saeed_Local_Files\DBPedia\iri_same_as_uri_en.nt" 

def Extract_Resource_From_URI(URI, mode="NotCleansed"):
    if(URI.startswith("<")):
        elems = URI.split(" ")
        chopped = elems[0].split("/")
        resource = chopped[len(chopped)-1][:-1]
        if(mode=="Cleansed"):
            return string_Cleanse(resource)
        else:
            return (resource)
    elif ('"' in URI):
        elems = URI.split('"')
        if(mode=="Cleansed"):
            return string_Cleanse(elems[1])
        else:
            return (elems[1])
        
        
        

def Create_DBPedia_Mappings():
    Mappings_dict={}
    with open(DBPedia_Mappings_Loc, 'r') as file:
            for line in file:
                if(line.startswith("#")==False):
                    elems = line.split(" ")
                    
                    Cleansed_resource = (Extract_Resource_From_URI(elems[0], "Cleansed"))
                    if(Cleansed_resource in Mappings_dict):
                        Mappings_dict[Cleansed_resource].append((Extract_Resource_From_URI(elems[1]), Extract_Resource_From_URI(elems[2], "Cleansed")))
                    
                    else:
                         Mappings_dict[Cleansed_resource] = [(Extract_Resource_From_URI(elems[1]), Extract_Resource_From_URI(elems[2], "Cleansed"))]
                #cntr+=1
                #if(cntr>10):
                    #break
                
    print(len(Mappings_dict))
    with open(DBPedia_Mappings_Pickle_Loc, 'wb') as f1:
        pickle.dump(Mappings_dict ,f1)
        
        
        
def Create_DBPedia_TotalRedirects():
    TotalRedirects_dict={}
    with open(DBPedia_Redirects_Loc, 'r') as file:
            for line in file:
                if(line.startswith("#")==False):
                    elems = line.split(" ")

                    First_resource = Extract_Resource_From_URI(elems[0], "Cleansed")
                    
                    Second_resource = Extract_Resource_From_URI(elems[2], "Cleansed")
                    
                    
                    
                    if(First_resource in TotalRedirects_dict):
                        if(Second_resource not in TotalRedirects_dict[First_resource]):
                            TotalRedirects_dict[First_resource].append(Second_resource)
                    
                    else:
                         TotalRedirects_dict[First_resource] = [Second_resource]
                         
                    if(Second_resource in TotalRedirects_dict):
                        if(First_resource not in TotalRedirects_dict[Second_resource]):
                            TotalRedirects_dict[Second_resource].append(First_resource)
                    
                    else:
                         TotalRedirects_dict[Second_resource] = [First_resource]
                         
                         
                         
                         
    with open(DBPedia_Disambig_Loc, 'r') as file:
        for line in file:
            if(line.startswith("#")==False):
                elems = line.split(" ")

                First_resource = Extract_Resource_From_URI(elems[0], "Cleansed")
                
                Second_resource = Extract_Resource_From_URI(elems[2], "Cleansed")
                
                
                
                if(First_resource in TotalRedirects_dict):
                    if(Second_resource not in TotalRedirects_dict[First_resource]):
                        TotalRedirects_dict[First_resource].append(Second_resource)
                
                else:
                     TotalRedirects_dict[First_resource] = [Second_resource]
                     
                if(Second_resource in TotalRedirects_dict):
                    if(First_resource not in TotalRedirects_dict[Second_resource]):
                        TotalRedirects_dict[Second_resource].append(First_resource)
                
                else:
                     TotalRedirects_dict[Second_resource] = [First_resource]
                     
                     
    with open(DBPedia_SameAS_Loc, 'r') as file:
        for line in file:
            if(line.startswith("#")==False):
                elems = line.split(" ")

                First_resource = Extract_Resource_From_URI(elems[0], "Cleansed")
                
                Second_resource = Extract_Resource_From_URI(elems[2], "Cleansed")
                
                
                
                if(First_resource in TotalRedirects_dict):
                    if(Second_resource not in TotalRedirects_dict[First_resource]):
                        TotalRedirects_dict[First_resource].append(Second_resource)
                
                else:
                     TotalRedirects_dict[First_resource] = [Second_resource]
                     
                if(Second_resource in TotalRedirects_dict):
                    if(First_resource not in TotalRedirects_dict[Second_resource]):
                        TotalRedirects_dict[Second_resource].append(First_resource)
                
                else:
                     TotalRedirects_dict[Second_resource] = [First_resource]
                #cntr+=1
                #if(cntr>10):
                    #break
                
    print(len(TotalRedirects_dict))
    with open(DBPedia_TotalRedirects_Pickle_Loc, 'wb') as f1:
        pickle.dump(TotalRedirects_dict ,f1)
        
        

class DBPedia:
    

    
    def __init__(self):
        #self.Init_Ontology()
        self.Init_Mappings()
        self.Init_TotalRedirects()
        
    def __del__(self):
        del self.Mappings
        
        
    def Init_Mappings(self):

        with open(DBPedia_Mappings_Pickle_Loc, 'rb') as f1:
            self.Mappings = pickle.load(f1)
            
        print(len(self.Mappings))
        
                
    def Init_TotalRedirects(self):
        with open(DBPedia_TotalRedirects_Pickle_Loc, 'rb') as f1:
            self.TotalRedirects = pickle.load(f1)
            
        
        
    def Search_in_Mappings(self, word):
        
         #####
            #This Function Returns a List of  matches of the input words in the Mappings.
        ####
        
        
        Cleansed_word =  string_Cleanse(word)
        Results = [Cleansed_word]
        
        #Approximate_Mathces
        '''for elem in self.Mappings:
            
            #print(elem)
            if(Edit_Distance_Ratio(elem, Cleansed_word)<0.15):
                if(elem not in Results):
                    Results.append(elem)'''
                    
                    
        Redirects=[]
        for elem in Results:
            if(elem in self.TotalRedirects):
                Redirects += self.TotalRedirects[elem]
                
        return set(Results+Redirects)
                
            
    def Extract_Predicates(self, subj_set, obj_set):
        
         #####
            #This Function Returns All Predicates that are used foe triple in the format of (subj_elem, Predicate, obj_elem) 
            #or (obj_elem, Predicate, subj_elem)
        ####
        
        #subj_set = self.Search_in_Mappings(subj)
        #obj_set = self.Search_in_Mappings(obj)
        
        Predicates=set()
        
        for s in subj_set:
            for o in obj_set:
                if (s in self.Mappings):
                    for t in self.Mappings[s]:
                        #if(Char_Level_Edit_Distance(t[1],o)<0.15):
                        if(t[1]==o):
                            #print((s,t,o))
                            Predicates.add(t[0])
                            
        for s in obj_set:
            for o in subj_set:
                if (s in self.Mappings):
                    for t in self.Mappings[s]:
                        #if(Char_Level_Edit_Distance(t[1],o)<0.15):
                        if(t[1]==o):
                            #print((s,t,o))
                            Predicates.add(t[0])
                        
        return Predicates
    
    

    def Extract_Subj_Obj_for_Predicates(self, pred):
        
         #####
            #This Function Returns All Subject and objects for a given predicate 
        ####
        
        results={}
        
    
        #cleansed_pred = string_Cleanse(pred)
        for subj in self.Mappings:
            for p in self.Mappings[subj]:
                if p[0] == pred :
                    print(subj)
                    print(p[1])
                    if(subj not in results):
                        results[subj]= [p[1]]
                    else:
                        results[subj].append(p[1])


        return results
    
    
    
    def  Extract_All_Triples(self,querykey):
        
         #####
            #This Function Returns All triples that are used for triple in the format of (querykey, X, Y)
        ####
        
        cleansed_queryKey = string_Cleanse(querykey)
        QueryKeys=[cleansed_queryKey]
        '''if(cleansed_queryKey in self.TotalRedirects):
            QueryKeys += self.TotalRedirects[cleansed_queryKey]'''
        
        #print(QueryKeys)
        triples=set()
        
        for key in QueryKeys:
            if (key in self.Mappings):
                for t in self.Mappings[key]:
                    triples.add((key,t[0],t[1]))
                            
        '''for key in self.Mappings:
            for t in self.Mappings[key]:
                if(t[1] in QueryKeys):
                    triples.add((key,t[0],t[1]))'''
                    
        return triples




def Create_Ground_Truth(QueryKeysMapping_Path, dbPedia):
    
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
        QueryKeys.append(string_Cleanse(q[1].split("/")[-1]))
        
    print(QueryKeys)
    
    Extended_Table=[]  #Final Table which will be returned!
    FirstRow=["Keys"]
    for q in URIQueryKeys_Mapping:
        FirstRow.append(q[0])
        
    Extended_Table.append(FirstRow) #Adding a Column in a Table Style to the Final Table.
    
    Extended_Dict={}  #Dict Equivalent of the Extended Table, with "Property Headers" as keys as  and (QueryKey, Value) as values
    
    for querykey in QueryKeys:
        tripleList = dbPedia.Extract_All_Triples(querykey)
        for triple in tripleList:
            Key = triple[0]
            Property =  triple[1]
            Value = triple[2]
            
            if(Property not in Extended_Dict):
                Extended_Dict[Property]=[(Key, Value)]
            else:
                Extended_Dict[Property].append((Key, Value))
                
    #Create_Columns
    Extensions_Cols=[]
    for Property in Extended_Dict:
        col=[]
        col.append(Property)
        col.append(Extended_Dict[Property])
        Extensions_Cols.append(col)
            
            
    #Sort Based on the coverage of the column
    sortedCols = (sorted(Extensions_Cols,key=lambda col: len(col[1]), reverse=True))
    
    for col in sortedCols:
        #Add_Columns: 
        col_Header= col[0]
        New_Col=[col_Header]
        for key in QueryKeys:
            Extended_Values=[]
            
            #Add All Values for the Given Key to Extended_Values:
            #Which in this case should be only one!
            for val in col[1]:
                if(val[0]== string_Cleanse(key)):
                    
                    Extended_Values.append(val[1])
            
            #Concatenate All Values for each key for each property
            Predicted_Value = ""
            for elem in Extended_Values:
                Predicted_Value+=elem+"|"
                
            if (Predicted_Value.endswith("|")):
                Predicted_Value = Predicted_Value[:-1]
                
            
            New_Col.append(Predicted_Value)
                
        Extended_Table.append(New_Col)
        
    Write_Table_To_CSV(Extended_Table, "C:/Saeed_Local_Files/GroundTruth/DBPedia/" + QueryKeysMapping_Path.split("/")[-1].split(".")[0] + ".csv")   
        
    
    
    
    
    
    


if  (__name__ == "__main__"):
    start = time.time()
    #Create_DBPedia_Mappings()
    #Create_DBPedia_TotalRedirects()
    dbPedia = DBPedia()
    
    #print(dbPedia.Extract_Predicates(dbPedia.Search_in_Mappings("Armenia"),dbPedia.Search_in_Mappings("2010")))
    #print(dbPedia.Search_in_Mappings("Armenia"))
    '''res = (dbPedia.Extract_Subj_Obj_for_Predicates("highestRank"))
    for key in res:
        print(key+":")
        print(res[key])
        print("--")'''
    #Create_Ground_Truth("C:/Saeed_Local_Files/Logs/Mapped_to_DBPedia/Queries/Games_Queries.csv", dbPedia)
    end = time.time()
    elapsed = end - start
    print("elapsed time: "+str(elapsed))
    
    #Create_DBPedia_TotalRedirects()
    
    #print(Extract_Word_From_URI('''<fskjdhf/skjhdfj/sdkjfhdshkjhf/word>'''))
    
    
    #print(dbPedia.Extract_All_Triples("Denmark"))
    #Create_DBPedia_Mappings()
    #s=1
    #Create_DBPedia_Redirects()
    #print(dbPedia.Redirects[string_Cleanse("irn")])
    #4172474
    #4133947