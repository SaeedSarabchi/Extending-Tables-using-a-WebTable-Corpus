#### Codes Relating to Column Annotation Paper ####
#from DBPedia import *
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
DBPedia_Orig_Resource_Mappings_Pickle_Loc = "C:\Saeed_Local_Files\DBPedia\Orig_Resource_mappingbased_properties_cleaned_en_2014.picke" 
DBPedia_Redirects_Loc = r"C:\Saeed_Local_Files\DBPedia\redirects_transitive_en_2014.nt" 
DBPedia_TotalRedirects_Orig_Resource_Pickle_Loc = r"C:\Saeed_Local_Files\DBPedia\total_Orig_Resource_redirects_en_2014.pickle" 
DBPedia_SameAS_Loc = r"C:\Saeed_Local_Files\DBPedia\disambiguations_en.nt" 
DBPedia_Disambig_Loc = r"C:\Saeed_Local_Files\DBPedia\iri_same_as_uri_en.nt" 
DBPedia_Type_Mapping_Loc = r"C:\Saeed_Local_Files\DBPedia\instance_types_sdtyped_dbo_en.nt" 
DBPedia_Type_Mapping_Pickle_Loc = r"C:\Saeed_Local_Files\DBPedia\instance_types_sdtyped_dbo_en.pickle" 
Column_Context_Pickle_Loc = r"C:\Saeed_Local_Files\DBPedia\Column_Context.pickle" 
Save_Parent_Folder = r"C:\Saeed_Local_Files\Column_Annotation"


        
def Create_DBPedia_Mappings():
    '''
        #This Function transforms DBPedia rdfs into a pickle map file, where the key is the subject,
        #and for each subject, the related list of (Predicate,Object) is attached.
    '''
    
    Mappings_dict={}
    with open(DBPedia_Mappings_Loc, 'r') as file:
            for line in file:
                if(line.startswith("#")==False):
                    elems = line.split(" ")
                    
                    resource = elems[0]
                    if(resource in Mappings_dict):
                        Mappings_dict[resource].append((elems[1], elems[2]))
                    
                    else:
                         Mappings_dict[resource] = [(elems[1], elems[2])]
                #cntr+=1
                #if(cntr>10):
                    #break
                
    print(len(Mappings_dict))
    with open(DBPedia_Orig_Resource_Mappings_Pickle_Loc, 'wb') as f1:
        pickle.dump(Mappings_dict ,f1)

     
   

    
def Create_DBPedia_TotalRedirects():
    TotalRedirects_dict={}
    with open(DBPedia_Redirects_Loc, 'r') as file:
            for line in file:
                if(line.startswith("#")==False):
                    elems = line.split(" ")

                    First_resource = elems[0]
                    
                    Second_resource = elems[2]
                    
                    
                    
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

                First_resource = (elems[0])
                
                Second_resource = (elems[2])
                
                
                
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

                First_resource = (elems[0])
                
                Second_resource = (elems[2])
                
                
                
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
    with open(DBPedia_TotalRedirects_Orig_Resource_Pickle_Loc, 'wb') as f1:
        pickle.dump(TotalRedirects_dict ,f1)
        

class DBPedia:
    

    
    def __init__(self):
        
        self.Init_Mappings()
        #self.Init_TotalRedirects()
        #self.Init_Types()
        
        
    def Init_Mappings(self):

        with open(DBPedia_Orig_Resource_Mappings_Pickle_Loc, 'rb') as f1:
            self.Mappings = pickle.load(f1)
            
        print(len(self.Mappings))
        
                
    def Init_TotalRedirects(self):
        with open(DBPedia_TotalRedirects_Orig_Resource_Pickle_Loc, 'rb') as f1:
            self.TotalRedirects = pickle.load(f1)
            
    def Init_Types(self):
        with open(DBPedia_Type_Mapping_Pickle_Loc, 'rb') as f1:
            self.Types = pickle.load(f1)
            
        
        
    def Search_in_Mappings(self, URI):
        
         #####
            #This Function Returns a List of  matches of the input words in the Mappings.
        ####
        
        Results = [URI]
        
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
            #This Function Returns All Predicates that are used for triple in the format of (subj_elem, Predicate, obj_elem) 
            #or (obj_elem, Predicate, subj_elem)
        ####
        
        #subj_set = self.Search_in_Mappings(subj)
        #obj_set = self.Search_in_Mappings(obj)
        
        Predicates=set()
        
        for s in subj_set:
            for o in obj_set:
                if (s in self.Mappings):
                    for t in self.Mappings[s]:
                        if(Edit_Distance_Ratio(t[1],o)<0.15):
                            Predicates.add(t[0])
                            
        for s in obj_set:
            for o in subj_set:
                if (s in self.Mappings):
                    for t in self.Mappings[s]:
                        if(Edit_Distance_Ratio(t[1],o)<0.15):
                            Predicates.add(t[0])
                        
        return Predicates
    
    
    def Search_in_Type_Mappings(self, input_subjects):
         #####
            #This Function Returns all values X in the form of (input_subject, type, X) 
        ####
        

        
        Results=set()
        for res in input_subjects:
            if(res in self.Types):
                for obj in self.Types[res]:
                    Results.add(obj)

        return set(Results)
                
        
    
    def  Extract_All_Triples(self,subject):
        
         #####
            #This Function Returns All triples that are used for triple in the format of (subject, X, Y)
        ####
        
        QueryKeys=[subject]
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
    

def Search_in_Type_Mappings(Types, input_subjects):
     #####
        #This Function Returns all values X in the form of (input_subject, type, X) 
    ####
    
    
            
    Results=[]
    for res in input_subjects:
        if(res.lower() in Types):
            for obj in Types[res.lower()]:
                Results.append(obj.lower())

    return (Results)



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
        
        
def Create_DBPedia_Type_Mappings():
    '''
        #This Function transforms DBPedia rdfs into a pickle map file, where the key is the subject,
        #and for each subject, the related list of (Predicate,Object) is attached.
    '''
    
    Mappings_dict={}
    with open(DBPedia_Type_Mapping_Loc,encoding="utf8") as file:
            for line in file:
                if(line.startswith("#")==False):
                    elems = line.split(" ")
                    
                    resource = str(elems[0]).lower()
                    if(resource in Mappings_dict):
                        Mappings_dict[resource].append(str(elems[2]).lower())
                    
                    else:
                         Mappings_dict[resource] = [str(elems[2]).lower()]
                #cntr+=1
                #if(cntr>10):
                    #break
                
    print(len(Mappings_dict))
    with open(DBPedia_Type_Mapping_Pickle_Loc, 'wb') as f1:
        pickle.dump(Mappings_dict ,f1) 

def Create_Column_Context():
    '''
        #This Function Creates the mapping between the objects of each predicate and the type of their related subject 
        # Type ->  {predicate -> [Predicate_Content]}
    '''
    
    
    Type_Mapping_dict={}
    with open(DBPedia_Type_Mapping_Pickle_Loc, 'rb') as f1:
            Type_Mapping_dict = pickle.load(f1)
            
    Types = {}
    with open(DBPedia_Type_Mapping_Pickle_Loc, 'rb') as f1:
            Types = pickle.load(f1)
            
    
    Column_Context = {}
    cnt_Exists=0
    cnt_Not_Exists=0
    with open(DBPedia_Mappings_Loc, 'r') as file:
            for line in file:
                if(line.startswith("#")==False):
                    elems = line.split(" ")
                    
                    subject = elems[0]
                    predicate =  elems[1].lower()
                    if(elems[2].startswith("\"")):
                        quotationSplits = line.split("\"")
                        object_val = quotationSplits[1]
                        #object_val = quotationSplits[1]+quotationSplits[2].split(" ")[0]+"\""
                    else:
                        object_val = Extract_Resource_From_URI(elems[2])
                    
                    
                    
                    subject_Types = Search_in_Type_Mappings(Types, [subject])
                    
                    if(len(subject_Types)>0):
                        cnt_Exists+=1
                        #print("Type Exists")
                        type_val = str(subject_Types[0])
                        if(type_val in Column_Context):
                            if (predicate in Column_Context[type_val]):
                                Column_Context[type_val][predicate].add(object_val)
                            else:
                                Column_Context[type_val][predicate] = set([object_val])
                    
                        else:
                            Column_Context[type_val] = {predicate: set([object_val])}
                    else:
                        print(subject)
                        cnt_Not_Exists+=1
                        #print(line)
                        
                        

    print("cnt_Exists:"+str(cnt_Exists)+"  cnt_Not_Exists:"+str(cnt_Not_Exists)+" and Existance Ratio: "+str(cnt_Exists/(cnt_Exists+cnt_Not_Exists)))

    with open(Column_Context_Pickle_Loc, 'wb') as f1:
        pickle.dump(Column_Context ,f1)
        
        
def Create_AllColumns_From_DBPedia():
    '''
        #This Function Creates the mapping between the objects of each predicate and the Predicate_Content
        # {predicate -> [Predicate_Content]}
    '''

    
    AllColumns_From_DBPedia = {}
    with open(DBPedia_Mappings_Loc, 'r') as file:
            cnt=0
            for line in file:
                if(line.startswith("#")==False):
                    elems = line.split(" ")
                    
                    subject = elems[0]
                    predicate =  elems[1].lower()
                    if(elems[2].startswith("\"")):
                        quotationSplits = line.split("\"")
                        object_val = quotationSplits[1]
                        #object_val = quotationSplits[1]+quotationSplits[2].split(" ")[0]+"\""
                    else:
                        object_val = Extract_Resource_From_URI(elems[2])
                        
                    if(predicate not in AllColumns_From_DBPedia):
                        cnt+=1
                        AllColumns_From_DBPedia[predicate] = set([object_val])
                    else:
                        cnt+=1
                        AllColumns_From_DBPedia[predicate].add(object_val)
                        

    print("Number of Triples Processed: "+str(cnt))
    with open(Save_Parent_Folder+"/AllColumns_From_DBPedia.pickle", 'wb') as f1:
        pickle.dump(AllColumns_From_DBPedia ,f1)
        
def Fetch_Column_Context(type_val, predicate):
    Column_Context_dict={}
    with open(Column_Context_Pickle_Loc, 'rb') as f1:
            Column_Context_dict = pickle.load(f1)
    print("length context table: "+str(len(Column_Context_dict)))
    '''cnt=0
    for key in Column_Context_dict:
        print(key)
        cnt+=1
        if(cnt>10):
            break'''
    
    
    
    Result_ColumnContext={}
    predicate=predicate.lower()
    type_val = type_val.lower()
    if(type_val in Column_Context_dict):
        if(predicate in Column_Context_dict[type_val] ):
            Result_ColumnContext[predicate]=(Column_Context_dict[type_val][predicate])
        else:
            print("type_val Exists but no predicate as "+str(predicate)+" !")
            
    else:
        print("No Results!")
        
    print(Result_ColumnContext)
    return Result_ColumnContext


def Save_Column_Context_From_DBPedia(type_val):
    Column_Context_dict={}
    with open(Column_Context_Pickle_Loc, 'rb') as f1:
            Column_Context_dict = pickle.load(f1)
    
    '''cnt=0
    for key in Column_Context_dict:
        print(key)
        cnt+=1
        if(cnt>10):
            break'''
    
    Result_ColumnContext={}
    type_val = type_val.lower()
    if(type_val in Column_Context_dict):
        for predicate in Column_Context_dict[type_val]:
            Result_ColumnContext[predicate]=(Column_Context_dict[type_val][predicate])
            
    else:
        print("No Results!")
        
    print(str(Result_ColumnContext))
    
    with open(Save_Parent_Folder+"/tempColumnContext.pickle", 'wb') as f1:
        pickle.dump(Result_ColumnContext ,f1)
        
    return Result_ColumnContext



def Save_Column_Context_From_WebTables():
    with open(get_Constant("Deduplicated_Merged_Json_URL"), 'r') as f1:
        dict = json.load(f1)
        print(len(dict))
        
        Result_ColumnContext={}
        for temp in dict:
            table = temp["relation"]
            table_No_Dup=[]
            for col in table:
                col2Set = set(col)
                table_No_Dup.append(col2Set)
                
            Result_ColumnContext[temp["ID"]]=table_No_Dup
            
        with open(Save_Parent_Folder+"/AllColumns_From_WebTable.pickle", 'wb') as f1:
            pickle.dump(Result_ColumnContext ,f1)
        
            
 
def test():
    
    dbPedia = DBPedia()
    #subjects = dbPedia.Search_in_Mappings("<http://dbpedia.org/resource/Berlin>")
    print(dbPedia.Search_in_Type_Mappings(["<http://dbpedia.org/resource/Berlin>"]))
    
    
    '''with open(DBPedia_Orig_Resource_Mappings_Pickle_Loc, 'rb') as f1:
        Mappings = pickle.load(f1)
        QueryKeys=["<http://dbpedia.org/resource/Barack_Obama>"]
        triples=set()
        for key in QueryKeys:
            if (key in Mappings):
                for t in Mappings[key]:
                    triples.add((key,t[0],t[1]))
            
        print(Mappings["<http://dbpedia.org/resource/Barack_Obama>"])'''
    
    


if  (__name__ == "__main__"):
    
    
    
    start = time.time()
    #test()
    #print(dbPedia.Extract_All_Triples("<http://dbpedia.org/resource/Barack_Obama>"))
    #Create_DBPedia_Mappings()
    #Create_DBPedia_TotalRedirects()
    #print(dbPedia.Search_in_Mappings("<http://dbpedia.org/resource/Iran>"))
    #dbPedia = DBPedia()
    #print(dbPedia.Search_in_Mappings("<http://dbpedia.org/resource/Mahmoud_Ahmadinejad>","<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"))
    #Create_DBPedia_Type_Mappings()
    Create_Column_Context()
    #test()
    #Fetch_Column_Context("<http://dbpedia.org/ontology/film>","<http://dbpedia.org/ontology/>")
    #Save_Column_Context("<http://dbpedia.org/ontology/film>")
    #Save_Column_Context_From_WebTables()
    #Create_AllColumns_From_DBPedia()
    end = time.time()
    elapsed = end - start
    print("elapsed time: "+str(elapsed))

