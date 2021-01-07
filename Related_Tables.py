'''
This is the implementation of the Related Table system, from the paper:
    "Finding Related Tables", SIGMOD 2012
'''

import json
import pickle
import urllib
import re
from CommonUtilities import * 

#This Function Maps a query to Knowledge Graph's topic Unique ID.
#If No Matching was found in the Knowledge Base, then its Original Name is used.
def GetKnowledgeGraphID(query):
    api_key = "AIzaSyAZdDvUzLXth6BP08K7vNWCs7VtVvEucqI"
    
    
    service_url = 'https://kgsearch.googleapis.com/v1/entities:search'
    params = {
        'query': query,
        'limit': 1,
        'indent': True,
        'key': api_key,
    }
    url = service_url + '?' + urllib.parse.urlencode(params)
    try:
        response = json.loads(urllib.request.urlopen(url).read())
        if (len(response["itemListElement"])>0):
        
            return  (response["itemListElement"][0]["result"]["@id"])        
        else:
            return (query)
    
            
    except:
        print("Error!")
        print("query was: ")
        print(query)
        print("end")
        return (query)
    
    
            


#Index for retrieving the posting list of the table IDs
#which were used for each key entity of the corpus tables.
def Create_WIK_FreeBase():
    
    print("Create_WIK Started ... ")
    WIK_Index = {}
    
    with open(get_Constant("Deduplicated_Merged_Json_URL"), 'r') as f1:
        dict = json.load(f1)
        
        for obj in dict:
            KeyIndex = obj["keyColumnIndex"]
            
            Keys = obj["relation"][KeyIndex]
            if (obj["hasHeader"]):
                Keys.pop(obj["headerRowIndex"])
            for key in Keys:
                #if(re.search('[a-zA-Z1-9]', key)):
                key = GetKnowledgeGraphID(key)
                if (key not in WIK_Index):
                    WIK_Index[key] = [obj["ID"]]
                else:
                    if(obj["ID"] not in WIK_Index[key]):
                        WIK_Index[key].append(obj["ID"])
                        
                        
    with open(get_Constant("RelatedTable_WIK_Pickle_URL"), 'wb') as outfile:
        pickle.dump(WIK_Index, outfile)

    print("Create_WIK Finished ")
    
    
    
#Index for retrieving the posting list of the table IDs
#which were used for each key entity of the corpus tables.
def Create_WIK_NoFreeBase():
    
    print("Create_WIK Started ... ")
    WIK_Index = {}
    
    with open(get_Constant("Deduplicated_Merged_Json_URL"), 'r') as f1:
        dict = json.load(f1)
        
        for obj in dict:
            KeyIndex = obj["keyColumnIndex"]
            
            Keys = obj["relation"][KeyIndex]
            if (obj["hasHeader"]):
                Keys.pop(obj["headerRowIndex"])
            for key in Keys:
                key = string_Cleanse(key)
                if (key not in WIK_Index):
                    WIK_Index[key] = [obj["ID"]]
                else:
                    if(obj["ID"] not in WIK_Index[key]):
                        WIK_Index[key].append(obj["ID"])
                        
                        
    with open(get_Constant("RelatedTable_WIK_NoFreeBase_Pickle_URL"), 'wb') as outfile:
        pickle.dump(WIK_Index, outfile)

    print("Create_WIK Finished ")


# This index is  The Attribute Co-Occurence Statistics DB Index
# Which was introduced in Exploring Relational Web Paper
def Create_ACSDB():
    print("ACSDB Started ... ")
    ACSDB = {}
    
    with open(get_Constant("Deduplicated_Merged_Json_URL"), 'r') as f1:
        dict = json.load(f1)
        
        for obj in dict:
            if(obj["hasHeader"]):
                HeaderRowIndex = obj["headerRowIndex"]
                for col in obj["relation"]:
                    Attribute = string_Cleanse(col[HeaderRowIndex])
                    if (Attribute not in ACSDB):
                        ACSDB[Attribute] = [obj["ID"]]
                    else:
                        if(obj["ID"] not in ACSDB[Attribute]):
                            ACSDB[Attribute].append(obj["ID"])
                    


                        
                        
    with open(get_Constant("ACSDB_Pickle_URL"), 'wb') as outfile:
        pickle.dump(ACSDB, outfile)
        
        
    Write_Only_Dict_To_File_Generic(ACSDB, "ACSDB", 'C:/Saeed_Local_Files/RelatedTables')

    print("ACSDB Finished ")

    
# The Attribute Consistency Measure is based on the Co-Occurence measure
# Extracted from ACSDB Index 
# Attriute_Consistency(Atr1, Atr2) equals to Prob(a2|a1) = Freq{a1,a2}/Freq{a1}
def Attribute_Consistency(Atr1, Atr2, Match_Type):
    
    Atr1 = string_Cleanse(Atr1)
    Atr2 = string_Cleanse(Atr2)
    
    with open(get_Constant("ACSDB_Pickle_URL"), 'rb') as outfile:
        ACSDB={}
        ACSDB = pickle.load(outfile)
        
        # A1_Set and A2_Set are the sets containing the TableID of tables with header names Atr1 and Atr2
        A1_Set = set()
        A2_Set = set()
        
        if(Match_Type == "Exact_Match"):
            if(Atr1 in ACSDB ):
                A1_Set = set(ACSDB[Atr1])
            if(Atr2 in ACSDB ):
                A2_Set = set(ACSDB[Atr2])
        
        if(Match_Type == "Approximate_Match"):
            
            EditDistanceThreshold = 0.3
            
            for Atr in ACSDB:
                
                if(Edit_Distance_Ratio(Atr,Atr1)<EditDistanceThreshold or (Atr1 in Atr) ):
                        #or (Atr1 in Atr)
                        #print("Match: "+str(Atr)+" and " + str(Atr1))
                        for tableID in ACSDB[Atr]:
                            A1_Set.add(tableID)
                        
                if(Edit_Distance_Ratio(Atr,Atr2)<EditDistanceThreshold or (Atr2 in Atr) ):
                        #or (Atr2 in Atr)
                        #print("Match: "+str(Atr)+" and " + str(Atr2))
                        for tableID in ACSDB[Atr]:
                            A2_Set.add(tableID)
                        
                    
            
                
        
        Intersect = A1_Set.intersection(A2_Set)
        if(len(A1_Set)!=0):
            Consistency = len(Intersect)/len(A1_Set)
        else:
            Consistency=0
        
        return Consistency
    
#Benefit of Added_Atr_Set added to Input_Atr_Set
#Is equall to the average Attribute_Consistency of each of the added attributes to each of the input attributes
def Added_Attribute_Benefit(Input_Atr_Set, Added_Atr_Set, Match_Type):
    
    Benefit_Score = 0
    
    Addable_Atr_Set = Added_Atr_Set.difference(Input_Atr_Set)
    for Added_Atr in Addable_Atr_Set:
        Added_Atr_Benefit_Score = 0
        for Input_Atr in Input_Atr_Set:    
            Added_Atr_Benefit_Score += Attribute_Consistency(Input_Atr, Added_Atr, Match_Type)
        Benefit_Score +=  Added_Atr_Benefit_Score/len(Input_Atr_Set)
        
    return Benefit_Score
        
    
    

#Table Scoring Method in "Finding Related Tables" paper
#Score = 
def Schema_Complement_Table_Search(keyList, Input_Atr_Set, Threshold, Match_Type,  Added_Atr_Benefit_Enabled=False ):
    
    Cleansed_queryKeys_set = set()
    # Cleansing the Key List + Unique
    for key in keyList:
        Cleansed_queryKeys_set.add(GetKnowledgeGraphID(key))
            

        
    
    with open(get_Constant("RelatedTable_WIK_Pickle_URL"), 'rb') as f3:
        WIK = pickle.load(f3)
        
        DMA_MATCH = {}
        for queryKey in Cleansed_queryKeys_set:
            if (queryKey in WIK):
                WIK_Tables = WIK[queryKey]
                for table in WIK_Tables:
                    if (table not in DMA_MATCH):
                        DMA_MATCH[table] = 1
                    else:
                        DMA_MATCH[table] += 1
    
                
    
        # Populating DMA_Result(tableKey->(DMA Score,Related Attribute))
        with open(get_Constant("Deduplicated_Merged_Pickle_URL"), 'rb') as f3:
            WT_Corpus = pickle.load(f3)
            DMA_Result = {}
            querySize = len(Cleansed_queryKeys_set)
            for table in DMA_MATCH:
                if(Added_Atr_Benefit_Enabled):
                    Added_Attribute_Benefit_Score = 0
                    if(WT_Corpus[table]["hasHeader"]):
                        HeaderRowIndex = WT_Corpus[table]["headerRowIndex"]
                        Added_Table_Atr_Set = set()
                        for col in WT_Corpus[table]["relation"]:
                            Attribute = (col[HeaderRowIndex])
                            Added_Table_Atr_Set.add(Attribute)
                        Added_Attribute_Benefit_Score = Added_Attribute_Benefit(Input_Atr_Set , Added_Table_Atr_Set, Match_Type)
                    Table_Score = (DMA_MATCH[table] / querySize) * Added_Attribute_Benefit_Score
                else:
                    Table_Score = (DMA_MATCH[table] / querySize)
                    
                if (Table_Score >= Threshold):
                    DMA_Result[table] = (Table_Score)

            return DMA_Result
        
        
        

def RelatedTable_SC_OnlySetContainment_Table_Search(keyList, Threshold, MatchType, Input_Atr_Set,Added_Atr_Benefit_Enabled=False ):
    Cleansed_queryKeys_set = set()
    # Cleansing the Key List + Unique
    for key in keyList:
        Cleansed_queryKeys_set.add(string_Cleanse(key))


    if(MatchType=="Approximate_Match"):
        EditDistanceThreshold = 0.3
        

        
    DMA_MATCH = {}
    with open(get_Constant("RelatedTable_WIK_NoFreeBase_Pickle_URL"), 'rb') as f3:
        WIK = pickle.load(f3)
        
        if(MatchType=="Exact_Match"):
            DMA_MATCH = {}
            for queryKey in Cleansed_queryKeys_set:
                if (queryKey in WIK):
                    WIK_Tables = WIK[queryKey]
                    for table in WIK_Tables:
                        if (table not in DMA_MATCH):
                            DMA_MATCH[table] = 1
                        else:
                            DMA_MATCH[table] += 1
                            
        if(MatchType=="Approximate_Match"):
            DMA_MATCH = {}
            for queryKey in Cleansed_queryKeys_set:
                TableSet= set()
                for key in WIK:
                    if(Edit_Distance_Ratio(key, queryKey)<EditDistanceThreshold):
                        WIK_Tables = WIK[key]
                        for table in WIK_Tables:
                            TableSet.add(table)
                        
                        
                        
                for table in TableSet:
                    if (table not in DMA_MATCH):
                        DMA_MATCH[table] = 1
                    else:
                        DMA_MATCH[table] += 1
                
                
                
            
                # Populating DMA_Result(tableKey->(DMA Score,Related Attribute))
        with open(get_Constant("Deduplicated_Merged_Pickle_URL"), 'rb') as f3:
            WT_Corpus = pickle.load(f3)
            DMA_Result = {}
            querySize = len(Cleansed_queryKeys_set)
            for table in DMA_MATCH:
                if(Added_Atr_Benefit_Enabled):
                    Added_Attribute_Benefit_Score = 0
                    if(WT_Corpus[table]["hasHeader"]):
                        HeaderRowIndex = WT_Corpus[table]["headerRowIndex"]
                        Added_Table_Atr_Set = set()
                        for col in WT_Corpus[table]["relation"]:
                            Attribute = (col[HeaderRowIndex])
                            Added_Table_Atr_Set.add(Attribute)
                        Added_Attribute_Benefit_Score = Added_Attribute_Benefit(Input_Atr_Set , Added_Table_Atr_Set, MatchType)
                    Table_Score = (DMA_MATCH[table] / querySize) * Added_Attribute_Benefit_Score
                else:
                    Table_Score = (DMA_MATCH[table] / querySize)
                    
                if (Table_Score >= Threshold):
                    DMA_Result[table] = (Table_Score)

            return DMA_Result

        '''# Populating DMA_Result(tableKey->(DMA Score,Related Attribute))
        with open(get_Constant("Deduplicated_Merged_Pickle_URL"), 'rb') as f3:
            WT_Corpus = pickle.load(f3)
            DMA_Result = {}
            querySize = len(Cleansed_queryKeys_set)
            for table in DMA_MATCH:
                KeyColSize = len(set(WT_Corpus[table]["relation"][0]))
                if(WT_Corpus[table]["hasHeader"]):
                    KeyColSize-=1
                #minsize = min(querySize, KeyColSize)
                DMA_Score = (DMA_MATCH[table] / querySize)
                if (DMA_Score >= Threshold):
                    DMA_Result[table] = (DMA_Score)



            return DMA_Result'''
    
    


if  (__name__ == "__main__"):
    '''query="--,"
    if(re.search('[a-zA-Z1-9]', query)):
        print(GetKnowledgeGraphID(query))
    else:
        print("nothing")'''
    Create_ACSDB()
    Create_WIK_NoFreeBase()
    Create_WIK_FreeBase()
    #print(Attribute_Consistency('rank','titleclicktoview', "Approximate_Match"))
    #print(Added_Attribute_Benefit({"subject", "release", "rank"},{"podcast","titleclicktoview" }, "Approximate_Match"))
    



#kg:/m/02mjmr