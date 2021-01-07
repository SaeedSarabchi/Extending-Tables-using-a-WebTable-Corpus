'''
This is the implementation of SMW Graph used in InfoGather system.
SMW Graph is used to predict the relatedness score between a pair of Entity Attribute Binary table (EAB).
It uses a classification method for predicting this relatedness score.
'''

import json
import  pickle
import copy
from scipy import spatial
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
import collections
from CommonUtilities import *
from WDC_Analysis import *

from sklearn import svm
from sklearn import tree
from sklearn.preprocessing import normalize
from sklearn.naive_bayes import GaussianNB
import math
import numpy as np
import re, math
from collections import Counter
import operator
import networkx as nx
import time
import random
from pathlib import Path

G=nx.Graph()
Global_SMW_Graph={}





def IDF_Gen():
    with open('Sample_EAB.json', 'r') as f3:
        dict = json.load(f3)
        IDF_Index={}
        Indexed_Words=[]
        for obj in dict:
            Data= obj["textBeforeTable"]+ obj["textAfterTable"]
            Data=Data.lower()
            words=Data.split()
            for w in words:
                if(w not in Indexed_Words):
                    Indexed_Words.append(w)
                    if(w in IDF_Index):
                        IDF_Index[w]=IDF_Index[w]+1
                    else:
                        IDF_Index[w] = 1

            Indexed_Words=[]

        with open('IDFs/Context.IDF.pickle', 'wb') as outfile:
            pickle.dump(IDF_Index, outfile)

def Fetch_IDF(Key):
    with open('IDFs/Context.IDF.pickle', 'rb') as f3:
        dict = pickle.load(f3)
        print(dict[Key])

def Feature_Gen(Constant_Feature_Name):
    print("Feature_Gen "+Constant_Feature_Name+" Started ...  ")
    with open(get_Constant("EAB_Merged_Json_URL"), 'r') as f3:
        jsonFiles = json.load(f3)
        #d=collections.OrderedDict(sorted(dict.items(), key=lambda t: t[0]))
        documents = []
        tfidf_vectorizer=None
        cnt=0
        if(Constant_Feature_Name == "Feature_URL_Pickle"):
            for x in range(len(jsonFiles)):
                cnt+=1
                #print(d[obj]["ID"])
                #print(jsonFiles[x])
                Data=""
                if("url" in jsonFiles[x]):
                    Data=jsonFiles[x]["url"]
                #Corpus used for TF IDF Cosine Simmilairty
                documents.append(Data)
            tfidf_vectorizer = TfidfVectorizer()
        if (Constant_Feature_Name == "Feature_CONTEXT_Pickle"):
            for x in range(len(jsonFiles)):
                cnt += 1
                # print(d[obj]["ID"])
                Data=""
                if("textBeforeTable" in jsonFiles[x]):
                    Data = jsonFiles[x]["textBeforeTable"]
                if("textAfterTable" in jsonFiles[x]):
                    Data += " "+jsonFiles[x]["textAfterTable"]
                # Corpus used for TF IDF Cosine Simmilairty
                documents.append(Data)
            tfidf_vectorizer = TfidfVectorizer()
        if (Constant_Feature_Name == "Feature_TUPLE_Pickle"):
            for x in range(len(jsonFiles)):
                cnt += 1
                # print(d[obj]["ID"])
                Data = ""
                for y in range(len(jsonFiles[x]["relation"][0])):
                    Data += string_Cleanse(str(jsonFiles[x]["relation"][0][y]) +  str(jsonFiles[x]["relation"][1][y]))

                # Corpus used for TF IDF Cosine Simmilairty
                documents.append(Data)
            tfidf_vectorizer = TfidfVectorizer(use_idf=False, smooth_idf=False)
        if (Constant_Feature_Name == "Feature_TableContent_Pickle"):
            for x in range(len(jsonFiles)):
                cnt += 1
                # print(d[obj]["ID"])
                Data = ""
                for y in range(len(jsonFiles[x]["relation"][0])):
                    Data += " " + str(jsonFiles[x]["relation"][0][y]) + " " + str(jsonFiles[x]["relation"][1][y])

                # Corpus used for TF IDF Cosine Simmilairty
                documents.append(Data)
            tfidf_vectorizer = TfidfVectorizer()
        if (Constant_Feature_Name == "Feature_Table2Context_Pickle"):
            #Table2Context Feature is considered as the Union of the Tale Content and The Table Context of a table
            for x in range(len(jsonFiles)):
                cnt += 1
                # print(d[obj]["ID"])
                Data = ""
                for y in range(len(jsonFiles[x]["relation"][0])):
                    Data += " " + str(jsonFiles[x]["relation"][0][y]) + " " + str(jsonFiles[x]["relation"][1][y])
                if("textBeforeTable" in jsonFiles[x]):
                    Data +=" "+ jsonFiles[x]["textBeforeTable"]
                if("textAfterTable" in jsonFiles[x]):
                    Data += " "+jsonFiles[x]["textAfterTable"]

                # Corpus used for TF IDF Cosine Simmilairty
                documents.append(Data)
            tfidf_vectorizer = TfidfVectorizer()
        if (Constant_Feature_Name == "Feature_AttributeNames_Pickle"):
            for x in range(len(jsonFiles)):
                cnt += 1
                # print(d[obj]["ID"])
                Data = ""
                atr1="Null"
                atr2="Null"
                if (jsonFiles[x]["hasHeader"]):
                    atr1 = jsonFiles[x]["relation"][0][(jsonFiles[x]["headerRowIndex"])]
                    atr2 = jsonFiles[x]["relation"][1][(jsonFiles[x]["headerRowIndex"])]
                Data += atr1+" "+atr2

                # Corpus used for TF IDF Cosine Simmilairty
                documents.append(Data)
            tfidf_vectorizer = TfidfVectorizer(use_idf=False, smooth_idf=False)
        if (Constant_Feature_Name == "Feature_ColValues_Pickle"):
            for x in range(len(jsonFiles)):
                cnt += 1
                Data = ""
                Col_elements=[]
                for y in range(len(jsonFiles[x]["relation"][0])):
                    cell_data = jsonFiles[x]["relation"][0][y]
                    if cell_data not in Col_elements:
                        Col_elements.append(cell_data)
                        
                for y in range(len(jsonFiles[x]["relation"][0])):
                    cell_data = jsonFiles[x]["relation"][1][y]
                    if cell_data not in Col_elements:
                        Col_elements.append(cell_data)
                        
                for elem in Col_elements:
                     Data += str(elem)

                # Corpus used for TF IDF Cosine Simmilairty
                documents.append(Data)
            tfidf_vectorizer = TfidfVectorizer(use_idf=False, smooth_idf=False)
        if (Constant_Feature_Name == "Feature_ColWidth_Pickle"):
            for x in range(len(jsonFiles)):
                cnt += 1
                # print(d[obj]["ID"])
                Data = ""
                Col_elements=[]
                for y in range(len(jsonFiles[x]["relation"][0])):
                    cell_data = jsonFiles[x]["relation"][0][y]
                    if cell_data not in Col_elements:
                        Col_elements.append(cell_data)
                        
                for y in range(len(jsonFiles[x]["relation"][0])):
                    cell_data = jsonFiles[x]["relation"][1][y]
                    if cell_data not in Col_elements:
                        Col_elements.append(cell_data)
                        
                for elem in Col_elements:
                     Data += str(elem)
                        
                       

                # Corpus used for TF IDF Cosine Simmilairty
                documents.append(Data)
            tfidf_vectorizer = TfidfVectorizer()
        print(cnt)
        #tfidf_vectorizer = TfidfVectorizer(use_idf=False, smooth_idf=False)
        tfidf = tfidf_vectorizer.fit_transform(documents)
        print ("startOfPairwiseSim! .. ")
        print (tfidf.shape)
        pairwise_sim_sparse = tfidf *  tfidf.T
        print ("End_OfPairwiseSim! ")
        pairwise_similarity = pairwise_sim_sparse.toarray()
        Features={}

        print(len(jsonFiles))
        #print((pairwise_similarity.shape[0]))

        #Feature_URL_Pickle
        itemCntr=0
        Feature_Cnt=0
        open(get_Constant(Constant_Feature_Name), 'w')
        with open(get_Constant(Constant_Feature_Name), 'ab') as featureFile:
            for x in range(len(jsonFiles)-1):
                for y in range(x+1, len(jsonFiles)):
                    #Features[(x, y)] = (pairwise_similarity[x][y], jsonFiles[x]["ID"], jsonFiles[y]["ID"])
                    Features[(jsonFiles[x]["ID"],jsonFiles[y]["ID"])]=pairwise_similarity[x][y]
                    itemCntr+=1
                    if(itemCntr>=5000000):
                        Feature_Cnt+=5000000
                        pickle.dump(Features, featureFile)
                        itemCntr=0
                        Features={}

        if(len(Features)>0):
            with open(get_Constant(Constant_Feature_Name), 'ab') as featureFile:
                Feature_Cnt+=len(Features)
                pickle.dump(Features, featureFile)

        print("Feature Length: "+str(Feature_Cnt))
        print("Feature_Gen "+Constant_Feature_Name+" Finished ...  ")

def ColWidth_Feature_Gen():
    print("Feature_Gen ColWidth Started ...  ")
    with open(get_Constant("EAB_Merged_Json_URL"), 'r') as f3:
        jsonFiles = json.load(f3)
        Features={}
        #Feature_URL_Pickle
        itemCntr=0
        Feature_Cnt=0
        open(get_Constant("Feature_ColWidth_Pickle"), 'w')
        with open(get_Constant("Feature_ColWidth_Pickle"), 'ab') as featureFile:
            for x in range(len(jsonFiles)-1):
                for y in range(x+1, len(jsonFiles)):
                    #Features[(x, y)] = (pairwise_similarity[x][y], jsonFiles[x]["ID"], jsonFiles[y]["ID"])
                    Features[(jsonFiles[x]["ID"],jsonFiles[y]["ID"])]=ColWidth_Sim(jsonFiles[x]["relation"],jsonFiles[y]["relation"])
                    itemCntr+=1
                    if(itemCntr>=5000000):
                        Feature_Cnt+=5000000
                        pickle.dump(Features, featureFile)
                        itemCntr=0
                        Features={}

        if(len(Features)>0):
            with open(get_Constant("Feature_ColWidth_Pickle"), 'ab') as featureFile:
                Feature_Cnt+=len(Features)
                pickle.dump(Features, featureFile)

        print("Feature Length: "+str(Feature_Cnt))
        print("Feature_Gen "+"Feature_ColWidth_Pickle"+" Finished ...  ")

#This Function calculates the Column Width similarity which is discussed in Octopus paper
#The function picks the minimum pairwise difference between the average column widths, 
#and then calculates the similarity based on the mentioned difference
#However, in EAB case, the matching is keyCol->keyCol and AtribCol->AtribCol
def ColWidth_Sim(T0,T1):
    
    Table0_Avg_Width0=0
    Table0_Avg_Width1=0
    Table1_Avg_Width0=0
    Table1_Avg_Width1=0
    
    for i in range(len(T0[0])):
        Table0_Avg_Width0 += len(T0[0][i]) 
        Table0_Avg_Width1 += len(T0[1][i]) 
    
    Table0_Avg_Width0 = Table0_Avg_Width0/len(T0[0])
    Table0_Avg_Width1 = Table0_Avg_Width1/len(T0[1])
    
    
    for i in range(len(T1[0])):
        Table1_Avg_Width0 += len(T1[0][i]) 
        Table1_Avg_Width1 += len(T1[1][i]) 
    
    Table1_Avg_Width0 = Table1_Avg_Width0/len(T1[0])
    Table1_Avg_Width1 = Table1_Avg_Width1/len(T1[1])
    
    Diff1 = abs(Table1_Avg_Width0 - Table0_Avg_Width0)/(max(Table1_Avg_Width0,Table0_Avg_Width0) if max(Table1_Avg_Width0,Table0_Avg_Width0)!=0 else 1)  +  abs(Table1_Avg_Width1 - Table0_Avg_Width1)/(max(Table1_Avg_Width1,Table0_Avg_Width1) if max(Table1_Avg_Width1,Table0_Avg_Width1)!=0 else 1)
    #Diff2 = abs(Table1_Avg_Width1 - Table0_Avg_Width0)/(max(Table1_Avg_Width1, Table0_Avg_Width0) if max(Table1_Avg_Width1, Table0_Avg_Width0)!=0 else 1 ) +  abs(Table1_Avg_Width0 - Table0_Avg_Width1)/(max(Table1_Avg_Width0,Table0_Avg_Width1) if max(Table1_Avg_Width0,Table0_Avg_Width1)!=0 else 1 )
    
    #MinDiff = min(Diff1, Diff2)
       
    
    return (2-Diff1)/2
    
    
    
    
def Fetch_From_ConstantsPickle_With_TableKey_Score(FeatureConstantName, Threshold, GreaterThan=True):
    cnt=0
    Features = {}
    with open(get_Constant(FeatureConstantName), 'rb') as featureFile:
        # pickle.load(featureFile)
        while 1:
            try:
                Features = (pickle.load(featureFile))
                for item in Features:
                    if(GreaterThan):
                        if (Features[item] > Threshold):
                            print(str(item)+": "+str(Features[item]))
                    else:
                        if (Features[item] < Threshold):
                            print(str(item)+": "+str(Features[item]))
                        #a = 1

                    cnt+=1
                Features = {}
            except EOFError:
                break
    print("ConstantsPickle Size: "+str(cnt))




def get_cosine(vec1, vec2):
    intersection = set(vec1.keys()) & set(vec2.keys())
    numerator = sum([vec1[x] * vec2[x] for x in intersection])

    sum1 = sum([vec1[x] ** 2 for x in vec1.keys()])
    sum2 = sum([vec2[x] ** 2 for x in vec2.keys()])
    denominator = math.sqrt(sum1) * math.sqrt(sum2)

    if not denominator:
        return 0.0
    else:
        return float(numerator) / denominator


def text_to_vector(text):
    words = WORD.findall(text)
    return Counter(words)

def test_distance_cosine():
    dataSetI = ["salam aleikom", "hi hello", "meghsi bucoup"]
    dataSetII =  ["hi hello", "salam chetori", "onnan"]
    sim = 1 - spatial.distance.cosine(dataSetI, dataSetII)
    vector1 = text_to_vector(dataSetI)
    vector2 = text_to_vector(dataSetII)

    cosine = get_cosine(vector1, vector2)
    print(sim)

def test_TF_IDF_Vectorizor():
    tfidf_vectorizer = TfidfVectorizer()
    tfidf = tfidf_vectorizer.fit_transform(["kjasd  kjhsdf ksdjfh sdkfjhksd skdjfhksdjf sdkjfhskjdf ",
                           "An apple a day keeps the doctor away",
                             "Never compare an apple to an orange",
                             "I prefer scikit-learn to Orange",
                              "kjasd  kjhsdf ksdjfh sdkfjhksd skdjfhksdjf djfh"])

    pairwise_sim_sparse = ((tfidf * tfidf.T).A)
    pairwise_similarity = pairwise_sim_sparse.toarray()

    print(pairwise_similarity.shape)

    for x in range(pairwise_similarity.shape[0]-1):
        for y in range(x + 1, pairwise_similarity.shape[0]):
            print(pairwise_similarity[x][y])

def Generate_Small_Pickle_For_Labelling(Cnt):
    with open(get_Constant("EAB_Merged_Pickle_URL"), 'rb') as f3:
        dict = pickle.load(f3)
        Result = {}
        cntr=0
        for key in dict:
            cntr += 1
            Result[key] = dict[key]
            if(cntr>=Cnt):
                break

        with open(get_Constant("EAB_Small_Pickle"), 'wb') as outfile:
            pickle.dump(Result, outfile)


#Determines the Intersection Percentage
#between two web tables
def Relation_SIM(i,j):

    T1 = copy.deepcopy(i["relation"])
    T2 = copy.deepcopy(j["relation"])

    # Delete The Header Row for i
    if (i["hasHeader"]):
        T1[0].pop(i["headerRowIndex"])
        T1[1].pop(i["headerRowIndex"])

    # Delete The Header Row for j
    if (j["hasHeader"]):
        T2[0].pop(j["headerRowIndex"])
        T2[1].pop(j["headerRowIndex"])

    MinRow_Table=[]
    MaxRow_Table=[]
    
    if(len(T1[0])<len(T2[0])):
        MinRow_Table = T1
        MaxRow_Table = T2
    else:
        MinRow_Table = T2
        MaxRow_Table = T1
    
    # Count the Number of Intersections
    Intersections=0
    for x in range(0, len(MinRow_Table[0])):
        for y in range (0, len(MaxRow_Table[0])):
            if(Edit_Distance_Ratio(string_Cleanse(str(MinRow_Table[0][x])),string_Cleanse(str(MaxRow_Table[0][y])))<0.2 and string_Cleanse(str(MinRow_Table[0][x]))!= "" and string_Cleanse(str(MaxRow_Table[0][y]))!=""):
                if(Edit_Distance_Ratio(string_Cleanse(str(MinRow_Table[1][x])), string_Cleanse(str(MaxRow_Table[1][y])))<0.2 and string_Cleanse(str(MinRow_Table[1][x]))!= "" and string_Cleanse(str(MaxRow_Table[1][y]))!=""):
                    Intersections+=1
                    return 1
                    break
                else:
                    return -1
    #if (len(T1[0]) == len(T2[0])) and (len(T2[0]) == Intersections):
        #return 0
    simmilarity = 0
    minsize = min(len(T1[0]), (len(T2[0])))
    if(minsize!=0):
        simmilarity = float(Intersections/minsize)

    return simmilarity



'''# It is assumed that:
# 1- IF Ti INTERSECT Tl >= Theta AND
# Tj INTERSECT Tl >= Theta => Sim(Ti,Tj)=1
# 2- IF Ti(KEY) = Tl(KEY), Then IF Ti(Attr) != Tl(Attr) => Sim(Ti, Tj) !=1
# Naive and Inefficient
def Create_Labeling(labelCnt, Threashold):
    print("Create_Labeling Started ... ")
    #Generate_Small_Pickle_For_Labelling(labelCnt)
    with open(get_Constant("EAB_Merged_Pickle_URL"), 'rb') as f3:
        dict = pickle.load(f3)
        #dict2={}
        cntr=0
        Positive_Labelled=0
        Negative_Labelled=0
        #for elem in dict:
            #dict2[elem]=dict[elem]
            #cntr+=1
            #if(cntr==1000):
                #break
        #print("len dict2: "+str(len(dict2)))

        #Ti=copy.deepcopy(dict2)
        #Tl=copy.deepcopy(dict2)
        #Tj=copy.deepcopy(dict2)
        Labelled={}
        
        lablled_table_pairs=[]

        for ti in dict:
            if((Positive_Labelled)>(labelCnt/2)):
                    break
            for tl in dict:
                if((Positive_Labelled)>(labelCnt/2)):
                    break
                sim1=Relation_SIM(dict[ti],dict[tl])
                if (sim1>=Threashold):
                    for tj in dict:
                        if(((ti,tj) not in Labelled) and ((tj,ti) not in Labelled) and (ti!= tj)):
                            sim2 = Relation_SIM(dict[tl],dict[tj])

                            if (sim2 >= Threashold):

                                
                                if(TableContent_To_OrderedStr(dict[ti]["relation"]) != TableContent_To_OrderedStr(dict[tj]["relation"])):
                                            MergedTables = Merge_Tables(dict[ti]["relation"], dict[tj]["relation"])
                                            if(TableContent_To_OrderedStr(MergedTables) not in lablled_table_pairs):
                                                lablled_table_pairs.append(TableContent_To_OrderedStr(MergedTables))
                                                
                                        #For Debugging: Labelled[(ti,tj, tl)]= (sim1,sim2)
                                                Labelled[(ti,tj)]= 1
                                                Positive_Labelled+=1
                                                print(Positive_Labelled)
                                                break

        
        print("positive labels done")
        #TableContent_To_OrderedStr Should be REmoved!!
        for ti in dict:
            if(Positive_Labelled+Negative_Labelled>(labelCnt)):
                        break
            for tj in dict:
                if(Positive_Labelled+Negative_Labelled>(labelCnt)):
                        break
                if(TableContent_To_OrderedStr(dict[ti]["relation"]) == TableContent_To_OrderedStr(dict[tj]["relation"])):
                #if(ti == tj):
                    continue
                
                Add_Flag=True
                for tl in dict:
                    if(Positive_Labelled+Negative_Labelled>(labelCnt)):
                        break
                    
                    sim1 = Relation_SIM(dict[ti],dict[tl])
                    sim2 = Relation_SIM(dict[tj],dict[tl])
                    
                    if(sim1>= Threashold and sim2>=Threashold):
                        Add_Flag = False
                        break
                    
                if(Add_Flag):
                    MergedTables = Merge_Tables(dict[ti]["relation"], dict[tj]["relation"])
                    if(TableContent_To_OrderedStr(MergedTables) not in lablled_table_pairs):
                        lablled_table_pairs.append(TableContent_To_OrderedStr(MergedTables))
                    
                    #For Debugging: Labelled[(ti, tj, '0')] = (0,0)
                        Labelled[(ti, tj)] = 0
                        Negative_Labelled += 1
                        print(Negative_Labelled)
                    
                    
                        
                        


                    

        print("len Ti = "+str(len(dict)))
        print("len Labelled = "+str(len(Labelled)))
        #print(Labelled)
        with open(get_Constant('EAB_Label_Pickle'), 'wb') as outfile:
            pickle.dump(Labelled, outfile)
            
        print("Create_Labeling Finished  ")
'''

# 1- IF Ti INTERSECT Tl >= Theta AND
# Tj INTERSECT Tl >= Theta => Sim(Ti,Tj)=1
# 2- IF Ti(KEY) = Tl(KEY), Then IF Ti(Attr) != Tl(Attr) => Sim(Ti, Tj) !=1
# Naive and Inefficient
def Create_Labeling(labelCnt, Threashold):
    print("Create_Labeling Started ... ")
    Generate_Small_Pickle_For_Labelling(labelCnt*10)
    with open(get_Constant("EAB_Small_Pickle"), 'rb') as f3:
        dict = pickle.load(f3)
        #dict2={}
        cntr=0
        Positive_Labelled=0
        Negative_Labelled=0
        #for elem in dict:
            #dict2[elem]=dict[elem]
            #cntr+=1
            #if(cntr==1000):
                #break
        #print("len dict2: "+str(len(dict2)))

        #Ti=copy.deepcopy(dict2)
        #Tl=copy.deepcopy(dict2)
        #Tj=copy.deepcopy(dict2)
        Labelled={}
        
        '''lablled_table_pairs=[]

        for ti in dict:
            print(ti)
            if((Positive_Labelled)>(labelCnt/2)):
                    break
            for tl in dict:
                if(ti!=tl):
                    if((Positive_Labelled)>(labelCnt/2)):
                        break
                    sim1=Relation_SIM(dict[ti],dict[tl])
                    if (sim1>=Threashold):'''
        lablled_table_pairs=[]


        CountryTables = T2D_GoldStandard_Related_Tables("Country")

        for ti in dict:
            print(ti)
            if(ti.split("-")[0] in CountryTables):
                print("YES")
                if((Positive_Labelled)>(labelCnt/2)):
                        break
                for tl in dict:
                    if((Positive_Labelled)>(labelCnt/2)):
                        break
                    sim1=Relation_SIM(dict[ti],dict[tl])
                    if (sim1>=Threashold):
                        for tj in dict:
                            if(((ti,tj) not in Labelled) and ((tj,ti) not in Labelled) and (ti!= tj)):
                                sim2 = Relation_SIM(dict[tl],dict[tj])
    
                                if (sim2 >= Threashold):
                                    
                                    if(TableContent_To_Str(dict[ti]["relation"]) != TableContent_To_Str(dict[tj]["relation"])):
                                        if(ti<tj):
                                            MergedTables = Merge_Tables(dict[ti]["relation"], dict[tj]["relation"])
                                        else:
                                            MergedTables = Merge_Tables(dict[tj]["relation"], dict[ti]["relation"])
                                        if(TableContent_To_Str(MergedTables) not in lablled_table_pairs):
                                            lablled_table_pairs.append(TableContent_To_Str(MergedTables))
                                            
                                    #For Debugging: Labelled[(ti,tj, tl)]= (sim1,sim2)
                                            Labelled[(ti,tj)]= 1
                                            Positive_Labelled+=1
                                            print(Positive_Labelled)
                                            break

                    
                                '''if(TableContent_To_Str(dict[ti]["relation"]) != TableContent_To_Str(dict[tl]["relation"])):
                                            if(ti<tl):
                                                MergedTables = Merge_Tables(dict[ti]["relation"], dict[tl]["relation"])
                                            else:
                                                MergedTables = Merge_Tables(dict[tl]["relation"], dict[ti]["relation"])
                                            if(TableContent_To_Str(MergedTables) not in lablled_table_pairs):
                                                lablled_table_pairs.append(TableContent_To_Str(MergedTables))
                                                
                                        #For Debugging: Labelled[(ti,tj, tl)]= (sim1,sim2)
                                                Labelled[(ti,tl)]= 1
                                                Positive_Labelled+=1
                                                print(Positive_Labelled)
                                                break'''

        
        print("positive labels done")
        #TableContent_To_OrderedStr Should be REmoved!!
        for ti in dict:
            print(ti)
            orig_tables_match=[]
            if(Positive_Labelled+Negative_Labelled>(labelCnt)):
                        break
            for tj in dict:
                if(tj.split("-")[0] in orig_tables_match):
                    continue
                if(tj.split("-")[0]==ti.split("-")[0]):
                    continue
                print(tj)
                if(Positive_Labelled+Negative_Labelled>(labelCnt)):
                        break
                if(TableContent_To_Str(dict[ti]["relation"]) == TableContent_To_Str(dict[tj]["relation"])):
                #if(ti == tj):
                    continue
                
                
                    
                sim = Relation_SIM(dict[ti],dict[tj])
                
                if(sim>= Threashold or sim==-1 ):
                
                    if(sim==-1):
                        print("yes "+ti+"  ,   "+tj)
                    continue
               
                if(ti<tj):
                    MergedTables = Merge_Tables(dict[ti]["relation"], dict[tj]["relation"])
                else:
                    MergedTables = Merge_Tables(dict[tj]["relation"], dict[ti]["relation"])
                if(TableContent_To_Str(MergedTables) not in lablled_table_pairs):
                    lablled_table_pairs.append(TableContent_To_Str(MergedTables))
                
                #For Debugging: Labelled[(ti, tj, '0')] = (0,0)
                    orig_tables_match.append(tj.split("-")[0])
                    Labelled[(ti, tj)] = 0
                    Negative_Labelled += 1
                    print(Negative_Labelled)
                '''for tl in dict:
                    if(Positive_Labelled+Negative_Labelled>(labelCnt)):
                        break
                    
                    if(ti.split("-")[0]!=tl.split("-")[0]):
                        sim1 = Relation_SIM(dict[ti],dict[tl])
                    else:
                        continue
                    if(tj.split("-")[0]!=tl.split("-")[0]):
                        sim2 = Relation_SIM(dict[tj],dict[tl])
                    else:
                        continue
                    
                    if((sim1>= Threashold and sim2>=Threashold) or sim1==-1 or sim2==-1):
                        Add_Flag = False
                        if(sim1==-1 ):
                            print("yes "+ti+"  ,   "+tl)
                        if (sim2==-1):
                            print("yes "+tj+"  ,   "+tl)
                        break
            if(Add_Flag):
                if(ti<tj):
                    MergedTables = Merge_Tables(dict[ti]["relation"], dict[tj]["relation"])
                else:
                    MergedTables = Merge_Tables(dict[tj]["relation"], dict[ti]["relation"])
                if(TableContent_To_Str(MergedTables) not in lablled_table_pairs):
                    lablled_table_pairs.append(TableContent_To_Str(MergedTables))
                
                #For Debugging: Labelled[(ti, tj, '0')] = (0,0)
                    Labelled[(ti, tj)] = 0
                    Negative_Labelled += 1
                    print(Negative_Labelled)'''
                    
        

        print("len Ti = "+str(len(dict)))
        print("len Labelled = "+str(len(Labelled)))
        #print(Labelled)
        with open(get_Constant('EAB_Label_Pickle'), 'wb') as outfile:
            pickle.dump(Labelled, outfile)
            
        print("Create_Labeling Finished  ")



def Create_Labeling_Manual():
    
    
    Labelled={}
     
     
    Country_Capital={"55238374_0_3379409961751009152-1",
    "57943722_0_8666078014685775876-1",
    "61121469_0_6337620713408906340-1",
    "68779923_0_3859283110041832023-3",
    "68779923_1_3240042497463101224-3",
    "68779923_0_3859283110041832023-3"}
    
    for t in Country_Capital:
        for j in Country_Capital:
            if(((t,j) not in Labelled) and ((j,t) not in Labelled)and (j!=t)):
                Labelled[(t,j)]=1
    
    
    
    Film_Director={
    "16767252_0_2409448375013995751-3",
    "84548468_0_5955155464119382182-3",
    "73988811_0_2775758476756716904-1",
    "54719588_0_8417197176086756912-5",
    "53822652_0_5767892317858575530-3",
    "50245608_0_871275842592178099-3"}
    
    for t in Film_Director:
        for j in Film_Director:
            if(((t,j) not in Labelled) and ((j,t) not in Labelled) and (j!=t)):
                Labelled[(t,j)]=1
    
    
    Company_Industry={
    "11278409_0_3742771475298785475-2",
    "96203994_0_2127964719640427252-2",
    "64092785_0_4696367782987533337-1",
    "63389809_0_8179819543692215824-1",
    "56224555_0_3713922722778385817-3",
    "96203994_0_2127964719640427252-2"}
    
    for t in Company_Industry:
        for j in Company_Industry:
            if(((t,j) not in Labelled) and ((j,t) not in Labelled) and (j!=t)):
                Labelled[(t,j)]=1
    
    VideoGame_Publisher={
    "11833461_1_3811022039809817402-1",
    "75367212_2_2745466355267233390-2",
    "73242003_5_4847571983313033360-3",
    "57938705_0_8737506792349461963-5",
    "4501311_8_8306082458935575308-3",
    "27466715_0_3913547177671701530-3"}
    
    
    for t in VideoGame_Publisher:
        for j in VideoGame_Publisher:
            if(((t,j) not in Labelled) and ((j,t) not in Labelled)and (j!=t)):
                Labelled[(t,j)]=1
    
    cntr=0
    for t in Film_Director:
        if(cntr>15):
            break
        for j in Company_Industry:
            if(cntr>15):
                break
            if(((t,j) not in Labelled) and ((j,t) not in Labelled)and (j!=t)):
                Labelled[(t,j)]=0
                cntr+=1
                
                
    cntr=0
    for t in Company_Industry:
        if(cntr>15):
            break
        for j in VideoGame_Publisher:
            if(cntr>15):
                break
            if(((t,j) not in Labelled) and ((j,t) not in Labelled)and (j!=t)):
                Labelled[(t,j)]=0
                cntr+=1
            
    cntr=0
    for t in VideoGame_Publisher:
        if(cntr>15):
            break
        for j in Country_Capital:
            if(cntr>15):
                break
            if(((t,j) not in Labelled) and ((j,t) not in Labelled)and (j!=t)):
                Labelled[(t,j)]=0
                cntr+=1
                
    cntr=0
    for t in Country_Capital:
        if(cntr>15):
            break
        for j in Film_Director:
            if(cntr>15):
                break
            if(((t,j) not in Labelled) and ((j,t) not in Labelled)and (j!=t)):
                Labelled[(t,j)]=0
                cntr+=1
    
   

 
    
    print(len(Labelled))
    
    with open(get_Constant('EAB_Label_Pickle'), 'wb') as outfile:
            pickle.dump(Labelled, outfile)
    
    
    


def Fetch_From_Labels(ID1,ID2):
    with open(get_Constant('EAB_Label_Pickle'), 'rb') as f3:
        dict = pickle.load(f3)
        print(dict[(ID1,ID2)])

def Fetch_All_Positive_Labels():
    with open(get_Constant('EAB_Label_Pickle'), 'rb') as f3:
        dict = pickle.load(f3)
        for data in dict:
            if(dict[data]>0):
                print(str(data)+" --> "+str(dict[data]))
        
                
def Write_All_Lables_to_File():
    with open(get_Constant("EAB_Merged_Pickle_URL"), 'rb') as Small_Pickle:
        Small_Pickle_dict = pickle.load(Small_Pickle)
        with open(get_Constant('EAB_Label_Pickle'), 'rb') as f3:
            Label_dict = pickle.load(f3)
            Textfile_InfoGather_Labels = open(
                    "C:/Saeed_Local_Files/InfoGather/" + Dataset + "_Labels.txt", "w", encoding="utf-8")
            
            sorted_x = sorted(Label_dict.items(), key=lambda item:item[1], reverse=True)
            line_Num=0
            for data in sorted_x:
                line_Num+=1
                Textfile_InfoGather_Labels.write(str(line_Num)+"-Lable: "+str(data[0])+" --> "+str(data[1])+"\n")
                Textfile_InfoGather_Labels.write(JsonTable_To_String(Small_Pickle_dict[data[0][0]]))
                Textfile_InfoGather_Labels.write(JsonTable_To_String(Small_Pickle_dict[data[0][1]]))
                #if(data[0][2]!='0'):
                    #Textfile_InfoGather_Labels.write(JsonTable_To_String(Small_Pickle_dict[data[0][2]]))
                Textfile_InfoGather_Labels.write("##############################################\n")
                
                
            
            Textfile_InfoGather_Labels.close()



def SMW_Graph_Creation():
    print("SMW Graph Creation Stated ...")
    #Training Phase:
    clf = svm.SVC(probability=True)
    featureArray=[]
    labelArray=[]
    labelDict={}
    with open(get_Constant('EAB_Label_Pickle'), 'rb') as f3:
        labelDict = pickle.load(f3)
    cnt=0
    #Populating featureArray and labelArray:
    with open(get_Constant("Feature_URL_Pickle"), 'rb') as URLfeatureFile:
        with open(get_Constant("Feature_CONTEXT_Pickle"), 'rb') as CONTEXTfeatureFile:
            with open(get_Constant("Feature_TUPLE_Pickle"), 'rb') as TUPLEfeatureFile:
                with open(get_Constant("Feature_TableContent_Pickle"), 'rb') as TableContentfeatureFile:
                    with open(get_Constant("Feature_Table2Context_Pickle"), 'rb') as Table2ContextfeatureFile:
                        with open(get_Constant("Feature_AttributeNames_Pickle"), 'rb') as AttributeNamesfeatureFile:
                            with open(get_Constant("Feature_ColValues_Pickle"), 'rb') as ColValuesfeatureFile:
                                with open(get_Constant("Feature_ColWidth_Pickle"), 'rb') as ColWidthfeatureFile:

                                    while 1:
                                        try:
                                            cnt+=1
                                            print(cnt)
                                            FeatureDict={}
                                            Features = (pickle.load(URLfeatureFile))
                                            for item in labelDict:
                                                if (((item[0],item[1]) in Features) or ((item[1],item[0]) in Features)):
                                                    Features_per_item=[]
                                                    if((item[0],item[1]) in Features):
                                                        Features_per_item.append(Features[(item[0],item[1])])
                                                    else:
                                                        Features_per_item.append(Features[(item[1],item[0])])
                
                                                    FeatureDict[item] = Features_per_item
                
                                            Features = {}
                                            Features = (pickle.load(CONTEXTfeatureFile))
                                            for item in labelDict:
                                                if (((item[0],item[1]) in Features) or ((item[1],item[0]) in Features)):
                
                                                    if((item[0],item[1]) in Features):
                                                        FeatureDict[item].append(Features[(item[0],item[1])])
                                                    else:
                                                        FeatureDict[item].append(Features[(item[1],item[0])])
                
                                            Features = {}
                                            Features = (pickle.load(TUPLEfeatureFile))
                                            for item in labelDict:
                                                if (((item[0],item[1]) in Features) or ((item[1],item[0]) in Features)):
                
                                                    if((item[0],item[1]) in Features):
                                                        FeatureDict[item].append(Features[(item[0],item[1])])
                                                    else:
                                                        FeatureDict[item].append(Features[(item[1],item[0])])
                
                                            Features = {}
                                            Features = (pickle.load(TableContentfeatureFile))
                                            for item in labelDict:
                                                if (((item[0],item[1]) in Features) or ((item[1],item[0]) in Features)):
                
                                                    if((item[0],item[1]) in Features):
                                                        FeatureDict[item].append(Features[(item[0],item[1])])
                                                    else:
                                                        FeatureDict[item].append(Features[(item[1],item[0])])
                                                        
                                            Features = {}
                                            Features = (pickle.load(Table2ContextfeatureFile))
                                            for item in labelDict:
                                                if (((item[0],item[1]) in Features) or ((item[1],item[0]) in Features)):
                
                                                    if((item[0],item[1]) in Features):
                                                        FeatureDict[item].append(Features[(item[0],item[1])])
                                                    else:
                                                        FeatureDict[item].append(Features[(item[1],item[0])])
                                                        
                                            Features = {}
                                            Features = (pickle.load(AttributeNamesfeatureFile))
                                            for item in labelDict:
                                                if (((item[0],item[1]) in Features) or ((item[1],item[0]) in Features)):
                
                                                    if((item[0],item[1]) in Features):
                                                        FeatureDict[item].append(Features[(item[0],item[1])])
                                                    else:
                                                        FeatureDict[item].append(Features[(item[1],item[0])])
                                                        
                                            Features = {}
                                            Features = (pickle.load(ColValuesfeatureFile))
                                            for item in labelDict:
                                                if (((item[0],item[1]) in Features) or ((item[1],item[0]) in Features)):
                
                                                    if((item[0],item[1]) in Features):
                                                        FeatureDict[item].append(Features[(item[0],item[1])])
                                                    else:
                                                        FeatureDict[item].append(Features[(item[1],item[0])])
                                                        
                                            Features = {}
                                            Features = (pickle.load(ColWidthfeatureFile))
                                            for item in labelDict:
                                                if (((item[0],item[1]) in Features) or ((item[1],item[0]) in Features)):
                
                                                    if((item[0],item[1]) in Features):
                                                        FeatureDict[item].append(Features[(item[0],item[1])])
                                                    else:
                                                        FeatureDict[item].append(Features[(item[1],item[0])])
                
                
                                            for item in FeatureDict:
                                                featureArray.append(FeatureDict[item])
                                                labelArray.append(labelDict[item])
                
                
                                            #for x in range()
                
                
                                        except EOFError:
                                            break

    print("fitting started ...")
    
    clf.fit(featureArray, labelArray)
    
    print("fitting finished ...")

    #Classification Phase:

    #print(clf.classes_)
    #print(clf.predict_proba([[0.9]]))
    cnt=0

    #Populating feature array for prediction
    with open(get_Constant("Feature_URL_Pickle"), 'rb') as URLfeatureFile:
        with open(get_Constant("Feature_CONTEXT_Pickle"), 'rb') as CONTEXTfeatureFile:
            with open(get_Constant("Feature_TUPLE_Pickle"), 'rb') as TUPLEfeatureFile:
                with open(get_Constant("Feature_TableContent_Pickle"), 'rb') as TableContentfeatureFile:
                    with open(get_Constant("Feature_Table2Context_Pickle"), 'rb') as Table2ContextfeatureFile:
                        with open(get_Constant("Feature_AttributeNames_Pickle"), 'rb') as AttributeNamesfeatureFile:
                            with open(get_Constant("Feature_ColValues_Pickle"), 'rb') as ColValuesfeatureFile:
                                with open(get_Constant("Feature_ColWidth_Pickle"), 'rb') as ColWidthfeatureFile:
                                    open(get_Constant('SMW_Graph_Pickle'), 'wb')
                                    with open(get_Constant('SMW_Graph_Pickle'), 'ab') as outfile:
                                        while 1:
                                            try:
                                                cnt+=1
                                                print(cnt)
                                                Features = (pickle.load(URLfeatureFile))
                                                featureArray = []
                                                keyArray=[]
                                                for item in Features:
                                                    Features_per_item = []
                                                    Features_per_item.append(Features[(item)])
                                                    featureArray.append(Features_per_item)
                                                    keyArray.append(item)
                
                                                #Now that KeyArray has been Populated,
                                                #we can use that to populate the featureArray
                                                Features={}
                                                Features = (pickle.load(CONTEXTfeatureFile))
                                                for x in range(len(keyArray)):
                                                    featureArray[x].append(Features[keyArray[x]])
                
                                                Features = {}
                                                Features = (pickle.load(TUPLEfeatureFile))
                                                for x in range(len(keyArray)):
                                                    featureArray[x].append(Features[keyArray[x]])
                
                                                Features = {}
                                                Features = (pickle.load(TableContentfeatureFile))
                                                for x in range(len(keyArray)):
                                                    featureArray[x].append(Features[keyArray[x]])
                                                    
                                                Features = {}
                                                Features = (pickle.load(Table2ContextfeatureFile))
                                                for x in range(len(keyArray)):
                                                    featureArray[x].append(Features[keyArray[x]])
                                                    
                                                Features = {}
                                                Features = (pickle.load(AttributeNamesfeatureFile))
                                                for x in range(len(keyArray)):
                                                    featureArray[x].append(Features[keyArray[x]])
                                                    
                                                Features = {}
                                                Features = (pickle.load(ColValuesfeatureFile))
                                                for x in range(len(keyArray)):
                                                    featureArray[x].append(Features[keyArray[x]])
                                                    
                                                Features = {}
                                                Features = (pickle.load(ColWidthfeatureFile))
                                                for x in range(len(keyArray)):
                                                    featureArray[x].append(Features[keyArray[x]])
                                                    
                                                
                
                
                                                #Prediction Phase
                                                labelPrediction = clf.predict_proba(featureArray)
                
                                                SMW_Graph = {}
                                                for x in range(len(keyArray)):
                                                    SMW_Graph[keyArray[x]] = labelPrediction[x][1]
                                                pickle.dump(SMW_Graph, outfile)
                
                                            except EOFError:
                                                break
                                            
    print("SMW Graph Creation finished ...")


def Fetch_SMW_Graph_For_Table(tableID, Threshold_Edge_Weight):
    with open(get_Constant("SMW_Graph_Pickle"), 'rb') as SMWGraphFile:
        # pickle.load(featureFile)
        SMW_Graph={}
        while 1:
            try:
                PartialGraph = (pickle.load(SMWGraphFile))

                for item in PartialGraph:
                    SMW_Graph[item] = PartialGraph[item]

                        
            except EOFError:
                break
        cntr=0
        for key in SMW_Graph:
            if (key[0] == tableID or key[1] == tableID):
                if(SMW_Graph[key]>=Threshold_Edge_Weight):
                    print(str(key)+" --> "+str(SMW_Graph[key]))
                    cntr+=1
        print(cntr)
        

#By Cleaning SMW Graph we mean:
#1- deleting those edges from the same original table
def Clean_SMW_Graph():
    
    with open(get_Constant("SMW_Graph_Pickle"), 'rb') as SMWGraphFile:
        SMW_Graph={}
        while 1:
                try:
                    PartialGraph = (pickle.load(SMWGraphFile))
    
                    for item in PartialGraph:
                        SMW_Graph[item] = PartialGraph[item]
    
                            
                except EOFError:
                    break
         
            
    SMW_Graph_New = SMW_Graph.copy()        
    for (i,j) in SMW_Graph:
        t1=i.split("-")[0]
        t2=j.split("-")[0]
        if(t1==t2):
            del SMW_Graph_New[(i,j)]
            
            
    with open(get_Constant('SMW_Graph_Pickle'), 'wb') as outfile:
        pickle.dump(SMW_Graph_New, outfile)

    
        

def Write_SMW_to_File(NumberOfEdges, Threshold):
    with open(get_Constant("EAB_Merged_Pickle_URL"), 'rb') as f3:
        dict = pickle.load(f3)
        with open(get_Constant("SMW_Graph_Pickle"), 'rb') as SMWGraphfile:
            SMW_Graph = pickle.load(SMWGraphfile)
            print(len(SMW_Graph))
            Textfile_SMW_Graph_Log = open(
                        "C:/Saeed_Local_Files/InfoGather/" + Dataset + "_SMW_Graph_Log.txt", "w", encoding="utf-8")
            line_Num=0 
            for data in SMW_Graph:
                if(SMW_Graph[data]>=Threshold):
                    line_Num+=1
                    Textfile_SMW_Graph_Log.write(str(line_Num)+"-Lable: "+str(data)+" --> "+str(SMW_Graph[data])+"\n")
                    Textfile_SMW_Graph_Log.write(JsonTable_To_String(dict[data[0]]))
                    Textfile_SMW_Graph_Log.write(JsonTable_To_String(dict[data[1]]))
                 
                    Textfile_SMW_Graph_Log.write("##############################################\n")
                    if(line_Num>NumberOfEdges):
                        break
                
                
            
            Textfile_SMW_Graph_Log.close()
            
            

#Generate the Full Personalized Page Rank for 
#All nodes w.r.t. all edges with weight higher than a Threshold
def Generate_FPPR(Threshold):

    G=nx.Graph()
    #global G
    with open(get_Constant("SMW_Graph_Pickle"), 'rb') as SMWGraphFile:
        # pickle.load(featureFile)
        num=0
        while 1:
            try:
                PartialGraph = (pickle.load(SMWGraphFile))

                for item in PartialGraph:
                    if(PartialGraph[item]> Threshold):
                        G.add_edge(item[0],item[1],weight=PartialGraph[item])
                        num+=1

                        
            except EOFError:
                break
    print("Graph Loaded! With Size: "+str(num)+", #Nodes: "+str(len(G.nodes()))+" #Edges: "+ str(len(G.edges())))
    FPPR={}
    #with open(get_Constant("EAB_Merged_Pickle_URL"), 'rb') as f3:
        #dict = pickle.load(f3)
    for table in G.nodes():
        if (table=="1764-1"):
            
            
            print("Node: "+str(table))
            start_time = time.time()
            #G_Prime=nx.Graph()
            #EgoGraph(G, radius=10)
            #edge_list = list(nx.dfs_edges(G,table))
            
            #G_Prime.add_edges_from(edge_list)
            Paths = nx.single_source_shortest_path(G,table,cutoff=10)
            sel_nodes=[]
            for k in Paths:
                for elem in Paths[k]:
                    if elem not in sel_nodes:
                        sel_nodes.append(elem)
            G_Prime = G.subgraph(sel_nodes)
            
            
            '''paths_list=[]
            for n in Ego_Graph.nodes():
                all_paths = nx.all_simple_paths(Ego_Graph,table, n,cutoff=10)
                for p in all_paths:
                    if(p not in paths_list ):
                        paths_list.append(p)
            for p in paths_list:
                G_Prime.add_path(p)'''
                
            #G_Prime = G.subgraph(sel_nodes)
            #nx.ego_graph(G, table, radius=1, center=True, undirected=True, distance=None)
            elapsed_time = time.time() - start_time
            print(str(table)+" ego graph Done in "+str(elapsed_time)+" Seconds")
            print("len(G_Prime): "+str(len(G_Prime)))
            print("EgoGraph Loaded! With Edge Number: "+str(len(G_Prime.edges())))
            print("EgoGraph Loaded! With Node Number: "+str(len(G_Prime.nodes())))
            
            #personalizedMatrix={}
            #Populating Personalization Matrix:
            #for t in G_Prime.nodes():
                #if(t != table):
                    #personalizedMatrix[t]=0
                #else:
                    #personalizedMatrix[t]=1
                    
            print("nx.pagerank Start ...")
            start_time = time.time()
            #pr = nx.pagerank(G_Prime, alpha=0.85, personalization=personalizedMatrix,  tol=1e-04)
            pr=Personalized_PageRank(G,table,iteration=20)
            
            elapsed_time = time.time() - start_time
            print("nx.pagerank Finished in  "+str(elapsed_time)+" Seconds")
            FPPR[table]=pr
            print(str(table)+" Done!")
        
    with open(get_Constant("FPPR"), 'wb') as outfile:
        pickle.dump(FPPR, outfile)
       

    

def Generate_Parrallel_PPR(Threshold):
    print("Parallel PPR Started ...")
    G=nx.Graph()
    with open(get_Constant("EAB_Merged_Pickle_URL"), 'rb') as f3:
        All_EABs = pickle.load(f3)
        for table in All_EABs:
            G.add_node(table)
    with open(get_Constant("SMW_Graph_Pickle"), 'rb') as SMWGraphFile:
        # pickle.load(featureFile)
        num=0
        while 1:
            try:
                PartialGraph = (pickle.load(SMWGraphFile))

                for item in PartialGraph:
                    if(PartialGraph[item]>= Threshold):
                        
                        G.add_edge(item[0],item[1],weight=PartialGraph[item])
                        num+=1
                        
                            
                    
                        

                        
            except EOFError:
                break
    print("Graph Loaded! With Size: "+str(num))
    
    Node_Num = len(G.nodes())
    print("NodeNum: "+str(Node_Num))
    
    FPPR_FolderName = get_Constant("FPPR")+"_Files"
    
    if not os.path.exists(FPPR_FolderName):
        os.makedirs(FPPR_FolderName)
    else:
        for the_file in os.listdir(FPPR_FolderName):
            file_path = os.path.join(FPPR_FolderName, the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                #elif os.path.isdir(file_path): shutil.rmtree(file_path)
            except Exception as e:
                print(e)
    Oct = int(Node_Num/8)
    #PPR_Gen(G,1,Oct)
    
    
    pool = Pool(processes=8)
    
    
    pool.apply_async(PPR_Gen, [G,0,Oct])
    pool.apply_async(PPR_Gen, [G,Oct,2*Oct])
    pool.apply_async(PPR_Gen, [G,2*Oct,3*Oct])
    pool.apply_async(PPR_Gen, [G,3*Oct,4*Oct])
    pool.apply_async(PPR_Gen, [G,4*Oct,5*Oct])
    pool.apply_async(PPR_Gen, [G,5*Oct,6*Oct])
    pool.apply_async(PPR_Gen, [G,6*Oct,7*Oct])
    pool.apply_async(PPR_Gen, [G,7*Oct,Node_Num])
    
    
    '''PPR_Gen(G,0,Oct)
    PPR_Gen(G,Oct,2*Oct)
    PPR_Gen(G,2*Oct,3*Oct)
    PPR_Gen(G,3*Oct,4*Oct)
    PPR_Gen(G,4*Oct,5*Oct)
    PPR_Gen(G,5*Oct,6*Oct)
    PPR_Gen(G,6*Oct,7*Oct)
    PPR_Gen(G,7*Oct,Node_Num)'''
    
    pool.close()
    pool.join()
    
    
    print("Parallel PPR Finished ...")
    
    
    

            
        
        
        #PPR_Gen(table)
    
    
def PPR_Gen(G, StartIdx, EndIdx):
    
    Nodes = list(G.nodes())
    for i in range(StartIdx, EndIdx):
        table = Nodes[i]
        FPPR={}
        Paths = nx.single_source_shortest_path(G,table,cutoff=20)
        sel_nodes=[]
        for k in Paths:
            for elem in Paths[k]:
                if elem not in sel_nodes:
                    sel_nodes.append(elem)
        G_Prime = G.subgraph(sel_nodes)
        #G_Prime = nx.ego_graph(Graph, Personalized_node, radius=10, center=True, undirected=True, distance=None)
        #print("EgoGraph Loaded! With Size: "+str(len(G_Prime.edges())))
        #print(str(table)+" ego graph Done!")
        #personalizedMatrix={}
        #Populating Personalization Matrix:
        #for t in G_Prime.nodes():
            #if(t != Personalized_node):
                #personalizedMatrix[t]=0
            #else:
                #personalizedMatrix[t]=1
                
        #print("nx.pagerank Start ...")
        #pr = nx.pagerank(G_Prime, alpha=0.85, personalization=personalizedMatrix,  tol=1e-04)
        pr=Personalized_PageRank(G_Prime,table,iteration=20)
        #print("nx.pagerank End ...")
        
        FPPR[table]=pr
        

        with open(get_Constant("FPPR")+"_Files/"+str(table), 'wb') as outfile:
            pickle.dump(FPPR, outfile)
  

def Merge_PPR_To_FPPR():
    FPPR={}
    with open(get_Constant("EAB_Merged_Pickle_URL"), 'rb') as f3:
        dict = pickle.load(f3)
        print(len(dict))
        for table in dict:
            my_file = Path(get_Constant("FPPR")+"_Files/"+str(table))
            if my_file.is_file():
                with open(get_Constant("FPPR")+"_Files/"+str(table), 'rb') as PPRfile:
                    PPR=pickle.load(PPRfile)
                    FPPR[table]=PPR[table]
                    #test(PPR[table])
            
    with open(get_Constant("FPPR"), 'wb') as outfile:
        pickle.dump(FPPR, outfile)
    
def test(PPR):
    sorted_list = sorted(PPR.items(), key=lambda item:item[1], reverse=True)
    print(sorted_list[0])
    for key in PPR:
        print(key)
    

def Generate_Manual_FPPR(Threshold):
    

    with open(get_Constant("SMW_Graph_Pickle"), 'rb') as SMWGraphFile:
        # pickle.load(featureFile)
        num=0
        while 1:
            try:
                PartialGraph = (pickle.load(SMWGraphFile))

                for item in PartialGraph:
                    if(PartialGraph[item]> Threshold):
                        #G.add_edge(item[0],item[1],weight=PartialGraph[item])
                        if(item[0] not in Global_SMW_Graph):
                            Global_SMW_Graph[item[0]] = [(item[1], PartialGraph[item])]
                        else:
                            Global_SMW_Graph[item[0]].append((item[1], PartialGraph[item]))
                            
                        if(item[1] not in Global_SMW_Graph):
                            Global_SMW_Graph[item[1]] = [(item[0], PartialGraph[item])]
                        else:
                            Global_SMW_Graph[item[1]].append((item[0], PartialGraph[item]))
                        
                        
                        num+=1

                        
            except EOFError:
                break
    
    cnt=0
    for k in Global_SMW_Graph:
        cnt+=len(Global_SMW_Graph[k])
    print("AVG Node Link is: "+str(cnt/len(Global_SMW_Graph)))
        
        
        
    FPPR={}
    for table in Global_SMW_Graph:
        if (table=="5-2"):
            print("Node: "+str(table))
            start_time = time.time()
            G_Prime = EgoGraph(table,  radius=2)
            #nx.ego_graph(G, table, radius=10, center=True, undirected=True, distance=None)
            elapsed_time = time.time() - start_time
            print("EgoGraph Loaded! With Edge Number: "+str(len(G_Prime.edges())/2))
            print("EgoGraph Loaded! With Node Number: "+str(len(G_Prime.nodes())))
            print(str(table)+" ego graph Done in "+str(elapsed_time)+" Seconds")       
            personalizedMatrix={}
            #Populating Personalization Matrix:
            for t in G_Prime:
                if(t != table):
                    personalizedMatrix[t]=0
                else:
                    personalizedMatrix[t]=1
                    
            print("nx.pagerank Start ...")
            start_time = time.time()
            pr = nx.pagerank(G_Prime, alpha=0.85, personalization=personalizedMatrix,  tol=1e-04)
            
            elapsed_time = time.time() - start_time
            print("nx.pagerank Finished in  "+str(elapsed_time)+" Seconds")
            FPPR[table]=pr
            print(str(table)+" Done!")
            
            
        
    with open(get_Constant("FPPR"), 'wb') as outfile:
        pickle.dump(FPPR, outfile)

def EgoGraph(node,  radius):
    #G=nx.Graph()
    
    #Integrated_Graph={}
    G={}
    for (n,score) in Global_SMW_Graph[node]:
        
        #G={}
        
        #G.add_edge(node,n,weight=score)
        #if node not in G:
            #G[node]=[(n,score)]
        #else:
            #G[node].append((n,score))
            
        #if n not in G:
            #G[n]=[(node,score)]
        #else:
            #G[n].append((node,score))
        Seen_Nodes={}
        G[(n,node)]=score
        G[(node,n)]=score
        Seen_Nodes[node]=1
        #tmpGraph=Build_Recursive_EgoGraph(G, n, 2, radius)
        Build_Recursive_EgoGraph(G, n, 2, radius, Seen_Nodes)
        #Integrated_Graph.update(tmpGraph)
            
        
    
    Directed_Graph = nx.DiGraph()
    
    for (n1,n2) in G:
        Directed_Graph.add_edge(n1, n2, weight=G[(n1,n2)])
        
    
    return Directed_Graph

def Build_Recursive_EgoGraph(Graph, node, CurrentRadius, Maxradius, Seen_Nodes):
    if(CurrentRadius>Maxradius):
        #return Graph
        return 
    #Integrated_Graph = Graph
    Seen_Nodes[node]=1
    for (n,score) in Global_SMW_Graph[node]:
        #if(n not in Graph.nodes() ):
            
            if (node==2 ):
                a=1
            if (node, n) not in Graph:
                Graph[(node,n)] = score
                
            if (n,node) not in Graph:
                Graph[(n,node)]= score
                
                
           
            #Rec_Graph = Graph
            #tmpGraph = Build_Recursive_EgoGraph(Rec_Graph, n, CurrentRadius+1, Maxradius)
            #print(Seen_Nodes)
            #print(node)
            #print(CurrentRadius)
            
            #Rec_Seen_Nodes=copy.deepcopy(Seen_Nodes)
            Rec_Seen_Nodes=(Seen_Nodes)
            if n not in Seen_Nodes:
                Build_Recursive_EgoGraph(Graph, n, CurrentRadius+1, Maxradius, Rec_Seen_Nodes)
                #Integrated_Graph.update(tmpGraph)
    

    #return Integrated_Graph        
                

    
    

 
def Write_FPPR_To_File():
     with open(get_Constant("FPPR"), 'rb') as FPPRFile:
         FPPR_Dict = pickle.load(FPPRFile)
         Textfile_FPPR_Log = open(
                        "C:/Saeed_Local_Files/InfoGather/" + Dataset + "_FPPR.txt", "w", encoding="utf-8")
         cntr=0
         for node in FPPR_Dict:
             if(node=='11278409_0_3742771475298785475-4'):
                 Textfile_FPPR_Log.write(str(node)+" : \n")
                 sorted_list = sorted(FPPR_Dict[node].items(), key=lambda item:item[1], reverse=True)
                 Textfile_FPPR_Log.write(str(sorted_list)+"\n")
                 Textfile_FPPR_Log.write("\n####################################\n")
                 cntr += 1
                 #if(cntr>1000):
                     #break
             
         Textfile_FPPR_Log.close()
             

def Compute_TSP(Seed_Table_Scores, Threshold):
     with open(get_Constant("FPPR"), 'rb') as FPPRFile:
         FPPR_Dict = pickle.load(FPPRFile)
         
         TSP_Score_Dict={}
         
         #Calculating TSP Scores for each node
         for node in FPPR_Dict:
             TSP_Score=0
             for table in Seed_Table_Scores:
                 if table in FPPR_Dict:
                     if node in FPPR_Dict[table]:
                         TSP_Score += Seed_Table_Scores[table]*FPPR_Dict[table][node]
             if(TSP_Score >= Threshold):
                 TSP_Score_Dict[node] = TSP_Score
                 
         return TSP_Score_Dict


def SeedMinTSPScore(Seed_Table_Scores, InitThreshold):
     with open(get_Constant("FPPR"), 'rb') as FPPRFile:
         FPPR_Dict = pickle.load(FPPRFile)
         
         MinTSPScore = InitThreshold
         #Calculating TSP Scores for each node
         for table in Seed_Table_Scores:
             TSP_Score=0
             for node in Seed_Table_Scores:
                 if table in FPPR_Dict:
                     if node in FPPR_Dict[table]:
                         TSP_Score += Seed_Table_Scores[table]*FPPR_Dict[table][node]
             if(TSP_Score < MinTSPScore):
                 MinTSPScore = TSP_Score
                 
        
         print(MinTSPScore)
         return MinTSPScore
    
    
    
def Agglomerative_Cluster(SynLists):
    #Removing the second element of each dictionary value tuple, which is the Source Table ID, e.g.: 'popular vote': (0.15523607243369958, '3499-1')
    for elem in SynLists:
        for key in elem[0]:
            elem[0][key] = elem[0][key][0]
            
    Final_Clusters = SynLists
    while(len(Final_Clusters)>1):
        Cosine_Sim_Dict={}
        for i in range(len(Final_Clusters)-1):
            for j in range(i+1, len(Final_Clusters)):
                Cosine_Sim_Dict[(i,j)] = (Cosine_Sim(Final_Clusters[i][0], Final_Clusters[j][0]))
        
           
        if(not Is_Mergable(Cosine_Sim_Dict)):
            break
        MustBeMerged_Tuple = max(Cosine_Sim_Dict.items(), key=operator.itemgetter(1))[0]
        Final_Clusters = MergeClusters(Final_Clusters, MustBeMerged_Tuple )
    
    #Sort the Clusters based on Cluster Scores
    return sorted(Final_Clusters, key=lambda item:item[1], reverse=True)


def Cosine_Sim(doc1, doc2):

    
    
    
    #Initializing Vector:
    vector=[]
    for elem in doc1:
        if (elem not in vector):
            vector.append(elem)
    for elem in doc2:
        if (elem not in vector):
            vector.append(elem)
        
    #Populating Vector1   
    vector1=[]
    if(len(vector)==0):
        vector1=[0.01]
    else:
        for i in range(len(vector)):
            if(vector[i] in doc1):
                vector1.append(doc1[vector[i]])
            else:
                vector1.append(0)
             
             
    #Populating Vector2
    vector2=[]
    if(len(vector)==0):
        vector2=[0.01]
    else:
        for i in range(len(vector)):
            if(vector[i] in doc2):
                vector2.append(doc2[vector[i]])
            else:
                 vector2.append(0)
    
    Sim = 1 - spatial.distance.cosine(vector1, vector2)
                
    return Sim

# Merge two Clusters with indeces MustBeMerged_Tuple[0] and MustBeMerged_Tuple[0]
def MergeClusters(Clusters, MustBeMerged_Tuple):
    #print(len(Clusters))
    
    #print(MustBeMerged_Tuple)
    ToBeMerged1 = Clusters.pop(MustBeMerged_Tuple[0])
    ToBeMerged2 = Clusters.pop(MustBeMerged_Tuple[1]-1)
    
    Merged_Elems_Dict={}
    for elem in ToBeMerged1[0]:
        Merged_Elems_Dict[elem] = ( ToBeMerged1[0][elem])
        
    for elem in ToBeMerged2[0]:
        if(elem in Merged_Elems_Dict):
            Merged_Elems_Dict[elem]+=  ToBeMerged2[0][elem]
        else:
             Merged_Elems_Dict[elem] = ( ToBeMerged2[0][elem])
            
    #Normalizing Scores
    '''Total_Score=0
    for elem in Merged_Elems_Dict:
        Total_Score += Merged_Elems_Dict[elem]
        
    for elem in Merged_Elems_Dict:
        Merged_Elems_Dict[elem] =  Merged_Elems_Dict[elem] / Total_Score'''
        
        
    Merged_Score = ToBeMerged1[1] + ToBeMerged2[1]
    Merged_Cluster = (Merged_Elems_Dict,Merged_Score)
    Clusters.append(Merged_Cluster)
                    
            

    return Clusters

def Is_Mergable(Cosine_Sim_Dict):
    for key in Cosine_Sim_Dict:
        if(Cosine_Sim_Dict[key]>0.3):
            return True
    
    return False


    
def Personalized_PageRank(Graph, Personalized_Node, damp_Factor=0.85, iteration=100):
    start_time = time.time()
    #print(Graph.nodes())
    Adj=nx.adjacency_matrix(Graph,weight='weight').todense()
    #print("Adj:")
    #print(Adj)
    Column_Normalized = normalize(Adj, norm='l1', axis=0)
    #print("Row_Normalized")
    #print(Row_Normalized)
    '''test=np.multiply(2,Row_Normalized)
    print("test")
    print(test)'''
        
    
    States=[]
    for node in Graph.nodes():
        if(node==Personalized_Node):
            States.append(1)
        else:
            States.append(0)
            
    #print("States")
    #print(States)
    Personalization_Matrix=States
        
    for i in range(iteration):
        States=np.add(np.multiply(damp_Factor,np.dot(Column_Normalized, States)),np.multiply((1-damp_Factor),Personalization_Matrix))
        #print("inter-Res: ")
        #print(States)
        #States = [float(i)/sum(States) for i in States]
        #States = (normalize(States.reshape(1,-1), norm='l1', axis=0))
        #print("inter-Norm: ")
        #print(States)
        #States = normalize(States, norm='l1', axis=1)
    
    #States = [float(i)/sum(States) for i in States]
    cnt=0
    for elem in States:
        cnt+=elem
    
    States=np.multiply(1/cnt,States)
    
    #for i in range(8000):
        #G.add_edge(i,i+1,weight=i)
        
    #for node in Graph.nodes():
        
        
    elapsed_time = time.time() - start_time
    print(" Personalized_PageRank elapsed time : "+str(elapsed_time))
    print("Result: ")
    PPR={}
    Graph_nodes = list(Graph.nodes())
    for i in range(len(Graph_nodes)):
        PPR[Graph_nodes[i]] = States[i]
    
    #print(pr)
          
            
    #PPR=0
    
    return PPR
         
    

if  (__name__ == "__main__"):
    #Merge_PPR_To_FPPR()
    #test_distance_cosine()
    #IDF_Gen()
    #Feature_Gen("Feature_TableContent_Pickle")
    #test_TF_IDF_Vectorizor()
    #Fetch_From_ConstantsPickle_With_TableKey_Score("SMW_Graph_Pickle",0.05, False)
    #Create_Labeling(200,0.3)
    #compare_Three_Webtables('27-2', '76-2')
    #Fetch_All_Positive_Labels()
    #SMW_Graph_Creation()
    #Create_Labeling(50,0.3)
    #Write_All_Lables_to_File()
    #input("something: ")
    #print(editdistance.eval("intention","execution"))
    #T1=[["asd","sasdf","as","qweee","asdasd"],["asdfdsa","asdfas","app","a","asd"]]
    #T2=[["asdhgjh","jhkjhkjhkjh","as","qweee","hgjhgjhgjhg","khkjhkhkhkhkjhkhjkhkjh"],["kjhkj","asdfas","jhgjhgjhgjhg","kjhkjhkjh","kjhkjhkjhk","khkjhkhkhkhkjhkhjkhkjh"]]
    #print(ColWidth_Sim(T1,T2))
    #ColWidth_Feature_Gen()
    #Clean_SMW_Graph()
    #Write_SMW_to_File(10000,0.5)
    
    #for i in range(1,10):
        #print(i)
    #Generate_FPPR(0.3)
    #Generate_Manual_FPPR(0.3)
    #Generate_Parrallel_FPPR(0.3)
    
    #print("transpose: "+str(np.transpose([1,2,3])))
    
    
    #G=nx.Graph()
    #G.add_edge(1,2, weight=0.91)
    #G.add_edge(2,3, weight=0.91)
    #G.add_edge(2,4, weight=0.91)
    #G.add_edge(1,5, weight=0.11)
    '''G.add_edge(5,6, weight=0.91)
    G.add_edge(6,7, weight=0.91)
    G.add_edge(6,8, weight=0.91)
    G.add_edge(8,9, weight=0.91)
    G.add_edge(1,4, weight=0.91)
    G.add_edge(5,4, weight=0.91)'''
    
    
    #Personalized_PageRank(G,1,iteration=100)
    #print("nx.pagerank")
    #print(nx.pagerank(G, alpha=0.85, personalization={1:1,2:0,5:0},  tol=1e-04, weight='weight'))
    #print(G.subgraph([1,2,3,4,5,6,7,8,9]).edges())
      

    '''Global_SMW_Graph[1]=[(2,0.1)]
    Global_SMW_Graph[2]=[(1,0.1)]
    Global_SMW_Graph[2].append((3,0.1))
    Global_SMW_Graph[3] = [(2,0.1)]
    Global_SMW_Graph[2].append((4,0.1))
    Global_SMW_Graph[4] = [(2,0.1)]
    Global_SMW_Graph[1].append((5,0.1))
    Global_SMW_Graph[5] = [(1,0.1)]
    Global_SMW_Graph[5].append((6,0.1))
    Global_SMW_Graph[6] = [(5,0.1)]
    Global_SMW_Graph[6].append((7,0.1))
    Global_SMW_Graph[7]=[(6,0.1)]
    Global_SMW_Graph[6].append((8,0.1))
    Global_SMW_Graph[8]=[(6,0.1)]
    Global_SMW_Graph[8].append((9,0.1))
    Global_SMW_Graph[9] = [(9,0.1)]
    
    
    Global_SMW_Graph[1].append((4,0.1))
    Global_SMW_Graph[4].append((1,0.1))
    
    
    Global_SMW_Graph[4].append((5,0.1))
    Global_SMW_Graph[5].append((4,0.1))'''

    
    #radius = 4
    #print(nx.ego_graph(G, 1, radius=radius, center=True, undirected=True, distance=None).edges())
    #print("#Edge: "+str(len(nx.ego_graph(G, 1, radius=radius  , center=True, undirected=True, distance=None).edges())))
    #H=nx.Graph()
    #H.add_path([1,2,3])
    #paths = nx.single_source_shortest_path(G,1,cutoff=radius)
    '''for n in G.nodes():
        all_paths = nx.all_simple_paths(G,1, n,cutoff=radius)
        for p in all_paths:
            H.add_path(p)'''
    #edge_list = list(nx.dfs_edges(G,1))
    #H.add_edges_from(edge_list)
    #paths=list(nx.dfs_edges(G,1))
    #print(paths)
    #for p in paths:
        #H.add_path(paths[p])
    #print(H.edges())
    #print(len(H.edges()))
    
    #print("\n##################\n")
    
    #print(EgoGraph(1,  radius).edges())
    #print("#Edge: "+str(len(EgoGraph(1, radius).edges())))
  

    
    Write_FPPR_To_File()
    #Fetch_FPPR()
    
    
    #Fetch_SMW_Graph_For_Table('11278409_0_3742771475298785475-4', 0.3)
    
    #Write_FPPR_To_File()
    #SMW_Graph_New={}
    #with open(get_Constant('SMW_Graph_Pickle'), 'wb') as outfile:
        #pickle.dump(SMW_Graph_New, outfile)
    
    #14067031_0_559833072073397908-5
    #10630177_0_4831842476649004753-0
    #0.0177957316268
    
    
    
    #Matrix={(0,1):0.5, (0,2):0.1, (1,2):0.8}
    #print(max(Matrix.items(), key=operator.itemgetter(1))[0])
    #doc1 = {"salam":0.2,"bye": 0.4, "aghebat":0.3, "darya":0.7}
    #doc2 = {"salam":0.2,"bye": 0.4, "aghebat":0.3}
    #Clusters=[({"ahaay":0.3, "bingo":0.4},0.5),(doc1,0.4),(doc2,0.3), ({"salam":0.6,"chetor":0.3,"bingo":0.2},0.3)]
    
    #Seed_Table_Scores={'456-2':0.1,'1332-1':0.2}
    #print(Compute_TSP(Seed_Table_Scores,0))
    #print(Agglomerative_Clustering(Clusters))
    
    #print(Cosine_Sim(doc1, doc2))
    #Write_All_Lables_to_File()
    #Generate_Parrallel_PPR(0.3)
    #Generate_Parrallel_PPR(0.1)
    #Create_Labeling_Manual()