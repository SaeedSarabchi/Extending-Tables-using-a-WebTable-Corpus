'''
This is the implenetation of Extend function in Octopus system.
'''

import json
import pickle
import copy
from WDC_Analysis import *
#from scipy import spatial
#from scipy.optimize import linear_sum_assignment
#from sklearn.metrics.pairwise import cosine_similarity
#from sklearn.feature_extraction.text import TfidfVectorizer
import collections
from CommonUtilities import *
from SMW_Graph_Module import *
from Dataset_Gathering import *
#from sklearn import svm
#from sklearn import tree
#from sklearn.naive_bayes import GaussianNB
import math
#import numpy as np
import re, math
from collections import Counter
from itertools import *
from InfoGather import *








GlobalString = ""





class WebTable:
    def __init__ (self, table):
        self.table = table
    URL=""
    QueryKeyElems=[]
    HeaderRowIdx=-1
    Num_of_QueryElems=0
    
    def ToStr(self):
        Result=""
        for i in range(len(self.table[0])):
            for j in range(len(self.table)):
                Result += " ["+str(self.table[j][i])+"] "
            Result += "\n"
        Result +="****\n"
        Result += "URL: "+ str(self.URL) + "\n"
        Result += "SourceElements: "+ str(self.QueryKeyElems) + "\n"
        Result += "Coverage: "+str(len(self.QueryKeyElems)/self.Num_of_QueryElems)+"\n"
        Result +="###########################\n"
        
        return Result
                
            


#Each Cluster Consists of a set of tables,
#There is a Score assigned to each Cluster
class Cluster:
    def __init__ (self, webTables):
        self.webTables = webTables
        self.score = 0
        self.UniqueSrcElems=[]
        
    #UniqueSrcElems=[]
    #Sets Cluster Score Based on Join_Score function in Octopus Paper: 
    #The degree to which a cluster covers data from the query column
    def setScore(self, QueryKeys):
        for wt in self.webTables:
            for elem in wt.QueryKeyElems:
                if(elem not in self.UniqueSrcElems):
                    self.UniqueSrcElems.append(elem)
        
        self.score = (len(self.UniqueSrcElems)/len(QueryKeys))*1.0
        
    def ToStr(self):
        Result = ""
        for wt in self.webTables:
            Result += wt.ToStr()
            Result +="************************************************\n"
        Result += "Cluster UniqueSrcElems: "+ str(self.UniqueSrcElems)+"\n"
        Result += "Cluster Score: "+ str(self.score)+"\n"
        Result += "-------------------------------------------\n"
            
        return Result
        
        

    #def setURL(self, URL):
        #self.URL = URL

#Extend Definition w.r.t. Scehma auto Completion:
#The Steps are:
#1- Multi Join
#2- Tables Extension
def Extend(QueryKeys):
    Textfile_Octopus_Extend = open(
            "C:/Saeed_Local_Files/Octopus/" + Dataset + "_Octopus_Extend.txt", "w", encoding="utf-8")
    Textfile_Octopus_HighestCluster = open(
            "C:/Saeed_Local_Files/Octopus/" + Dataset + "_Octopus_HighestCluster.txt", "w", encoding="utf-8")
    Cluster = MultiJoin(QueryKeys)
    Textfile_Octopus_HighestCluster.write("Cluster tables: "+str(len(Cluster.webTables))+"\n")
    Textfile_Octopus_HighestCluster.write(Cluster.ToStr())
    ExtendedTable = TableExtension_TableStyle(QueryKeys, Cluster)
    
    '''for col in ExtendedTable:
        Textfile_Octopus_Extend.write("Header: "+str(col[0])+" - Coverage:"+str(len(col[1])/len(QueryKeys))+"\n\n")
        Textfile_Octopus_Extend.write( str(col[1]) +"\n ---------------------------------------\n")'''
        
    Write_Table_To_CSV(ExtendedTable, "C:/Saeed_Local_Files/Octopus/" + Dataset + "_Octopus_Extend.csv")   
    
    Textfile_Octopus_Extend.close()
    Textfile_Octopus_HighestCluster.close()
    
#!!!!! Not ye tKey Containment!!!! Still Exact Match
#Extend the QueryKeys with all the Matched Rows in the cluster 
#in a set style: meaning that each key extension is seperate from other key extensions
def TableExtension_SeperateKeyStyle(QueryKeys, Cluster):
    ExtendedTable={}
    for webtable in Cluster.webTables:
            for i in range(len(webtable.table[0])):
                for j in range(len(webtable.table)):
                    cellVal=string_Cleanse(webtable.table[j][i])
                    if( Is_Value_Approximately_In_Set(cellVal,webtable.QueryKeyElems, 0.3)):
                        for k in range(len(webtable.table)):
                            if(k!=j):
                                Header="Null"
                                if(webtable.HeaderRowIdx!=-1):
                                    Header = webtable.table[k][webtable.HeaderRowIdx]
                                if(cellVal not in ExtendedTable):
                                    ExtendedTable[cellVal]=[(Header, webtable.table[k][i])]
                                else:
                                    ExtendedTable[cellVal].append((Header, webtable.table[k][i]))
    return ExtendedTable
                                    
       

#FuzzyGroup for Ocopus, Only returns the fuzzy groupings based on their frequency
def Octopus_FuzzyGroup(InputList, Value_Distance_Threhsold):
    #cleaning InputLis:
    InputList = [x for x in InputList if str(x).strip() != ""]
    FuzzyGroupedList=[]
    while (len(InputList)>0):
        NewGroupCentoid = (InputList[0])    
        NewGroupNames = [InputList[0]]
        NewGroupScore = 1
        del InputList[0]
        i=0
        while (i<len(InputList)):
            
            #Char_Level_Edit_Distance
            if (Edit_Distance_Ratio((InputList[i]),NewGroupCentoid)<Value_Distance_Threhsold):
                if (InputList[i] not in NewGroupNames ):
                    NewGroupNames.append(InputList[i])
                NewGroupScore += 1
                del InputList[i]
            else:
                i+=1
        
        Sorted_NewGroupNames = sorted(NewGroupNames, key=lambda item:(len(item)))
        FuzzyGroupedList.append((Sorted_NewGroupNames, NewGroupScore))
        
    
    
    FuzzyGroupedList = sorted(FuzzyGroupedList, key=lambda item:(item[1], 1/len(item[0][0]) if len(item[0][0])>0 else 0) , reverse=True)
        
    return FuzzyGroupedList

def Octopus_FuzzyGroup_Centroid_List(InputList, Value_Distance_Threhsold):
    ####
    #This Function returns only the list of centroids of the fuzzy groups
    #Here, centroid is the element of the group with the highest frequency
    ####
    
    fuzzy_groups = Octopus_FuzzyGroup(InputList, Value_Distance_Threhsold)
    centroids = []
    
    for grp in fuzzy_groups:
        data_list = grp[0]
        cnt = Counter(data_list)
        if(cnt.most_common(1)[0][0].strip()!=""):
            centroids.append(cnt.most_common(1)[0][0])
        
    return centroids
        

#Table Extension is peroformed based on coverage of tables and the output is
#a table and whenever a table does not contain a specified key, Null value will be augmented.
def TableExtension_TableStyle(QueryKeys, Cluster):
    Extended_Table=[]
    FirstRow=["Keys"]+QueryKeys
    Extended_Table.append(FirstRow)
    Extensions_Dicts={}
    for webtable in Cluster.webTables:
        #ExtensionDict_PerTable={}
        for i in range(len(webtable.table[0])):
                for j in range(len(webtable.table)):
                    cellVal=string_Cleanse(webtable.table[j][i])
                    Approximate_Match=Approximate_Match_In_Set(cellVal,webtable.QueryKeyElems, 0.3)
                    if(Approximate_Match!=""):
                        for k in range(len(webtable.table)):
                            if(k!=j):
                                Header="Null"
                                if(webtable.HeaderRowIdx!=-1):
                                    Header = webtable.table[k][webtable.HeaderRowIdx]
                                if(Header not in Extensions_Dicts):
                                    Extensions_Dicts[Header]=[(Approximate_Match, webtable.table[k][i])]
                                else:
                                    Extensions_Dicts[Header].append((Approximate_Match, webtable.table[k][i]))
        #Extensions_Dicts.append(ExtensionDict_PerTable)
            
    
    #Create_Columns
    Extensions_Cols=[]
    #for d in Extensions_Dicts:
    for header in Extensions_Dicts:
        col=[]
        col.append(header)
        col.append(Extensions_Dicts[header])
        Extensions_Cols.append(col)
            
            
    sortedCols = (sorted(Extensions_Cols,key=lambda col: len(col[1]), reverse=True))
    
    for col in sortedCols:
         #Add_Columns: 
        col_Header= col[0]
        New_Col=[col_Header]
        for key in QueryKeys:
            Extended_Values=[]
            
            #Add All Values for the Given Key to Extended_Values:
            for val in col[1]:
                if(val[0]== string_Cleanse(key)):
                    
                    Extended_Values.append(val[1])
            
            FuzzyGroup = Octopus_FuzzyGroup(Extended_Values)
            if(len(FuzzyGroup)>0):
                Predicted_Value= FuzzyGroup[0][0][0]
            else:
                Predicted_Value=""
            New_Col.append(Predicted_Value)
                
        Extended_Table.append(New_Col)
        
    
    
    
    return(Extended_Table)
    
    
#The Impl. of MultiJoin Algorithm in Cotopus paper:
#The steps are:
#1- Filter Tables
#2- Cluster Tables
#3- Score clusters based on their coverage with query keys
#4- returnThe cluster with the highest Score
def MultiJoin(QueryKeys):
    global GlobalString
    
    Textfile_Octopus_Log = open(
            "C:/Saeed_Local_Files/Octopus/" + Dataset + "_Octopus_Log.txt", "w", encoding="utf-8")
    Textfile_Octopus_Clusters = open(
            "C:/Saeed_Local_Files/Octopus/" + Dataset + "_Octopus_Clusters.txt", "w", encoding="utf-8")
    FilteredTables = []
    FilteredTables = FilterTables(QueryKeys)
    Textfile_Octopus_Log.write("Number of Filtered Tables: "+str(len(FilteredTables))+"\n")
    Clusters = []
    cntr123=0
    for webtable in FilteredTables:
        cntr123+=1
        #if(cntr123>10):
            #break
        Textfile_Octopus_Log = open(
            "C:/Saeed_Local_Files/Octopus/" + Dataset + "_Octopus_Log.txt", "a", encoding="utf-8")
        Textfile_Octopus_Clusters = open(
    "C:/Saeed_Local_Files/Octopus/" + Dataset + "_Octopus_Cluster.txt", "a", encoding="utf-8")
        GlobalString+=("Clustering Started For Central Table: \n")
        print("Clustering Started For Central Table: \n")
        #print(webtable.ToStr())
        GlobalString += webtable.ToStr()+"\n********************\n"
        cluster = []
        #if(len(webtable.table)<=4):
        cluster = Cluster(MakeCluster(webtable, FilteredTables, 0.3))
        if(len(cluster.webTables)>=1):
            cluster.setScore(QueryKeys)
            Textfile_Octopus_Clusters.write("cluster is:\n")
            Textfile_Octopus_Clusters.write(cluster.ToStr()+"\n")
        GlobalString+=("\nClustering Finished ...\n")
        Textfile_Octopus_Log.write(GlobalString)
        print("\nClustering Finished ...\n")
        GlobalString=""
        Textfile_Octopus_Log.close()
        Textfile_Octopus_Clusters.close()
        Clusters.append(cluster)
    print("Finished.")
    Textfile_Octopus_Clusters = open(
    "C:/Saeed_Local_Files/Octopus/" + Dataset + "_Octopus_Cluster.txt", "a", encoding="utf-8")
    Textfile_Octopus_Clusters.write("Finished")
    #sorted(Clusters, key=lambda cluster: cluster.score)
    #print(Clusters[0].ToStr())
    #Textfile_Octopus_Cluster.write(Clusters[-1].ToStr())
    HighestCluster = Cluster([])
    for cluster in Clusters:
        if(cluster.score>HighestCluster.score):
            HighestCluster=cluster
            
    print(HighestCluster.ToStr())
    Textfile_Octopus_Clusters.write(HighestCluster.ToStr())
            
    Textfile_Octopus_Log.close()
    Textfile_Octopus_Clusters.close()
    

    
    #SortedClusters = Sort(Clusters)
    return HighestCluster


#FilterTables Filter those webtables that contain at least one value from QueryKeys 
#Right Now There are two ways for matching the keys: one way is exact match [which is the current way]
#the other way is based on word containment (for camera dataset testing) [which is commented and referred to as Key Containment]
def FilterTables(QueryKeys):
    global FilteredTables
    FilteredTables=[]
    Cleansed_QueryKeys = []
    for val in QueryKeys:
        Cleansed_QueryKeys.append(string_Cleanse(val))
        #Code for word Containment [for testing camera keys]:
        #Cleansed_QueryKeys.append(val.lower())
    with open(get_Constant("Merged_Json_URL"), 'r') as f1:
        # with open('C:/Saeed_Local_Files/basketball_NBA_json_files/Merged_basketball_NBA.json', 'r') as f1:
        dict = json.load(f1)
        print(len(dict))
        for temp in dict:
            
                #An if Statement for limiting the Filtered Tables:
                #if(len(temp["relation"])<=4):
                    
                    #AddTableFlag=False
                    SourceElems=[]
                    for col in temp["relation"]:
                        for val in col:
                            cleansed_val = string_Cleanse(val)
                            Approximate_Match = Approximate_Match_In_Set(cleansed_val,Cleansed_QueryKeys, 0.3)
                            if(Approximate_Match !=""):
                                if(Approximate_Match not in SourceElems):
                                    SourceElems.append(Approximate_Match)
                            
                            #Key Containment Code [for testing camera keys]
                            #cleansed_val = (val.lower())
                            #for key in Cleansed_QueryKeys:
                                #if key in cleansed_val:
                                     #if(key not in SourceElems):
                                         #SourceElems.append(key)
                            
                                

                    if(len(SourceElems)>=1):
                        webtable = WebTable(temp["relation"])
                        webtable.URL= temp["url"]
                        webtable.QueryKeyElems = SourceElems.copy()
                        webtable.Num_of_QueryElems = len(QueryKeys)
                        if(temp["hasHeader"]):
                            webtable.HeaderRowIdx = temp["headerRowIndex"]
                        FilteredTables.append(webtable)
                        
    
   
    Textfile_Octopus_FilteredTables = open(
            "C:/Saeed_Local_Files/Octopus/" + Dataset + "_Octopus_FilteredTables.txt", "w", encoding="utf-8")
    
    sortedFilteredTables=[]
    sortedFilteredTables = sorted(FilteredTables,key=lambda table: len(table.QueryKeyElems))
    sortedFilteredTables = sortedFilteredTables[-100:]
    sortedFilteredTables = sortedFilteredTables
    for table in sortedFilteredTables:
        Textfile_Octopus_FilteredTables.write(table.ToStr())
    
    Textfile_Octopus_FilteredTables.close()
    
    print ("Number of Filtered Tables: "+ str(len(sortedFilteredTables)))
    return sortedFilteredTables


#MakeCluster returns a set of tables which are categorized in the same group as the input table
#Based on a Distance function: Those Tables that have a distance lower than a thresholf with the input table
#are categorized in the same cluster
def MakeCluster(inTable, TableCorpus, Threshold):
    global GlobalString
    #print("Make Cluster Start! ... Central Table is")
    #print(inTable.ToStr())
    ClusteredTables = []
    ClusteredTables.append(inTable)
    numOfCols1 = len(inTable.table)
    for t in TableCorpus:
        print("Next_Table...")
        GlobalString+=t.ToStr()+"\n***\n"
        d = ColumnText_Distance(inTable.table, t.table)
        
        
        numOfCols2 = len(t.table)
        minCols = min(numOfCols1, numOfCols2)
        diff_ratio = d/minCols
        
        if (diff_ratio < Threshold):
            GlobalString+=("Appended!!! With d: "+str(d))
            ClusteredTables.append(t)
    return ClusteredTables

#ColumnText_Distance is based on the ColumnTextCluster Feature in Octopus paper,
#The Table to Table Distance is calculated basesd on the best Column to Column Matching between the columns of tho tables
#Which would be the sum of pairwise matching distance scores
#The pairwise Distance Score is calculated based on the TF-IDF Cosine Distance of the values of the two columnns
#The Best Possible Pairwise Matching Makes the Table to Tabel Score Minimized
def ColumnText_Distance(Table1, Table2):
    global GlobalString

    SmallerTable=[]
    BiggerTable = []
    if(len(Table1)>len(Table2)):
        BiggerTable = Table1
        SmallerTable = Table2
    else:
        BiggerTable = Table2
        SmallerTable = Table1
        
    #DistanceMatrix is the Distance between each pairwise Matching of Smaller Table Columns and Bigger Table Columns:
    DistanceMatrix = []
    for i in range(len(SmallerTable)):
        MatrixRow=[]
        for j in range(len(BiggerTable)):
            MatrixRow.append(TF_IDF_DISTANCE(SmallerTable[i],BiggerTable[j]))
        DistanceMatrix.append(MatrixRow) 
    GlobalString+=("DistanceMatrix: "+str(DistanceMatrix)+"\n")
    
    
    #Finding  minimum weight matching in bipartite graphs
    #cost is the Adjacency matrix for the bipartite graph
    
    cost = np.array(DistanceMatrix)
    row_ind, col_ind = linear_sum_assignment(cost)
    MinDistScore = cost[row_ind, col_ind].sum()
    
    
    #print("DistanceMatrix Done!")
    
    #print ("SmallerTable: "+ str(SmallerTable))
    #print ("BiggerTable: " +  str(BiggerTable))
    #print ("DistanceMatrix: "+ str(DistanceMatrix))
    #print ("derayeye [1][0]: "+str(DistanceMatrix[1][0]))
    
    
    #Finding the Best Possible Matching Through BruteForce using all Permuations of Pairwise Matchings
    #The Best Possible Pairwise Matching Makes the Table to Tabel Score Minimized
    '''MinDistScore = sys.maxsize
    
    for i in permutations(range(len(BiggerTable)), len(SmallerTable)):
        Matching=[]
        DistanceScore=0
        for x in range(len(SmallerTable)):
            DistanceScore += (DistanceMatrix[x][i[x]])
            Matching.append((i[x],DistanceMatrix[x][i[x]]))
        if(DistanceScore<MinDistScore):
            BestMatch=[]
            BestMatch= Matching.copy()
            MinDistScore = DistanceScore'''
    #print(MinDistScore)
    

    
    GlobalString+=("MinDistScore: "+str(MinDistScore)+"\n")
    GlobalString+=("BestMatching: "+str(col_ind)+"\n##################\n")
    return MinDistScore
            
        
        
    

    
def TF_IDF_DISTANCE(Col1, Col2):
    tfidf_vectorizer = TfidfVectorizer(token_pattern='\\b\\w+\\b')
    Documents = []
    Col2Doc1=""

    for value in Col1:
        Col2Doc1 +=" "+ str(value)
    Col2Doc2=""
    for value in Col2:
        Col2Doc2 += " "+ str(value)
        
    
    if(string_Cleanse(Col2Doc1) == ""):
        Col2Doc1="Null"
    
    if(string_Cleanse(Col2Doc2) == ""):
        Col2Doc2="Null"
    
    Documents.append(Col2Doc1)
    Documents.append(Col2Doc2)
    #print(Documents)
    tfidf = tfidf_vectorizer.fit_transform(Documents)

    
    pairwise_sim_sparse = tfidf *  tfidf.T
    pairwise_similarity = pairwise_sim_sparse.toarray()
    
    return 1-pairwise_similarity[0][1]
    
    
def test_TF_IDF_Vectorizor():
    tfidf_vectorizer = TfidfVectorizer()
    tfidf = tfidf_vectorizer.fit_transform(["An apple one day keeps the doctor away",
                           "An apple a day keeps the doctor away"])

    
    pairwise_sim_sparse = tfidf *  tfidf.T
    pairwise_similarity = pairwise_sim_sparse.toarray()
    
    print(pairwise_similarity[0][1])

    print(pairwise_similarity.shape)

    for x in range(pairwise_similarity.shape[0]-1):
        for y in range(x + 1, pairwise_similarity.shape[0]):
            print(pairwise_similarity[x][y])
    
    

#This Function is the Modified Table Scoring Method
#Used for JoinTest in Octopus paper
#Change: Topic "k" is removed from the input
def JoinTest_Table_Search(Input_Keys,Threshold, Matching_Type):
    
    # Cleansing the Key List + Unique
    Cleansed_Input_Set = set()
    for key in Input_Keys:
        Cleansed_Input_Set.add(string_Cleanse(key))
    
    #print(Input_Keys)
    #print(len(Cleansed_Input_Set))
    
    Table_Score_Dict={}
    
    #Table Scoring Procedures :
    #Score of each Table = Max of Jacard Sim of any column (w.r.t. the input keys)       
    with open(get_Constant("Deduplicated_Merged_Json_URL"), 'r') as f1:
        dict = json.load(f1)
        for obj in dict:
            table = obj["relation"]
            
            if(Matching_Type=="Approximate_Match"):
                EditDistanceThreshold=0.3
            
            #Removing the Header row
            if (obj["hasHeader"]):
                for col in table:
                    col.pop(obj["headerRowIndex"])
                    
            #Calculating Jaccard Score for Each Column 
            Max_Jaccard_Score=0
            for col in table:
                Cleansed_Column_Set=set()
                for val in col:
                    Cleansed_Column_Set.add(string_Cleanse(val))
                    
                if(Matching_Type=="Exact_Match"):
                    
                    Intersect = Cleansed_Input_Set.intersection(Cleansed_Column_Set)
                    JaccardScore=len(Intersect)/(len(Cleansed_Input_Set)+len(Cleansed_Column_Set)-len(Intersect))
                    
                if(Matching_Type=="Approximate_Match"):
                    Intersect_Cnt = 0
                    for col_val in Cleansed_Column_Set:
                        for input_val in Cleansed_Input_Set:
                            if(Edit_Distance_Ratio(col_val,input_val)<EditDistanceThreshold):
                                Intersect_Cnt+=1
                                break
                    JaccardScore=Intersect_Cnt/(len(Cleansed_Input_Set)+len(Cleansed_Column_Set)-Intersect_Cnt)
                    
                if(JaccardScore>Max_Jaccard_Score):
                    Max_Jaccard_Score = JaccardScore
                    
                    
            if(Max_Jaccard_Score>Threshold):
                Table_Score_Dict[obj["ID"]] = Max_Jaccard_Score
            
        return Table_Score_Dict
                    
                
                    
                
                

        
            
        
    
                
    
    
    
if  (__name__ == "__main__"):
    '''QueryKeys=["kobe bryant","Shaquille O'Neal"]
    #FilterTables(QueryKeys)
    #test_TF_IDF_Vectorizor()
    Doc1 = [["An", "apple", "one"],[ "day", "keeps"],[ "the", "doctor", "away"]]
    Doc2 = [["An", "apple", "a", "day", "keeps"],[ "the", "doctor", "away"]]
    Doc3 = [["Something", "Not", "Related"]]
    Table1=[]
    Table1.append(Doc1)
    Table1.append(Doc2)
    Table1.append(Doc3)
    Table2 = []
    Table2.append(Doc1)
    Table2.append(Doc2)'''
    #ColumnText_Distance(Table1, Table2)
    #print(TF_IDF_DISTANCE(Doc1, Doc2))

    #for i in permutations([1,2,3], 1):
        #print(i[0])
    #print(i)
    #MultiJoin(basketball_Keys)
    #US_States
    #basketball_Keys
    #camera_Keys
    
    
    Extend(Populate_QueryKeys_From_ExcelFile("C:/Saeed_Local_Files/Logs/Mapped_to_DBPedia/Queries/Games_Queries.xlsx"))
    

    
    
    
    #print(sorted(Table1,key=lambda table: len(table)))
    
    #token_pattern='\\b\\w+\\b'
    #tfidf_vectorizer = TfidfVectorizer(stop_words=None)
    #Documents = []
    #Documents = ['F G C G G G G F F F G G G', 'F G C G G G G F F F G G G']
    #tfidf = tfidf_vectorizer.fit_transform(Documents)