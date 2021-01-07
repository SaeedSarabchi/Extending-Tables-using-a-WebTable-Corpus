'''
    This file is used for the functionalities of attribute correlation statistics database (AcsDB) 
    ACSDB is used in Column Ranking step in our proposed solution.
'''

from CommonUtilities import *
import math


#!!!!! Read P.S2 !!!!!
def CreateACSDB():
    ####
        #This Function creates the attribute correlation statistics database (AcsDB) 
        #that records corpus-wide statistics on cooccurrences of schema elements
        #In this implementation, ACSDB is a posting list, consisting of Attribute Names as the key of the posting list, and the list of schema IDs which that Attribute_Name is used
        #P.S1: Here, Schema ID is the same as TableID
        #P.S2: For each domain, duplicate schemas will NOT be ignored, Which Should!!!!!
    ####
    
    print("ACSDB Started ... ")
    Header_ACSDB = {}
    Content_ACSDB = {}
    
    with open(get_Constant("Deduplicated_Merged_Json_URL"), 'r') as f1:
        dict = json.load(f1)
        
        SeenDomains={}
        
        for obj in dict:
            
            #Creating Header_ACSDB:
            if(obj["hasHeader"]):
                HeaderRowIndex = obj["headerRowIndex"]
                for col in obj["relation"]:
                    Attribute = string_Cleanse(col[HeaderRowIndex])
                    if (Attribute not in Header_ACSDB):
                        Header_ACSDB[Attribute] = [obj["ID"]]
                    else:
                        if(obj["ID"] not in Header_ACSDB[Attribute]):
                            Header_ACSDB[Attribute].append(obj["ID"])
                            
            #Creating Content_ACSDB: Indexing only those cellData which co-occure in just one row, not in other rows.
            table = obj["relation"]
            for i in range(len(table[0])):
                for j in range(len(table)):
                    cellData = string_Cleanse(table[j][i])
                    if (cellData not in Content_ACSDB):
                        Content_ACSDB[cellData] = [str(obj["ID"])+"-"+str(i)]
                        #Content_ACSDB[cellData] = [str(obj["ID"])]
                    else:
                        if((str(obj["ID"])) not in Content_ACSDB[cellData]):
                            Content_ACSDB[cellData].append(str(obj["ID"])+"-"+str(i))
                            #Content_ACSDB[cellData].append(str(obj["ID"]))
            

            
                    


                        
                        
    with open(get_Constant("Header_ACSDB_Pickle_URL"), 'wb') as outfile1:
        pickle.dump(Header_ACSDB, outfile1)
        
    with open(get_Constant("Content_ACSDB_Pickle_URL"), 'wb') as outfile2:
        pickle.dump(Content_ACSDB, outfile2)
        
        
    #Write_Only_Dict_To_File_Generic(ACSDB, "ACSDB", 'C:/Saeed_Local_Files/RelatedTables')

    print("ACSDB Finished ")
    
    
def Fetch_Statistics(value, Statistics):
    #with open(get_Constant("Content_ACSDB_Pickle_URL"), 'rb') as f3:
        #dict = pickle.load(f3)
        #for val in dict:
            #print (val)
        return Statistics[value]


def Header_Or_Row_Consistency(Atr1,Atr2, ACSDB, Match_Type="Approximate_Match"):
    Atr1 = string_Cleanse(Atr1)
    Atr2 = string_Cleanse(Atr2)
        
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
        #print(len(ACSDB))
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
    Union = A1_Set.union(A2_Set)
    if(len(A1_Set)!=0):
        Consistency = len(Intersect)/len(A1_Set)
    else:
        Consistency=0
    
    return Consistency


    #return len(Statistics[InputVal])/CorpusSize


def Value_Probability(Input, Statistics, CorpusSize):
    '''
    # this function computes |T(InputVal)|/N, wihch is the number of tables that InputVal has occured 
    # over the number of tables in the whole corpus
    # If the InputVal is a tuple, then |T(inputVal[0]) Intersect T(inputVal[1])| would be calculated 
    # over the number of tables in the whole corpus
    '''
    try:
        if (type(Input)==type("c")):
            # Since T is the number of tables, but Statistics has the number of rows, we have to calculate the number of tables
            set_of_rows = Statistics[Input]
            set_of_tables = set()
            for elem in set_of_rows:
                set_of_tables.add(elem.split("-")[0])
            return len(set_of_tables)/CorpusSize
        elif (type(Input)==type(("a","b"))):
            set_1 = set(Statistics[Input[0]])
            set_2 = set(Statistics[Input[1]])
            intersect = set_1.intersection(set_2)
            
            # Since T is the number of tables, but Statistics has the number of rows, we have to calculate the number of tables
            set_of_rows = intersect
            set_of_tables = set()
            for elem in set_of_rows:
                set_of_tables.add(elem.split("-")[0])
                
            return len(set_of_tables)/CorpusSize
            
        raise "Something went Wrong!"
    
    except: 
        return 0
        

def Row_PMI(Tuple, Statistics, CorpusSize):
    log_numerator = Value_Probability(Tuple, Statistics, CorpusSize)
    log_denomerator = Value_Probability(Tuple[0], Statistics, CorpusSize)*Value_Probability(Tuple[1], Statistics, CorpusSize)
    if(log_numerator==0 or log_denomerator==0):
        return 0
    else:
        return math.log(log_numerator/log_denomerator)

def Normalized_Row_PMI(Tuple, Statistics, CorpusSize):
    row_pmi = Row_PMI(Tuple, Statistics, CorpusSize)
    if(row_pmi ==0):
        return 0
    return row_pmi/(-1*math.log(Value_Probability(Tuple, Statistics, CorpusSize)))
    
        
def tests():
    with open(get_Constant("Content_ACSDB_Pickle_URL"), 'rb') as f3:
        statistics = pickle.load(f3)
        set_cnt = set()
        for k in statistics:
            for elem in statistics[k]:
                #print(elem)
                #print(elem.split("-")[0])
                set_cnt.add(elem.split("-")[0])
        corpus_size = len(set_cnt)
        print(corpus_size )
        #print(len(Fetch_Statistics("1",statistics)))
        #with open(get_Constant("Deduplicated_Merged_Pickle_URL"), 'rb') as f4:
        #   WT_Corpus = pickle.load(f4)
        Tuple = (string_Cleanse("Country"),string_Cleanse("Company"))
        print(Row_PMI(Tuple,statistics,corpus_size))
        print(Normalized_Row_PMI(Tuple,statistics,corpus_size))
            #inputVal = ("1","dinakaran")
            #print(Normalized_Row_PMI(inputVal,statistics, len(WT_Corpus)))
        
    
if  (__name__ == "__main__"):
    
    #CreateACSDB()
    tests()
    #setA=set([1,2])
    #setA=set(Fetch_Content_ACSDB("canada"))
    #setB=set(Fetch_Content_ACSDB("canada"))
    #print(setA.intersection(setB))