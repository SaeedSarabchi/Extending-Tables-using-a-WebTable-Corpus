'''
This file is used for analysing the table corpus (T2D Gold Standard) from Web Data Commons
'''

import json
import pickle
import copy
from CommonUtilities import *
from Dataset_Gathering import *


json_obj1 = []
json_obj2 = []

dict = {}

#Generatign ID in each WebTable
def ID_Gen():
    ID = 0
    with open('../sample.json', 'r') as f1:
        # dict=json.load(f1)
        for iter in f1:
            temp = json.loads(iter)
            if ((temp["hasKeyColumn"]) and (temp["tableOrientation"]=="HORIZONTAL")):
                temp['ID'] = ID
                # print(temp)
                json_obj1.append(temp)
                ID = ID + 1

        print(len(json_obj1))

    with open('SamplePlusID.json', 'w') as outfile:
        json.dump(json_obj1, outfile)

    with open('SamplePlusID.json', 'r') as f3:
        dict = json.load(f3)
        for obj in dict:
            print(str(obj["ID"]) + ", " + str(obj["hasKeyColumn"]))

            
    
#Generating EAB Tables from n-ary tables
def EAB_Gen():
    print("EAB_Gen Started")
    #ID = 0
    EAB = []
    EABrelation = []
    with open(get_Constant("Deduplicated_Merged_Json_URL"), 'r') as f1:
        dict = json.load(f1)
        print(len(dict))
        for temp in dict:
            
                KeyIndex = temp["keyColumnIndex"]
                
                
                temp["keyColumnIndex"] = 0
                table = temp["relation"]

                

                if(KeyIndex==-1):
                    print("Table with key Index -1")
                TableID = temp["ID"]
                # Transforming n-ary table to EABs
                for x in range(0, len(table)):
                    #try:

                        if (x != KeyIndex):
                            EAB = (temp.copy())
                            
                            EAB["ID"] = str(TableID) + "-" + str(x)
                            # Assigning the Key Column to Relation
                            EABrelation.append(table[KeyIndex])
                            EABrelation.append(table[x])
        
                            # print("Start")
                            # print(EABrelation)
                            # print("End")
                            EAB["relation"] = EABrelation.copy()
                            
                            if("units" in EAB):
                                EAB_units=[]
                                EAB_units.append(temp["units"][KeyIndex])
                                EAB_units.append(temp["units"][x])
                                EAB["units"] = EAB_units.copy()
                                
                            json_obj1.append(EAB.copy())
                            # EAB.clear()
                            EABrelation.pop()
                            EABrelation.pop()
                        
                    #except:
                            #print("Error Occured")
                            #print(temp["units"])
                            #print(x)
                            #print(temp["ID"])
                            
                            #print(table)
                            #return
                            

            # print(temp)

                #ID = ID + 1

    print(len(json_obj1))

    with open(get_Constant("EAB_Merged_Json_URL"), 'w') as outfile:
        json.dump(json_obj1, outfile)

    print("##################")
    print("PRINTING IDS")
    print("##################")

    with open(get_Constant("EAB_Merged_Json_URL"), 'r') as f3:
        dict = json.load(f3)
        for obj in dict:
            # print(str(obj["ID"])+ str(obj["relation"]) + ", " + str(obj["hasKeyColumn"])+ ", " +  str(obj["keyColumnIndex"]))
            print(str(obj["ID"]))
            
    #Creating the Pickle format as well:
    JsonToPickle()        
    
    print("EAB_Gen Finished")
            
    
    
    



        
        
#Creating the Index for key attribute values So That :
# Given a query table Q, WIK(Q) returns
#the set of web tables that overlaps with Q on at least one of the keys.
def Create_WIK():
    print("Create_WIK Started ")
    WIK_Index = {}
    PostingList = []
    with open(get_Constant("EAB_Merged_Json_URL"), 'r') as f3:
        dict = json.load(f3)
        print("size of EAB_Merged_Json_URL : "+str(len(dict)))
        for obj in dict:
            KeyIndex = obj["keyColumnIndex"]
            #print("ID:" + str(obj["ID"]) + ", KeyIndex:" + str(KeyIndex))
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
    with open(get_Constant("WIK_Pickle_URL"), 'wb') as outfile:
        pickle.dump(WIK_Index, outfile)

    print("Create_WIK Finished ")
    
    


def Create_ColumnPostingList_Pickle_URL():
    '''
        #This Function Creates a posting list for each value used in each column (NOT EABs, Here we are dealing with each column of the table)
    '''
    print("ColumnPostingList_Pickle_URL Started ")
    PostingList = {}
    with open(get_Constant("Deduplicated_Merged_Json_URL"), 'r') as f3:
        dict = json.load(f3)
        print("size of Deduplicated_Merged_Pickle_URL : "+str(len(dict)))
        for obj in dict:
            
            Col_Idx=-1
            
            #Indexing each of the Table's Column:
            for col in obj["relation"]:
                
                Col_Idx+=1
                ColID = str(obj["ID"])+"-"+str(Col_Idx) #This is The ID which will be stored in the PostingList
                
                #Getting rid of the Header
                if (obj["hasHeader"]):
                    col.pop(obj["headerRowIndex"])
                
                for val in col:
                    Cleansed_val = string_Cleanse(val)
                    if Cleansed_val=="":
                        continue
                    if (Cleansed_val not in PostingList):
                        PostingList[Cleansed_val] = [ColID]
                    else:
                        if(ColID not in PostingList[Cleansed_val]):
                            PostingList[Cleansed_val].append(ColID)
                            
    with open(get_Constant("ColumnPostingList_Pickle_URL"), 'wb') as outfile:
        pickle.dump(PostingList, outfile)

    print("ColumnPostingList_Pickle_URL Finished ")
    
def Create_ColumnPostingList_Pickle_URL_Mannheim():
    '''
        #This Function Creates a posting list for each value used in each column (NOT EABs, Here we are dealing with each column of the table)
    '''
    print("ColumnPostingList_Pickle_URL Started ")
    PostingList = {}
    with open(get_Constant("Deduplicated_Merged_Json_URL"), 'r') as f3:
        dict = json.load(f3)
        print("size of Deduplicated_Merged_Pickle_URL : "+str(len(dict)))
        for obj in dict:
            
            Col_Idx=-1
            
            #Indexing each of the Table's Column:
            for col in obj["relation"]:
                
                Col_Idx+=1
                if(Col_Idx) == obj["keyColumnIndex"]:
                    ColID = str(obj["ID"])+"-"+str(Col_Idx) #This is The ID which will be stored in the PostingList
                    
                    #Getting rid of the Header
                    if (obj["hasHeader"]):
                        col.pop(obj["headerRowIndex"])
                    
                    for val in col:
                        Cleansed_val = string_Cleanse(val)
                        if (Cleansed_val not in PostingList):
                            PostingList[Cleansed_val] = [ColID]
                        else:
                            if(ColID not in PostingList[Cleansed_val]):
                                PostingList[Cleansed_val].append(ColID)
                            
    with open(get_Constant("ColumnPostingList_Pickle_URL"), 'wb') as outfile:
        pickle.dump(PostingList, outfile)

    print("ColumnPostingList_Pickle_URL Finished ")


def Fetch_WIK_ByKey(key):
    with open('C:/Saeed_Local_Files/camera_json_files/Camera_WIK_Index.pickle', 'rb') as f3:
        dict = pickle.load(f3)
        print(dict[key])


#Generating (WIKV) Index: An index for the web tables complete records (that is key and value combined).
# WIKV(Q) returns the set of web tables
#that contain at least one record from Q.
#An index for the web tables complete records (that is key and
#value combined) (WIKV). WIKV(Q) returns the set of web tables
#that contain at least one record from Q.
def Create_WIKV():
    print("Create_WIKV Started ")
    WIKV_Index={}
    PostingList=[]
    with open('C:/Saeed_Local_Files/camera_json_files/EAB_Merged_camera.json', 'r') as f3:
        dict = json.load(f3)
        table=[]
        for obj in dict:
            KeyIndex=obj["keyColumnIndex"]
            #print("ID:"+str(obj["ID"])+", KeyIndex:"+str(KeyIndex))
            table=obj["relation"]

            #Delete The Header Row
            if(obj["hasHeader"]):
                for x in range(0, len(table)):
                    table[x].pop(obj["headerRowIndex"])

            Keys = table[KeyIndex]

            #Delete The Key Column
            table.pop(KeyIndex)

            #Inserting into WIKV_Index
            for keyIndex in range(0, len(Keys)):
                key = Keys[keyIndex]
                for ColIndex in range(0, len(table)):
                    value=table[ColIndex][keyIndex]
                    if((key,value) not in WIKV_Index):
                        WIKV_Index[(key,value)]=[obj["ID"]]
                    else:
                        WIKV_Index[(key,value)].append(obj["ID"])
    with open('C:/Saeed_Local_Files/camera_json_files/Camera_WIKV_Index.pickle', 'wb') as outfile:
        pickle.dump(WIKV_Index, outfile)
        
    print("Create_WIKV Finished ")

def Fetch_WIKV_ByKeyValue(KeyValue):
    with open('WIKV_Index.pickle', 'rb') as f3:
        dict = pickle.load(f3)
        print(dict[KeyValue])


# (WIA): An index on the web
#tables attributes names (WIA), such that, WIA(Q) returns the set
#of web tables {T|T.B ≈ Q.A}
def Create_WIA():
    print("Create_WIA Started ")
    WIA_Index={}
    PostingList=[]
    with open(get_Constant("EAB_Merged_Json_URL"), 'r') as f3:
        dict = json.load(f3)
        for obj in dict:
            if(obj["hasHeader"]):
                HeaderIndex = obj["headerRowIndex"]
                Headers=[]
                Headers.append(obj["relation"][0][HeaderIndex])
                Headers.append(obj["relation"][1][HeaderIndex])
                for header in Headers:
                    header = string_Cleanse(header)
                    if(header not in WIA_Index):
                        WIA_Index[header]=[obj["ID"]]
                    else:
                        if(obj["ID"] not in WIA_Index[header]):
                            WIA_Index[header].append(obj["ID"])
    with open(get_Constant("WIA_Pickle_URL"), 'wb') as outfile:
        pickle.dump(WIA_Index, outfile)
    print("Create_WIA Finished ")



def Fetch_WIK_ByHeader(Header):
    with open('C:/Saeed_Local_Files/camera_json_files/Camera_WIA_Index.pickle', 'rb') as f3:
        dict = pickle.load(f3)
        print(dict[Header])

def Create_T2Syn_DMA(Threshold):
    print("Create_T2Syn_DMA Started ...")
    with open(get_Constant("EAB_Merged_Pickle_URL"), 'rb') as f3:
        dict = pickle.load(f3)
        T2Syn_DMA={}
        with open(get_Constant("SMW_Graph_Pickle"), 'rb') as featureFile:
            # pickle.load(featureFile)
            while 1:
                try:
                    PartialGraph = (pickle.load(featureFile))

                    #Populating T2Syn_DMA Index : (tableKey)=[(Attribute_Name,Score)]
                    #
                    for item in PartialGraph:
                        if(PartialGraph[item]>=Threshold):

                            #item[0], item[1] are the Nodes in each edge (Node=Table)
                            # each node is matched with the other node
                            if ((dict[item[1]]["hasHeader"]) and (dict[item[1]]["relation"][1][dict[item[1]]["headerRowIndex"]].strip()!="")): #If our Current Table is the first Node=item[0]

                                Matchedtable=dict[item[1]]
                                headerRowIndex= Matchedtable["headerRowIndex"]
                                valuePair= (Matchedtable["relation"][1][headerRowIndex],PartialGraph[item], item[1])
                                if(item[0] not in T2Syn_DMA ):
                                    T2Syn_DMA[item[0]]=[valuePair]
                                else:
                                    T2Syn_DMA[item[0]].append(valuePair)

                            if (dict[item[0]]["hasHeader"] and (dict[item[0]]["relation"][1][dict[item[0]]["headerRowIndex"]].strip()!="")): #If our Current Table is the Second Node=item[1]
                                Matchedtable = dict[item[0]]
                                headerRowIndex = Matchedtable["headerRowIndex"]
                                valuePair = (Matchedtable["relation"][1][headerRowIndex],PartialGraph[item], item[0])
                                if (item[1] not in T2Syn_DMA):
                                    T2Syn_DMA[item[1]] = [valuePair]
                                else:
                                    T2Syn_DMA[item[1]].append(valuePair)


                    Features = {}
                except EOFError:
                    break

        with open(get_Constant("T2Syn_DMA_Pickle"), 'wb') as outfile:
            pickle.dump(T2Syn_DMA, outfile)
            
    print("Create_T2Syn_DMA Finished ...")


#Groups Those Tables with Exact Attribtue Names
def ExactGroupForT2Syn(InputList, EAB_Dict):
    
    dict = EAB_Dict
    FuzzyGroupedList=[]
    while (len(InputList)>0):
        
        SynHeader="Null"
        if (dict[InputList[0][0]]["hasHeader"]):
                SynHeader = dict[InputList[0][0]]["relation"][1][(dict[InputList[0][0]]["headerRowIndex"])]
        #Syn_List.append((sorted_x[i][0],SynHeader,sorted_x[i][1]))
                        
        
        
        NewGroupCentoid_Not_Cleansed = SynHeader
        NewGroupCentoid = string_Cleanse(SynHeader)
        NewGroupNames = [SynHeader]
        NewGroupScore = InputList[0][1]
        NewGroupTables = [InputList[0][0]]
        del InputList[0]
        i=0
        while (i<len(InputList)):
            SynHeader="Null"
            if (dict[InputList[i][0]]["hasHeader"]):
                    SynHeader = dict[InputList[i][0]]["relation"][1][(dict[InputList[i][0]]["headerRowIndex"])]
            if ((string_Cleanse(SynHeader)==(NewGroupCentoid))):
                if (SynHeader not in NewGroupNames ):
                    NewGroupNames.append(SynHeader)
                NewGroupScore += InputList[i][1]
                if (InputList[i][0] not in NewGroupTables ):
                    NewGroupTables.append(InputList[i][0])
                del InputList[i]
            else:
                i+=1
        
        FuzzyGroupedList.append((NewGroupTables, NewGroupCentoid_Not_Cleansed, NewGroupScore))
        
    

        FuzzyGroupedList = sorted(FuzzyGroupedList, key=lambda item:item[2], reverse=True)
        
    return FuzzyGroupedList

#!!FuzzyGroup id added to T2Syn
#Plus Cleaning the T2Syn Index from those EABs of the same orginal Tables.
'''def ExactGroup_Create_T2Syn_From_SMW(TopN, Threshold):
     
    with open(get_Constant("EAB_Merged_Pickle_URL"), 'rb') as f3:
        dict = pickle.load(f3)
        T2Syn_From_SMW={}
        with open(get_Constant("FPPR"), 'rb') as FPPRFile:
            FPPR_Dict = pickle.load(FPPRFile)
            
            cntr=0
            for node in FPPR_Dict:
                cntr+=1
                if(cntr>25):
                    break
                print(cntr)
                SourceHeader="Null"
                if (dict[node]["hasHeader"]):
                    SourceHeader = dict[node]["relation"][1][(dict[node]["headerRowIndex"])]
                    
                    
                dict_to_list=[]
                tempDict = FPPR_Dict[node]
                for elem in tempDict:
                    #print(tempDict[elem])
                    
                    dict_to_list.append((elem,tempDict[elem]))
                    
                #dict_to_list = sorted(FPPR_Dict[node].items(), key=lambda item:item[1], reverse=True)
                
                #FuzzyClustered_FPPR_Results:    
                sorted_x = ExactGroupForT2Syn(dict_to_list, dict)    
                
                
                Syn_List=[]
                if(node=="10630177_0_4831842476649004753-6"):
                    a=1
                    
                Repeat=TopN
                UsedTables=set()
                i=-1
                while (i <(min(Repeat, len(sorted_x)))):
                    i+=1
                    if(sorted_x[i][2]> Threshold):
                        #Check If there is any Table in the Synonym tables which is used in previous UsedTables:
                        UsedFlag = False
                        for table in sorted_x[i][0]:
                            if table.split("-")[0] in UsedTables:
                                Repeat+=1
                                UsedFlag=True
                                break
                        if (UsedFlag == False):
                            #Insert the tables used in the synonym to the UsedTables:
                            for table in sorted_x[i][0]:
                                UsedTables.add(table.split("-")[0])
                            #Add it to the Synonym List:    
                            Syn_List.append((sorted_x[i][0],sorted_x[i][1],sorted_x[i][2]))
                        
                    #T2Syn_From_SMW[(node,SourceHeader)]=Syn_List
                T2Syn_From_SMW[node]=Syn_List
                    
                #SynSets = FuzzyCluster(SynSets)
                    
        with open(get_Constant("T2Syn_From_SMW"), 'wb') as outfile:
            pickle.dump(T2Syn_From_SMW, outfile)
            

'''



def Create_T2Syn_From_SMW(TopN, Threshold):
     
    with open(get_Constant("EAB_Merged_Pickle_URL"), 'rb') as f3:
        dict = pickle.load(f3)
        T2Syn_From_SMW={}
        with open(get_Constant("FPPR"), 'rb') as FPPRFile:
            FPPR_Dict = pickle.load(FPPRFile)
            
            for node in FPPR_Dict:

                SourceHeader="Null"
                if (dict[node]["hasHeader"]):
                    SourceHeader = dict[node]["relation"][1][(dict[node]["headerRowIndex"])]
                    
                    
                sorted_x = sorted(FPPR_Dict[node].items(), key=lambda item:item[1], reverse=True)
                  

                Syn_List=[]
                if(node=="10630177_0_4831842476649004753-6"):
                    a=1
                for i in range(min(TopN, len(sorted_x))):
                    if(sorted_x[i][1]> Threshold):
                        SynHeader="Null"
                        if (dict[sorted_x[i][0]]["hasHeader"]):
                                SynHeader = dict[sorted_x[i][0]]["relation"][1][(dict[sorted_x[i][0]]["headerRowIndex"])]
                        Syn_List.append((sorted_x[i][0],SynHeader,sorted_x[i][1]))
                    #T2Syn_From_SMW[(node,SourceHeader)]=Syn_List
                T2Syn_From_SMW[node]=Syn_List
                    
                #SynSets = FuzzyCluster(SynSets)
                    
        with open(get_Constant("T2Syn_From_SMW"), 'wb') as outfile:
            pickle.dump(T2Syn_From_SMW, outfile)

#In order to Clean those synonyms from the same original table:
def Clean_T2Syn_From_SMW():
    with open(get_Constant("T2Syn_From_SMW"), 'rb') as f3:
        T2Syn_From_SMW = pickle.load(f3)
        
        New_T2Syn_From_SMW ={}
        for node in T2Syn_From_SMW:
            Syn_List=[]
            Orig_Tables=[]
            for triple in T2Syn_From_SMW[node]:
                triple_orig_table = triple[0].split("-")[0]
                if(triple_orig_table not in Orig_Tables):
                    Syn_List.append(triple)
                    Orig_Tables.append(triple_orig_table)
                    
            New_T2Syn_From_SMW[node] = Syn_List
            
        with open(get_Constant("T2Syn_From_SMW"), 'wb') as outfile:
            pickle.dump(New_T2Syn_From_SMW, outfile)
            
            
    
    
def Write_T2Syn_From_SMW_To_File():
    with open(get_Constant("T2Syn_From_SMW"), 'rb') as f3:
        dict = pickle.load(f3)
        Textfile_T2Syn_From_SMW = open(
                        "C:/Saeed_Local_Files/InfoGather/" + Dataset + "_T2Syn_From_SMW.txt", "w", encoding="utf-8")
        for node in dict:
         
            Textfile_T2Syn_From_SMW.write(str(node)+" : \n")
            Textfile_T2Syn_From_SMW.write(str(dict[node])+"\n")
            Textfile_T2Syn_From_SMW.write("\n####################################\n")
         
        Textfile_T2Syn_From_SMW.close()
            
    
#Creating T2Syn Without using FPPR just by using the SMW_Graph   
def Write_T2Syn_Direct_To_File(TopN, Threshold):
    
    with open(get_Constant("EAB_Merged_Pickle_URL"), 'rb') as f3:
        dict = pickle.load(f3)
        with open(get_Constant("SMW_Graph_Pickle"), 'rb') as SMWGraphFile:
            # pickle.load(featureFile)
            SMW_Graph={}
            while 1:
                try:
                    PartialGraph = (pickle.load(SMWGraphFile))
    
                    for item in PartialGraph:
                        if(PartialGraph[item]>Threshold):
                            SMW_Graph[item] = PartialGraph[item]
    
                            
                except EOFError:
                    break
            cntr=0
            Textfile_T2Syn_Direct = open(
                "C:/Saeed_Local_Files/InfoGather/" + Dataset + "_Textfile_T2Syn_Direct.txt", "w", encoding="utf-8")
            T2Syn_Direct={}
            total=len(dict)
            num=0
            for tableID in dict:
                num+=1
                
                #print((num/total)*100)
                SourceHeader="Null"
                if (dict[tableID]["hasHeader"]):
                    SourceHeader = dict[tableID]["relation"][1][(dict[tableID]["headerRowIndex"])]
                Direct_edges=[]
                for key in SMW_Graph:
                    if(SMW_Graph[key]>Threshold):
                        if (key[0] == tableID or key[1] == tableID):
                            MatchTable=""
                            if(key[0]== tableID):
                                MatchTable = key[1]
                            else:
                                MatchTable = key[0]
                            
                            SynHeader="Null"
                            if (dict[MatchTable]["hasHeader"]):
                                SynHeader = dict[MatchTable]["relation"][1][(dict[MatchTable]["headerRowIndex"])]
                                
                            Direct_edges.append((MatchTable,SynHeader,SMW_Graph[key]))
                Sorted_Direct_Edges = sorted(Direct_edges, key=lambda item:item[2], reverse=True)[:min(TopN, len(Direct_edges))]
                T2Syn_Direct[tableID]= Sorted_Direct_Edges
                Textfile_T2Syn_Direct.write(str((tableID, SourceHeader))+": \n")
                Textfile_T2Syn_Direct.write(str(Sorted_Direct_Edges))
                Textfile_T2Syn_Direct.write("\n ######################################\n")
                
                        
                    
            
            Textfile_T2Syn_Direct.close()
            
            with open(get_Constant("T2Syn_Direct"), 'wb') as outfile:
                pickle.dump(T2Syn_Direct, outfile)
    
    
    
    
    
def Fetch_From_T2Syn(tableKey):
    with open(get_Constant("EAB_Merged_Pickle_URL"), 'rb') as f3:
        dict = pickle.load(f3)
        with open(get_Constant("T2Syn_DMA_Pickle"), 'rb') as f3:
            T2Syn_DMA = pickle.load(f3)
            headerRowIndex = dict[tableKey]["headerRowIndex"]
            Attribute = dict[tableKey]["relation"][1][headerRowIndex]
            print("Attribute is: "+str(Attribute))
            print ("Synonyms Are: "+str(T2Syn_DMA[tableKey]))

def Fetch_T2Syn(InputAttribute=None):
    with open(get_Constant("EAB_Merged_Pickle_URL"), 'rb') as f3:
        dict = pickle.load(f3)
        with open(get_Constant("T2Syn_DMA_Pickle"), 'rb') as f3:
            T2Syn_DMA = pickle.load(f3)
            for tableKey in T2Syn_DMA:
                headerRowIndex = dict[tableKey]["headerRowIndex"]
                Attribute = dict[tableKey]["relation"][1][headerRowIndex]
                if(InputAttribute == None or InputAttribute==Attribute ):
                    print("Attribute is: "+str(Attribute))
                    print ("Synonyms Are: "+str(T2Syn_DMA[tableKey]))



def FetchDataByID(ID):
    with open(get_Constant("EAB_Merged_Pickle_URL"), 'rb') as f3:
        dict = pickle.load(f3)
        Result = dict[ID]
        KeyIndex = Result["keyColumnIndex"]
        print("pageTitle: "+str(Result["pageTitle"]))
        print("Keys: "+ str(Result["relation"][KeyIndex]))
        print("Relation: " + str(Result["relation"]))
        print("URL: " + str(Result["url"]))
        print("#######################################")
        print(Result)

def Fetch_Data_By_URL(URL):
    with open('C:/Saeed_Local_Files/camera_json_files/EAB_Merged_camera.pickle', 'rb') as f3:
        dict = pickle.load(f3)
        for obj in dict:
            if (dict[obj]["url"] == URL):
                Result = dict[obj]
                KeyIndex = Result["keyColumnIndex"]
                print("pageTitle: " + str(Result["pageTitle"]))
                print("Keys: " + str(Result["relation"][KeyIndex]))
                print("Relation: " + str(Result["relation"]))
                print (obj)
                print("-----------------------------------")

                    # print(Result)


def OLD_FetchDataByURL(URL):
    with open('C:/Saeed_Local_Files/camera_json_files/Merged_camera.json', 'r') as f3:
        dict = json.load(f3)
        #print(len(dict))
        for group in dict:
            #print(len(group))
            for obj in group:
                if (obj["url"] == URL):
                    Result = obj
                    KeyIndex = obj["keyColumnIndex"]
                    print("pageTitle: " + str(Result["pageTitle"]))
                    print("Keys: " + str(Result["relation"][KeyIndex]))
                    print("Relation: " + str(Result["relation"]))
                    print (obj)
                    print("-----------------------------------")

                    # print(Result)





def compare_Three_Webtables(ID1, ID2, ID3="-1"):
    with open(get_Constant("EAB_Merged_Pickle_URL"), 'rb') as f3:
        dict = pickle.load(f3)
        print("################ Relation 1 ################# ")
        print("Relation: "+str(dict[ID1]["relation"]))
        print("URL: "+str(dict[ID1]["url"]))
        print("Context: "+str(dict[ID1]["textBeforeTable"])+" "+str(dict[ID1]["textAfterTable"]))

        print("################ Relation 2 ################# ")
        print("Relation: " + str(dict[ID2]["relation"]))
        print("URL: " + str(dict[ID2]["url"]))
        print("Context: " + str(dict[ID2]["textBeforeTable"]) + " " + str(dict[ID2]["textAfterTable"]))

        if(ID3!="-1"):
            print("################ Relation 3 ################# ")
            print("Relation: " + str(dict[ID3]["relation"]))
            print("URL: " + str(dict[ID3]["url"]))
            print("Context: " + str(dict[ID3]["textBeforeTable"]) + " " + str(dict[ID3]["textAfterTable"]))


def JsonToPickle():
    print("JsonToPickle Started2")
    with open(get_Constant("EAB_Merged_Json_URL"), 'r') as f3:
        dict={}
        JsonCollection = json.load(f3)
        print("len Json: "+str(len(JsonCollection)))
        for obj in JsonCollection:
            if obj["ID"] in dict:
                print(obj["ID"])
            dict[obj["ID"]]=obj
        print("len dict: "+str(len(dict)))
        with open(get_Constant("EAB_Merged_Pickle_URL"), 'wb') as outfile:
            pickle.dump(dict, outfile)
            print("len pickle: "+str(len(dict)))
            print("JsonToPickle Finished")

def test():
    with open('Sample_EAB.json', 'r') as f3:
        dict = json.load(f3)
        print(len(dict))

def Statistics_on_Dataset():
    TotalID=0
    TablesWithHeader=0
    TablesWithHeader_HorizontalWithKeys=0

    KeyID = 0
    VerticalCnt=0
    ColWithHeaders=0
    TotalCols=0

    TotalColsHorizonal=0
    ColdHorizontalWithHeader=0
    TotalColsKeyHorizontal=0
    ColKeyHorizontalWithHeader=0

    tempCnt=0

    EAB = []
    EABrelation = []
    with open(get_Constant("Merged_Json_URL"), 'r') as f1:
    #with open('C:/Saeed_Local_Files/basketball_NBA_json_files/Merged_basketball_NBA.json', 'r') as f1:
        dict = json.load(f1)
        Textfile_ColNames = open(Dataset+"_ColumnNames.txt", "w", encoding="utf-8")
        Textfile_Content = open(Dataset + "_Cotnent.txt", "w", encoding="utf-8")
        print(len(dict))
        for iter in dict:
            print(len(iter))
            for temp in iter:
                TotalID = TotalID + 1



                if ((temp["hasKeyColumn"]) and (temp["tableOrientation"] == "HORIZONTAL")):
                    KeyID = KeyID + 1
                    CorpusContentString = "***************************\n"
                    for rowCnt in range(len(temp["relation"][0])):
                        for col in temp["relation"]:
                            if (temp["hasHeader"] and temp["headerRowIndex"] == rowCnt):
                                HeaderName=col[rowCnt]
                                if(temp["relation"][temp["keyColumnIndex"]][rowCnt]==HeaderName):
                                    HeaderName = "*"+HeaderName+"*"
                                CorpusContentString += "[" + HeaderName + "], "
                            else:
                                CorpusContentString += "|" + col[rowCnt] + "|, "
                        CorpusContentString += "\n"
                    CorpusContentString += "***************************\n"
                    CorpusContentString += "URL: " + str(temp["url"]) + "\n"
                    Textfile_Content.write(CorpusContentString)

                    tempCnt = 0
                    ColString=""
                    ColString = "{"
                    for col in temp["relation"]:
                        TotalColsKeyHorizontal = TotalColsKeyHorizontal + 1
                        if (temp["hasHeader"]):
                            if (string_Cleanse(col[temp["headerRowIndex"]]) != ''):
                                ColKeyHorizontalWithHeader = ColKeyHorizontalWithHeader + 1
                                tempCnt = tempCnt+1
                                ColString += col[temp["headerRowIndex"]]+", "
                    ColString = ColString[:-2]
                    ColString += "}\n"
                    if(ColString!="}\n"):
                        Textfile_ColNames.write(ColString)
                    if (tempCnt == len(temp["relation"])):
                        TablesWithHeader_HorizontalWithKeys = TablesWithHeader_HorizontalWithKeys + 1


                if(temp["tableOrientation"] == "VERTICAL"):
                    VerticalCnt = VerticalCnt + 1

                    '''print('**************')
                    print(temp["relation"])
                    print(temp["url"])
                    print("hasKeyColumn: "+str(temp["hasKeyColumn"]))
                    print("keyColumnIndex: "+str(temp["keyColumnIndex"]))
                    print("hasHeader: " + str(temp["hasHeader"]))
                    print("headerRowIndex: " + str(temp["headerRowIndex"]))
                    print('**************')'''

                tempCnt = 0
                for col in temp["relation"]:
                    TotalCols = TotalCols+1
                    if(temp["hasHeader"]):
                        if(string_Cleanse(col[temp["headerRowIndex"]])!=''):
                            ColWithHeaders=ColWithHeaders+1
                            tempCnt=tempCnt+1
                if(tempCnt==len(temp["relation"])):
                    TablesWithHeader = TablesWithHeader+1

    Textfile_ColNames.close()
    Textfile_Content.close()
    print("Total Number of Tables: "+str(TotalID))
    print("Number of Horizontal Tables with Keys: " + str(KeyID))
    print("Ratio Horiz+Key to all: " + str(KeyID/TotalID))
    print("Total Number of Vertical Tables: " + str(VerticalCnt))
    print("Ratio Vert to all: " + str(VerticalCnt / TotalID))
    print("Total Number of Colums: " + str(TotalCols))
    print("Number of Colums with Headers: " + str(ColWithHeaders))
    print("Ratio Total Columns with Header to Total Columns: " + str(ColWithHeaders / TotalCols))
    print("Total Number of Colums in Horzontal Tables With Keys: " + str(TotalColsKeyHorizontal))
    print("Ratio Columns with Header to Total Columns in Horzontal Tables With Keys: " + str(ColKeyHorizontalWithHeader / TotalColsKeyHorizontal))
    print("Number of Tables with all Named Header: " + str(TablesWithHeader))
    print("Ratio of Tables with at least one Empty Header: " + str(1.0 - 1.0*(TablesWithHeader / TotalID)))
    print("Ratio of Tables with at least one Empty Header in Horzontal Tables With Keys: " + str(1.0 - 1.0 * (TablesWithHeader_HorizontalWithKeys / KeyID)))




def Statistics_on_Corpus_Sequential():
    TotalID=0
    TablesWithHeader=0
    TablesWithHeader_HorizontalWithKeys=0

    KeyID = 0
    VerticalCnt=0
    ColWithHeaders=0
    TotalCols=0

    TotalColsHorizonal=0
    ColdHorizontalWithHeader=0
    TotalColsKeyHorizontal=0
    ColKeyHorizontalWithHeader=0

    tempCnt=0



    for file in os.listdir("C:/Saeed_Local_Files/WDC_Data/Untarred_WDC_Data"):
        if file.endswith(".tar"):
            filename = file[:-4]
            filename = filename.replace('.','')
            filename = filename.replace('_', '')

            #Get_Data_From_Tar(filename)
            tar = tarfile.open("C:/Saeed_Local_Files/WDC_Data/Untarred_WDC_Data/" + filename + ".tar")
            # print(len(tar.getmembers()))

            for member in tar:
                f = tar.extractfile(member)
                if (member.offset != 0):
                    content = f.read()
                    temp = json.loads(content.decode("utf-8"))
                    TotalID = TotalID + 1
                    if ((temp["hasKeyColumn"]) and (temp["tableOrientation"] == "HORIZONTAL")):
                        KeyID = KeyID + 1
                        tempCnt = 0
                        for col in temp["relation"]:
                            TotalColsKeyHorizontal = TotalColsKeyHorizontal + 1
                            if (temp["hasHeader"]):
                                if (string_Cleanse(col[temp["headerRowIndex"]]) != ''):
                                    ColKeyHorizontalWithHeader = ColKeyHorizontalWithHeader + 1
                                    tempCnt = tempCnt+1
                        if (tempCnt == len(temp["relation"])):
                            TablesWithHeader_HorizontalWithKeys = TablesWithHeader_HorizontalWithKeys + 1


                    if(temp["tableOrientation"] == "VERTICAL"):
                        VerticalCnt = VerticalCnt + 1

                        '''print('**************')
                        print(temp["relation"])
                        print(temp["url"])
                        print("hasKeyColumn: "+str(temp["hasKeyColumn"]))
                        print("keyColumnIndex: "+str(temp["keyColumnIndex"]))
                        print("hasHeader: " + str(temp["hasHeader"]))
                        print("headerRowIndex: " + str(temp["headerRowIndex"]))
                        print('**************')'''

                    tempCnt = 0
                    for col in temp["relation"]:
                        TotalCols = TotalCols+1
                        if(temp["hasHeader"]):
                            if(string_Cleanse(col[temp["headerRowIndex"]])!=''):
                                ColWithHeaders=ColWithHeaders+1
                                tempCnt=tempCnt+1
                    if(tempCnt==len(temp["relation"])):
                        TablesWithHeader = TablesWithHeader+1

            tar.close()
    print("Total Number of Tables: "+str(TotalID))
    print("Number of Horizontal Tables with Keys: " + str(KeyID))
    print("Ratio Horiz+Key to all: " + str(KeyID/TotalID))
    print("Total Number of Vertical Tables: " + str(VerticalCnt))
    print("Ratio Vert to all: " + str(VerticalCnt / TotalID))
    print("Total Number of Colums: " + str(TotalCols))
    print("Number of Colums with Headers: " + str(ColWithHeaders))
    print("Ratio Total Columns with Header to Total Columns: " + str(ColWithHeaders / TotalCols))
    print("Total Number of Colums in Horzontal Tables With Keys: " + str(TotalColsKeyHorizontal))
    print("Ratio Columns with Header to Total Columns in Horzontal Tables With Keys: " + str(ColKeyHorizontalWithHeader / TotalColsKeyHorizontal))
    print("Number of Tables with all Named Header: " + str(TablesWithHeader))
    print("Ratio of Tables with at least one Empty Header: " + str(1.0 - 1.0*(TablesWithHeader / TotalID)))
    print("Ratio of Tables with at least one Empty Header in Horzontal Tables With Keys: " + str(1.0 - 1.0 * (TablesWithHeader_HorizontalWithKeys / KeyID)))






def Statistics_on_Corpus_Parralel():
    pool = Pool(processes=8)
    cnt=0
    for file in os.listdir("C:/Saeed_Local_Files/WDC_Data/Untarred_WDC_Data"):
            if file.endswith(".tar"):
                filename = file[:-4]
                filename = filename.replace('.','')
                filename = filename.replace('_', '')
                #pool.apply_async(Statistics_on_Corpus_Per_TarFile, [filename]) 

                
    pool.close()
    pool.join()

    ResultDict={
            "Number of Horizontal Tables with Keys":0,
            "AVG Column per table (Horzontal Tables With Keys)":0,
            "AVG Row per table (Horzontal Tables With Keys)":0,
            "Total EAB With Key Assumption (Horzontal Tables With Keys)":0,
            "Total EAB With No Assumption (Horzontal Tables With Keys)":0
            
            }
    for file in os.listdir("C:/Saeed_Local_Files/WDC_Data/Stats"):
        if file.endswith(".pickle"):
            with open("C:/Saeed_Local_Files/WDC_Data/Stats/"+file, 'rb') as f1:
                dict = pickle.load(f1)
                print(dict)
                
                ResultDict["AVG Column per table (Horzontal Tables With Keys)"] = ((ResultDict["AVG Column per table (Horzontal Tables With Keys)"]*ResultDict["Number of Horizontal Tables with Keys"]) + (dict["AVG Column per table (Horzontal Tables With Keys)"]*dict["Number of Horizontal Tables with Keys"]))/(dict["Number of Horizontal Tables with Keys"]+ResultDict["Number of Horizontal Tables with Keys"])
                ResultDict["AVG Row per table (Horzontal Tables With Keys)"] = ((ResultDict["AVG Row per table (Horzontal Tables With Keys)"]*ResultDict["Number of Horizontal Tables with Keys"]) + (dict["AVG Row per table (Horzontal Tables With Keys)"]*dict["Number of Horizontal Tables with Keys"]))/(dict["Number of Horizontal Tables with Keys"]+ResultDict["Number of Horizontal Tables with Keys"])
                ResultDict["Total EAB With Key Assumption (Horzontal Tables With Keys)"] += dict["Total EAB With Key Assumption (Horzontal Tables With Keys)"]
                ResultDict["Total EAB With No Assumption (Horzontal Tables With Keys)"] += dict["Total EAB With No Assumption (Horzontal Tables With Keys)"]
                ResultDict["Number of Horizontal Tables with Keys"] += dict["Number of Horizontal Tables with Keys"]
                
    
    Textfile = open(
                "C:/Saeed_Local_Files/WDC_Data/Stats/Whole_Corpus_Stats.txt", "w", encoding="utf-8")
    
    for key in ResultDict:
     
        Textfile.write(repr(key)+" :\n"+repr(ResultDict[key])+"\n")

        Textfile.write("##############################################\n")
        
        
    
    Textfile.close()
    
               

def Statistics_on_Corpus_Per_TarFile(tarfilename):
    TotalID=0
    TotalEAB_With_Key_assumption=0
    TotalEAB_No_assumption=0
    TablesWithHeader=0
    TablesWithHeader_HorizontalWithKeys=0
    
    KeyID = 0
    VerticalCnt=0
    ColWithHeaders=0
    TotalCols=0
    
    TotalColsHorizonal=0
    ColdHorizontalWithHeader=0
    TotalColsKeyHorizontal=0
    ColKeyHorizontalWithHeader=0
    
    RowHorizontalWithHeader=0
    
    tempCnt=0



    

    #Get_Data_From_Tar(filename)
    tar = tarfile.open("C:/Saeed_Local_Files/WDC_Data/Untarred_WDC_Data/" + tarfilename + ".tar")
    # print(len(tar.getmembers()))

    for member in tar:
        f = tar.extractfile(member)
        if (member.offset != 0):
            content = f.read()
            temp = json.loads(content.decode("utf-8"))
            TotalID = TotalID + 1
            if ((temp["hasKeyColumn"]) and (temp["tableOrientation"] == "HORIZONTAL")):
                KeyID = KeyID + 1
                RowHorizontalWithHeader +=  len(temp["relation"][0])
                TotalEAB_With_Key_assumption += len(temp["relation"]) - 1
                TotalEAB_No_assumption += (len(temp["relation"])*(len(temp["relation"])-1))/2
                tempCnt = 0
                for col in temp["relation"]:
                    TotalColsKeyHorizontal = TotalColsKeyHorizontal + 1
                    if (temp["hasHeader"]):
                        if (string_Cleanse(col[temp["headerRowIndex"]]) != ''):
                            ColKeyHorizontalWithHeader = ColKeyHorizontalWithHeader + 1
                            tempCnt = tempCnt+1
                if (tempCnt == len(temp["relation"])):
                    TablesWithHeader_HorizontalWithKeys = TablesWithHeader_HorizontalWithKeys + 1


            if(temp["tableOrientation"] == "VERTICAL"):
                VerticalCnt = VerticalCnt + 1

                '''print('**************')
                print(temp["relation"])
                print(temp["url"])
                print("hasKeyColumn: "+str(temp["hasKeyColumn"]))
                print("keyColumnIndex: "+str(temp["keyColumnIndex"]))
                print("hasHeader: " + str(temp["hasHeader"]))
                print("headerRowIndex: " + str(temp["headerRowIndex"]))
                print('**************')'''

            tempCnt = 0
            for col in temp["relation"]:
                TotalCols = TotalCols+1
                if(temp["hasHeader"]):
                    if(string_Cleanse(col[temp["headerRowIndex"]])!=''):
                        ColWithHeaders=ColWithHeaders+1
                        tempCnt=tempCnt+1
            if(tempCnt==len(temp["relation"])):
                TablesWithHeader = TablesWithHeader+1

    tar.close()
    
    
    StatDict={}
    StatDict["Total Number of Tables"]=(TotalID)
    StatDict["Number of Horizontal Tables with Keys"] =(KeyID)
    StatDict["Ratio Horiz+Key to all"] = (KeyID/TotalID)
    StatDict["Total Number of Vertical Tables"] = (VerticalCnt)
    StatDict["Ratio Vert to all"]= (VerticalCnt / TotalID)
    StatDict["Total Number of Colums"] = (TotalCols)
    StatDict["Number of Colums with Headers"]=(ColWithHeaders)
    StatDict["Ratio Total Columns with Header to Total Columns"] = (ColWithHeaders / TotalCols)
    StatDict["Total Number of Colums in Horzontal Tables With Keys"] = (TotalColsKeyHorizontal)
    StatDict["Ratio Columns with Header to Total Columns in Horzontal Tables With Keys"]  = (ColKeyHorizontalWithHeader / TotalColsKeyHorizontal)
    StatDict["Number of Tables with all Named Header"]  = (TablesWithHeader)
    StatDict["Ratio of Tables with at least one Empty Header"] = (1.0 - 1.0*(TablesWithHeader / TotalID))
    StatDict["Ratio of Tables with at least one Empty Header in Horzontal Tables With Keys"]=(1.0 - 1.0 * (TablesWithHeader_HorizontalWithKeys / KeyID))
    StatDict["AVG Column per table (Horzontal Tables With Keys)"] = TotalColsKeyHorizontal/KeyID
    StatDict["AVG Row per table (Horzontal Tables With Keys)"] = RowHorizontalWithHeader/KeyID
    StatDict["Total EAB With Key Assumption (Horzontal Tables With Keys)"] = TotalEAB_With_Key_assumption
    StatDict["Total EAB With No Assumption (Horzontal Tables With Keys)"] = TotalEAB_No_assumption
        
    
    


    with open('C:/Saeed_Local_Files/WDC_Data/Stats/'+tarfilename+'.pickle', 'wb') as outfile:
        pickle.dump(StatDict, outfile)

def Match_Columns(Threshold):

    NonNamed_Cols=[]
    NonNamed_Cols_URL=[]
    Named_Cols=[]
    Named_Cols_URL=[]
    Named_Headers=[]

    breakFlag=False

    with open(get_Constant("Merged_Json_URL"), 'r') as f1:
        # with open('C:/Saeed_Local_Files/basketball_NBA_json_files/Merged_basketball_NBA.json', 'r') as f1:
        dict = json.load(f1)
        print(len(dict))
        for iter in dict:
            print(len(iter))
            for temp in iter:
                for col in temp["relation"]:
                    if (temp["hasHeader"]):
                        if (string_Cleanse(col[temp["headerRowIndex"]]) != ''):
                            header = col.pop(temp["headerRowIndex"])
                            Named_Headers.append(header)
                            Named_Cols.append(col)
                            Named_Cols_URL.append(temp["url"])


                        else:
                            del col[0]
                            NonNamed_Cols.append(col)
                            NonNamed_Cols_URL.append(temp["url"])
                    else:
                        NonNamed_Cols.append(col)
                        NonNamed_Cols_URL.append(temp["url"])

        #Filtering Named and NoNaemd Columns
        Named_Cols = Named_Cols[:10000]
        NonNamed_Cols = NonNamed_Cols[:1000]



        #Documenting The No-named Column ValuesCamera
        '''Textfile_NoNamedCols_Values = open(Dataset + "_NoNamedCols_Values.txt", "w")
        for j in range(len(NonNamed_Cols)):
            Textfile_NoNamedCols_Values.write("\n******************************\n")
            Textfile_NoNamedCols_Values.write(str(j)+"- "+"No-Named Column Values: ")
            for value in NonNamed_Cols[j]:
                Textfile_NoNamedCols_Values.write(value + " |,| ")
            Textfile_NoNamedCols_Values.write("\n")
            Textfile_NoNamedCols_Values.write("Page Source: ")
            Textfile_NoNamedCols_Values.write(NonNamed_Cols_URL[j])
            #Textfile_NoNamedCols_Values.write("\n")
        Textfile_NoNamedCols_Values.close()'''




        #Documenting The Matching between No Named Columns with Named Columns
        '''
        Textfile_Match_NoNamedCol_W_NamedCol = open("C:/Saeed_Local_Files/"+Dataset + "_Match_NoNamedCol_W_NamedCol.txt", "w")

        Matched_Cntr=0
        for j in range(len(NonNamed_Cols)):
            Textfile_Match_NoNamedCol_W_NamedCol = open("C:/Saeed_Local_Files/"+Dataset + "_Match_NoNamedCol_W_NamedCol.txt", "a")
            Matched_NamedCols=[]
            Matched_NamedCols_Headers=[]
            Matched_NamedCols_URL=[]
            for i in range(len(Named_Cols)):
                OverLap_Cnt=0
                elem_num_processed=0
                min_setSize = min(len(NonNamed_Cols[j]), len(Named_Cols[i]))
                Max_NonOverlapped_elems = 1.0*(1-Threshold)*min_setSize
                Matchable=True
                for NoNamed_elem in NonNamed_Cols[j]:
                    for Named_elem in Named_Cols[i]:
                        if(string_Cleanse(NoNamed_elem)==string_Cleanse(Named_elem) and (string_Cleanse(NoNamed_elem)!="")):
                            OverLap_Cnt +=1
                            break
                        elem_num_processed += 1
                        if(elem_num_processed-OverLap_Cnt>Max_NonOverlapped_elems):
                            Matchable = False
                            break
                    if(Matchable==False):
                        break

                OverlapSim= (1.0*OverLap_Cnt)/(1.0*min_setSize)
                if (OverlapSim > Threshold):
                    Matched_NamedCols.append(Named_Cols[i])
                    Matched_NamedCols_Headers.append(Named_Headers[i])
                    Matched_NamedCols_URL.append(Named_Cols_URL[i])
            if(len(Matched_NamedCols)>0):
                Matched_Cntr += 1
                Textfile_Match_NoNamedCol_W_NamedCol.write("\n"+str(Matched_Cntr)+"***************************************\n")
                Textfile_Match_NoNamedCol_W_NamedCol.write("No-Named Column Values: ")
                for value in NonNamed_Cols[j]:
                    Textfile_Match_NoNamedCol_W_NamedCol.write(value+" |,| ")
                Textfile_Match_NoNamedCol_W_NamedCol.write("\n")
                Textfile_Match_NoNamedCol_W_NamedCol.write("Page Source: ")
                Textfile_Match_NoNamedCol_W_NamedCol.write(NonNamed_Cols_URL[j])
                Textfile_Match_NoNamedCol_W_NamedCol.write("\n")
                MatchedCol_Cntr=0
                for i in range(len(Matched_NamedCols)):
                    MatchedCol_Cntr += 1
                    Textfile_Match_NoNamedCol_W_NamedCol.write("\n"+ str(MatchedCol_Cntr)+"- "+"Matched Col Header: ")
                    Textfile_Match_NoNamedCol_W_NamedCol.write(Matched_NamedCols_Headers[i]+" - ")
                    Textfile_Match_NoNamedCol_W_NamedCol.write("Matched Col Values: ")
                    for value in Matched_NamedCols[i]:
                        Textfile_Match_NoNamedCol_W_NamedCol.write(value+" |,| ")
                    Textfile_Match_NoNamedCol_W_NamedCol.write("\nPage Source: "+Matched_NamedCols_URL[i])
                    Textfile_Match_NoNamedCol_W_NamedCol.write("\n")
                Textfile_Match_NoNamedCol_W_NamedCol.close() '''



        # Documenting The Matching between Named Columns

        Textfile_Match_NamedCol = open(
            "C:/Saeed_Local_Files/" + Dataset + "_Match_NamedCol.txt", "w")

        Textfile_Column_Match_Only = open(
            "C:/Saeed_Local_Files/" + Dataset + "_Column_Match_Only.txt", "w")

        #Visited=[]
        Matched_Cntr = 0
        for j in range(len(Named_Cols)):
            #if(j in Visited):
                #continue
            #Visited.append(j)
            QueryCol=[]
            for elem in Named_Cols[j]:
                if (elem not in QueryCol):
                    QueryCol.append(elem)
            Matched_NamedCols = []
            Matched_NamedCols_Headers = []
            Matched_NamedCols_URL = []
            for i in range(len(Named_Cols)):

                Textfile_Match_NamedCol = open(
                    "C:/Saeed_Local_Files/" + Dataset + "_Match_NamedCol.txt", "a")
                Textfile_Column_Match_Only = open(
                    "C:/Saeed_Local_Files/" + Dataset + "_Column_Match_Only.txt", "a")
                #if (i in Visited):
                    #continue
                Candidate_Col=[]
                for elem in Named_Cols[i]:
                    if (elem not in Candidate_Col):
                        Candidate_Col.append(elem)

                OverLap_Cnt = 0
                elem_num_processed = 0
                min_setSize = min(len(QueryCol), len(Candidate_Col))
                Max_NonOverlapped_elems = 1.0 * (1 - Threshold) * min_setSize
                Matchable = True
                for Named_elem1 in QueryCol:
                    #Detect Non-Matchables
                    elem_num_processed += 1
                    if (elem_num_processed - OverLap_Cnt > Max_NonOverlapped_elems):
                        break
                    for Named_elem2 in Candidate_Col:
                        if (string_Cleanse(Named_elem1) == string_Cleanse(Named_elem2)):
                            OverLap_Cnt += 1
                            break



                OverlapSim = (1.0 * OverLap_Cnt) / (1.0 * min_setSize)
                if (OverlapSim > Threshold):
                    #Visited.append(i)
                    if(OverlapSim>=1 ):#and len(Named_Cols[j])==len(Named_Cols[i])):
                        continue
                    Matched_NamedCols.append(Candidate_Col)
                    Matched_NamedCols_Headers.append(Named_Headers[i])
                    Matched_NamedCols_URL.append(Named_Cols_URL[i])
            if (len(Matched_NamedCols) > 0):
                Query_Plus_Matched_ColNames=[]
                String_Column_Match_Only=""
                Matched_Cntr += 1
                Textfile_Match_NamedCol.write(
                    "\n" + str(Matched_Cntr) + "***************************************\n")

                String_Column_Match_Only+=(
                    "\n" + str(Matched_Cntr) + "***************************************\n")

                Textfile_Match_NamedCol.write("Query Column Header: "+Named_Headers[j]+" - ")
                Query_Plus_Matched_ColNames.append(Named_Headers[j])
                String_Column_Match_Only+=("Query Column Header: " + Named_Headers[j] + "\nMatched Column Names: ")

                Textfile_Match_NamedCol.write("Query Column Values: ")
                for value in QueryCol:
                    Textfile_Match_NamedCol.write(value + " |,| ")
                Textfile_Match_NamedCol.write("\n")
                Textfile_Match_NamedCol.write("Page Source: ")
                Textfile_Match_NamedCol.write(Named_Cols_URL[j])
                Textfile_Match_NamedCol.write("\n")
                MatchedCol_Cntr = 0
                for i in range(len(Matched_NamedCols)):
                    MatchedCol_Cntr += 1
                    Textfile_Match_NamedCol.write(
                        "\n" + str(MatchedCol_Cntr) + "- " + "Matched Col Header: ")
                    Textfile_Match_NamedCol.write(Matched_NamedCols_Headers[i] + " - ")
                    if(Matched_NamedCols_Headers[i] not in Query_Plus_Matched_ColNames):
                        String_Column_Match_Only+=(Matched_NamedCols_Headers[i] + " - ")
                        Query_Plus_Matched_ColNames.append(Matched_NamedCols_Headers[i])

                    Textfile_Match_NamedCol.write("Matched Col Values: ")
                    for value in Matched_NamedCols[i]:
                        Textfile_Match_NamedCol.write(value + " |,| ")
                    Textfile_Match_NamedCol.write("\nPage Source: " + Matched_NamedCols_URL[i])
                    Textfile_Match_NamedCol.write("\n")
                Textfile_Match_NamedCol.close()
                if(len(Query_Plus_Matched_ColNames)>1):
                    Textfile_Column_Match_Only.write(String_Column_Match_Only)
                Textfile_Column_Match_Only.close()


def test_data_integrity():
    NonNamed_Cols=[]
    NonNamed_Cols_URL=[]
    Named_Cols=[]
    Named_Cols_URL=[]
    Named_Headers=[]

    breakFlag=False


    Matched_Tables=[]

    with open(get_Constant("Merged_Json_URL"), 'r') as f1:
        # with open('C:/Saeed_Local_Files/basketball_NBA_json_files/Merged_basketball_NBA.json', 'r') as f1:
        dict = json.load(f1)
        print(len(dict))
        for iter in dict:
            print(len(iter))
            for temp in iter:
                if(temp["url"]=="http://www.ebay.com.au/itm/Apple-iPhone-4s-16GB-Black-Smartphone-/181373687306"):
                    Matched_Tables.append(temp)

                for col in temp["relation"]:
                    if (temp["hasHeader"]):
                        if (string_Cleanse(col[temp["headerRowIndex"]]) != ''):
                            header = col.pop(temp["headerRowIndex"])
                            Named_Headers.append(header)
                            Named_Cols.append(col)
                            Named_Cols_URL.append(temp["url"])


                        else:
                            del col[0]
                            NonNamed_Cols.append(col)
                            NonNamed_Cols_URL.append(temp["url"])
                    else:
                        NonNamed_Cols.append(col)
                        NonNamed_Cols_URL.append(temp["url"])
        Matches=[]
        for i in range(len(Named_Cols)):
            if(Named_Cols_URL[i]=="https://www.whatifsports.com/nba-l/profile_player.asp?pid=398&view=5"):
                Matches.append(Named_Cols[i])
        print(len(Matches))


def Write_AllTables_to_File():
    with open(get_Constant("EAB_Merged_Pickle_URL"), 'rb') as f3:
        dict = pickle.load(f3)
       
        Textfile_All_Tables = open(
                    "C:/Saeed_Local_Files/InfoGather/" + Dataset + "_All_Tables.txt", "w", encoding="utf-8")
        line_Num=0 
        for data in dict:
            line_Num+=1
            Textfile_All_Tables.write(str(line_Num)+"-Lable: "+str(data)+"\n")
            Textfile_All_Tables.write(JsonTable_To_String(dict[data]))
         
            Textfile_All_Tables.write("##############################################\n")

            
            
        
        Textfile_All_Tables.close()
        
        
        
        

def Write_All_Filtered_Orig_Tables_to_File():
    with open(get_Constant("Deduplicated_Merged_Json_URL"), 'rb') as f3:
        dict = json.load(f3)
        Textfile_All_Filtered_Orig_Tables = open(
                    "C:/Saeed_Local_Files/dump/All_Filtered_Orig_Tables.txt", "w", encoding="utf-8")
        line_Num=0 
        for data in dict:
            line_Num+=1
            Textfile_All_Filtered_Orig_Tables.write(str(line_Num)+"-Lable: "+str(data["ID"])+"\n")
            Textfile_All_Filtered_Orig_Tables.write(JsonTable_To_String(data))
         
            Textfile_All_Filtered_Orig_Tables.write("##############################################\n")

            
            
        
        Textfile_All_Filtered_Orig_Tables.close()
        
        
        
        
        
        
def Write_All_Deduplicated_Tables_to_File(TableType=None, Preffered_Class=None):
    
    if(TableType=="T2D"):
        #T2D Parameter Initialization:
        ClassMap_CSV_File = "C:/Users/new/Downloads/classes_instance_233.csv"
        #ClassMap_CSV_SheetName = 'classes_instance_233'
        Attribute_Map_Folder = "C:/Users/new/Downloads/attributes_instance_233"
       
        
        #Setting ClassMap_Dict and AttribMap_Dict
        Dict_Arr = T2D_ClassMap_Attrib_Map(ClassMap_CSV_File, Attribute_Map_Folder)
        ClassMap_Dict = Dict_Arr[0]
        AttribMap_Dict = Dict_Arr[1]
        
    
    with open(get_Constant("Deduplicated_Merged_Json_URL"), 'rb') as f3:
        jsonDocs = json.load(f3)
        
        Textfile_All_Tables = open(
                    get_Constant("Json_Files_Directory")+ "/All_Deduplicated_Tables.txt", "w", encoding="utf-8")
        
        
        line_Num=0 
        for data in jsonDocs:
            
            if(TableType==None):
                line_Num+=1
                Textfile_All_Tables.write(str(line_Num)+"-Lable: "+str(data["ID"])+"\n")
                Textfile_All_Tables.write(JsonTable_To_String(data))
                Textfile_All_Tables.write("##############################################\n")
                                          
            if(TableType=="T2D"):
                if(ClassMap_Dict[data["ID"]] == Preffered_Class):
                    line_Num+=1
                    Textfile_All_Tables.write(str(line_Num)+"-Lable: "+str(data["ID"])+"\n")
                    Textfile_All_Tables.write("  Labelled Class: "+ClassMap_Dict[data["ID"]]+"\n")
                    Textfile_All_Tables.write("  Labelled Attributes: "+str(AttribMap_Dict[data["ID"]])+"\n")
                    Textfile_All_Tables.write(JsonTable_To_String(data))
                    Textfile_All_Tables.write("##############################################\n")

        Textfile_All_Tables.close()



def Set_All_Dedup_Tables_W_No_Headers():
    with open(get_Constant("Deduplicated_Merged_Json_URL"), 'rb') as f3:
        jsonDocs = json.load(f3)
        for table in jsonDocs:
            if (table["hasHeader"]==0):
                print(table["ID"])
                table["hasHeader"]=1
                table["headerRowIndex"] = 0 
                
                
    with open(get_Constant("Deduplicated_Merged_Json_URL"),'w') as outfile:
        json.dump(jsonDocs, outfile)
                    

def Fetch_Whole_Table(TableID):
    with open(get_Constant("EAB_Merged_Pickle_URL"), 'rb') as f3:
        dict = pickle.load(f3)
        Table = dict[TableID]
        print(JsonTable_To_String(Table))



if  (__name__ == "__main__"):
    #EAB_Gen()
    #Create_WIK()
    #FetchDataByID("15324-1")  #['56-0', '56-2', '56-3', '56-4', '75-0', '75-2', '75-3', '75-4', '113-0', '154-1',
    #OLD_FetchDataByURL("http://wiki.gnustep.org/index.php?title=FOSDEM_2006&diff=2884&oldid=2868")
    #Fetch_Data_By_URL("http://wiki.gnustep.org/index.php?title=FOSDEM_2006&diff=2884&oldid=2868")
    #Create_WIKV()WIA_queryAttr
    #Fetch_WIK_ByKey("Red Zone (Made-Att)25-0")
    #Fetch_WIKV_ByKeyValue(('Points Per Game','25.6'))
    #Create_WIA()
    #Fetch_WIK_ByHeader("Model")
    #test()
    #Create_Labeling(5)
    #Fetch_From_Labels("28-1","29-1")
    #Fetch_All_Labels()
    #JsonToPickle()
    #compare_Three_Webtables('30-2', '510-2')
    #Fetch_From_T2Syn('30-2')
    #Create_T2Syn_DMA(Threshold=0.3)
    #Statistics_on_Dataset()
    #Statistics_on_Corpus()
    #Match_Columns(0.6)
    #test_data_integrity()
    #print("test")
    #Generate_FPPR(0)
    #Write_T2Syn_Direct_To_File(2,0.1)
    #Create_T2Syn_From_SMW(100,0.0001)
    #Write_T2Syn_From_SMW_To_File()
   
    
    #Write_T2Syn_From_SMW_To_File()
    
    #Set_All_Dedup_Tables_W_No_Headers()
    
    '''Create_WIK()
    Create_WIA()
    Write_AllTables_to_File()
    Write_All_Filtered_Orig_Tables_to_File()'''
    #Write_T2Syn_Without_FPPR_To_File()
    
    #Write_AllTables_to_File()
    #Fetch_Whole_Table("627-1")
    
    #Deduplicate_Json_Tables()    
    #EAB_Gen()
    #Write_All_Filtered_Orig_Tables_to_File()
    Write_All_Deduplicated_Tables_to_File()
    #Write_AllTables_to_File()
    '''with open(get_Constant("EAB_Merged_Json_URL"), 'r') as f1:
        dict = json.load(f1)
        print(len(dict))'''
    #Statistics_on_Corpus_Parralel()
    
    #Clean_T2Syn_From_SMW()
    #Create_ColumnPostingList_Pickle_URL()
    


