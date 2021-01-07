'''
This is the implemetation of Mannheim Search Join,
which is used as one of the baselines in our evaluations.
Note that the Column Grouping and Grouped Column Consolidation steps in this implementation are invoked from
our proposed solution to ensure the comparisons are done fairly.
'''
import time
from CommonUtilities import *
from Dataset_Gathering import *
import subprocess
from Octopus import *
import copy
import random
from ACSDB import *
from shutil import copyfile
from Proposed_Solution import *

contributed_column_eval = (0,0,0)


def Mannheim_TableExtension(Input_Table, Input_idx, Vertical_Extensions = False):
    ###
    # This function extends the query keys by utilizing Unconstrained Extension proposed in Mannheim Search Join.
    # Note that for debugging purposes, a set of values are assigned for each cell in the result table, that the first value 
    # (which is the most frequent value correponding to the query value) is the final value for that cell. 
    ###
  
    
    
    #New Start ....
        #delete already created output Files
    for file in os.listdir(get_Constant("OutputFolder")):
        os.remove(get_Constant("OutputFolder")+"/"+file)
    
    #It uses a Threshold to search in those Tables that have columns which overlap with the input tables content higher than that Threshold
        
    
    Processed_Tables = []
    
    Col_Overlap_TH = 0.3
    Satifiability_Error_Threshold = 0.05
    


    #Create a table in which each columns Ai other than input columns follow the FD: INPUT->Ai
    #Outwards_orig_extracted_col_list hold a list of tuples (tableID, col_index), which are the columns extracted for outwards extension
    print("FD_Extension Started ...")
    
   
    #Outwards_orig_extracted_col_list hold a list of tuples (tableID, col_index), which are the columns extracted for outwards extension
    Outwards_orig_extracted_col_list, outwards_augmentingCols_with_keys, stat = Mannheim_Search_For_Augmenting_Columns([Input_Table[0]], Col_Overlap_TH, Satifiability_Error_Threshold, inwards=0 )
    
    Processed_Tables2=[]
    #As a Test, we want to ignore FD_Outwards_Extension and perform all column groupings in FD_inwards_Extension
    #FD_Outwards_Table, FD_Outwards_Units, Processed_Tables2  = FD_Outwards_Extension(Input_Table,outwards_augmentingCols_with_keys,  "0", Col_Overlap_TH, Satifiability_Error_Threshold, FD_Detection_Method, Processed_Tables,"", Vertical_Extensions)
    FD_Detection_Method = "Error_Threshold"
    FD_Outwards_Table, FD_Outwards_Units, Processed_Tables2  = FD_Outwards_Extension(Input_Table,[],  "0", Col_Overlap_TH, Satifiability_Error_Threshold, FD_Detection_Method, Processed_Tables,"", Vertical_Extensions)
    #if (len(FD_Outwards_Table)<=len(Input_Table)):
        #return [],[]
    
    '''print("Outwards_orig_extracted_col_list")
    for col in Outwards_orig_extracted_col_list:
        print(col)
    sys.exit()'''



    print("FD_Outwards_Extension Finished ...")
    print("Processed_Tables"+str(Processed_Tables))
    
    
    if(Processed_Tables2==None):
        Processed_Tables2=Processed_Tables
        
    #Save_ResultTables_To_File(FD_Outwards_Table)
    
    #Create the largest possible BCNF tables in which there exists a column set A: A->INPUT, 
    print("FD_inwards_Extension Started")
    num_of_inwards_extension=0
    if(FD_Detection_Method=="Error_Threshold"):
        #inwards_augmentingCols_with_keys.append(((keys, KeyID_To_InputColID_List),(table[row[0]],WT_Corpus[tableID]["units"][row[0]]), "to_be_checked_"+str(tableID)))
        #Check : What if we perform all the column Grouping in the style of Set-Valued Columns 
        
        all__augmentingCols_with_keys= outwards_augmentingCols_with_keys
        '''print("\n###############\nall__augmentingCols_with_keys\n################\n")
        for elem in all__augmentingCols_with_keys:
            print(elem)
            print("\n")
        print("\n###############\nall__augmentingCols_with_keys\n################\n")
        #system.exit()'''
        #num_of_inwards_extension  = FD_inwards_Extension(Input_Table,inwards_augmentingCols_with_keys,  "0"+"_"+str(Input_idx), Col_Overlap_TH, Satifiability_Error_Threshold, FD_Detection_Method, Processed_Tables2, Vertical_Extensions)
        num_of_inwards_extension  = FD_inwards_Extension(Input_Table,all__augmentingCols_with_keys,  "0"+"_"+str(Input_idx), Col_Overlap_TH, Satifiability_Error_Threshold, FD_Detection_Method, Processed_Tables2, Vertical_Extensions)
    
    
    if (len(FD_Outwards_Table)<len(Input_Table)):
        return [], [], "", ""
        
        
    '''if(FD_inwards_table_list=="Exit"):
        return FD_inwards_unit_list
    
    print("FD_inwards_Extension Finished")
    if(Processed_Tables3==None):
        Processed_Tables3=Processed_Tables2'''
        
    #Save_ResultTables_To_File(FD_inwards_table_list)
    
    
    '''
    #Integrate Candidate Input Columns
    outward_index_list=[]
    integrated_candidate_input_columns = []
    for i in range(len(Input_Table), len(FD_Outwards_Table)):
        if(FD_Outwards_Units[i]!=['text']):
            continue
        FD_Outwards_Table[i].pop(0) #getting rid of header
        integrated_candidate_input_columns.append(FD_Outwards_Table[i])
        outward_index_list.append(i)'''
        
        
    '''for list_idx in range(len(FD_inwards_table_list)):
        for table_idx in range(len(FD_inwards_table_list[list_idx])):
            if(FD_inwards_unit_list[list_idx][table_idx]!=['text']):
                continue
            FD_inwards_table_list[list_idx][table_idx].pop(0) #getting rid of header
            integrated_candidate_input_columns.append(FD_inwards_table_list[list_idx][table_idx])'''
        
    '''
    #Recursively perform this operationfor each column of the inwards/outwards table (other than input column) untill it reaches JoinDistance=MaxJoinDistance
    parent_Ext_ID = 0
    Ext_ID = parent_Ext_ID
    for idx in range(len(integrated_candidate_input_columns)):
        Ext_ID = Ext_ID+1
        Ext_ID=Recursive_Outwards_Extension([integrated_candidate_input_columns[idx]], outward_index_list[idx], Ext_ID, parent_Ext_ID, Processed_Tables3,Col_Overlap_TH, Satifiability_Error_Threshold)'''       
        
    
    

    
    #Integrate the Inwards and outwards extensions into one integrated extension.
    #The numebr for Col_Overlap_TH should be exactly the same the input parameter of FD_inwards_Extension function
    integrated_inwards_outwards_extension = Proposed_Integrate_Inwards_Outwards(Col_Overlap_TH, Input_Table)
    return integrated_inwards_outwards_extension
    #End ...



def Mannheim_FD_Outwards_Extension(Input_Table, QueryName, Col_Overlap_TH, Satifiability_Error_Threshold, Processed_Tables, input_header="", Vertical_Extensions=False):
    #return [],[],[],Processed_Tables 
    
    '''if(Table_Overlap(Input_Table, Processed_Tables) > 0.7):
        print("if(Table_Overlap(Input_Table, Processed_Tables) > 0.7):")
        return [],[],[],Processed_Tables'''
    
    #First of all, the column values should be cleansed and redundant values should be deleted
    CleansedColumnValues = []
        
    column=Input_Table[0]
    # Cleansing the Key List + Unique
    for x in range(len(column)):
        CleansedColumnValues.append((string_Cleanse(column[x]),column[x]))
    
    unique_input=[]
    #Deleting redundant values
    queryKeys = []
    for (cleansed_item, item) in CleansedColumnValues:
        if (cleansed_item not in queryKeys):
            queryKeys.append(cleansed_item)
            unique_input.append(item)
    #Val_Dist_Threshold = 0.15
    #1- Searching for the columns which can be used to extend the input table,
    '''
    #First, Discover All entities in the query's domain, untill no new enity is dicovered.
    print("Discovering new entities ...")
    l1 = Input_Table[0]
    while True:
        l2  = Discover_Query_Rows([l1], Col_Overlap_TH)
        if(len(set(l1))==len(set(l2))):
            break
        l1 = l2
    print("End of Discovering new entities")
    all_discovered_query_table_rows = l2

    #print(all_discovered_query_table_rows)
    

    search_list_unclustered = list(set(all_discovered_query_table_rows))
    search_list_clustered = Octopus_FuzzyGroup_Centroid_List(search_list_unclustered, Val_Dist_Threshold)
    '''
    
    #Outwards_orig_extracted_col_list hold a list of tuples (tableID, col_index), which are the columns extracted for outwards extension
    Outwards_orig_extracted_col_list, Augmenting_Cols, stat = Mannheim_Search_For_Augmenting_Columns([Input_Table[0]], Col_Overlap_TH, Satifiability_Error_Threshold, inwards=0 )
    if(stat=="Exit"):
        return Outwards_orig_extracted_col_list,[],[],[]
    

    
    #print("Augmenting_Columns")
    ##print(Augmenting_Cols)
    print("size of Augmenting_Cols is: "+repr(len(Augmenting_Cols)))
    #Write_Dict_W_Table_To_File(Seed_Tables,"AD_seeds")
    
    
    
    #2- Clustering the EAB_tables extracted from the AD_SeedTables
    #Based on Agglomerative Clustering:
    
    MergeCluster_Threshold = 0.5
    
    
    print("Agglomerative_Cluster_for_EABs Started ...!")
    
    Extension_Input_List = [unique_input]
    '''if (Vertical_Extensions):
        Extension_Input_List = [search_list_clustered]
    else:
        Extension_Input_List = [unique_input]'''

    #print(Augmenting_Cols)
    Aug_Clusters  = []
    #if("inwards" in QueryName):
    #for col in Augmenting_Cols:
        #print(str(col)+"\n")
    #sys.exit()
    Column_Grouping_Value_Dist_Threshold = 0.3
    Aug_Clusters  = Agglomerative_Cluster_for_Augmenting_Cols(Extension_Input_List,Augmenting_Cols, Column_Grouping_Value_Dist_Threshold, MergeCluster_Threshold)
    '''print("\n\n")
    for c in Aug_Clusters:
        for col in c:
            print(col)
        print("\n")
    print("\n\n")
    print("Agglomerative_Cluster_for_EABs Done!")
    sys.exit()'''
    
    #3-In this version, we do Value Fusion
    ExtendedTable = Grouped_Columns_As_Table(Extension_Input_List, Aug_Clusters, 0.1)
    
    #Adding the input headerName to the ExtendedTable (IF it is Inwards_Extension)
    if("inwards" in QueryName):
        ExtendedTable[0][0]=input_header
    
    if(len(ExtendedTable)>len(Input_Table)): #If we have an actual extension, then write it to file
        #Printing the output Extended Table into File
        outFile= get_Constant("OutputFolder")+"/" + QueryName +"_outward.csv"
        Write_Table_To_CSV(ExtendedTable, outFile)
    
    #populate units of the extended table:
    extended_table_units = []
    for col in Input_Table:
        extended_table_units.append(['input'])
    for cluster in Aug_Clusters:
        extended_table_units.append(cluster[1])
        
        
    #add input table to Processed_Tables
    Processed_Tables=Processed_Tables+[Input_Table]
    
    
    return Outwards_orig_extracted_col_list, ExtendedTable, extended_table_units, Processed_Tables
    

def Mannheim_Search_For_Augmenting_Columns(Input_Table, Col_Overlap_TH, Satifiability_Error_Threshold, inwards=0):
    '''
        #This function searches for the columns which have Functional Dependency with the input table columns
        #in this form, Those tables will be selected that consist of columns which have overlap data with all input columns (Higher than a threshold),
        #Then from those tables with the mapped query column under a non-key column, the Functional Dependecy constraint will be verified from a column to the mapped-key column,
        #and those columns passing the constraint, will be selected for augmentation.
        #Plus, from those tables with the mapped query column under a non-key column, we accept those columns with AVG value correlation with the mapped query column higher than a threshold.
    '''
    
    #The algorithm is as follows:
    #For Each input Column, we select the columns which have an overlap higher than TH with one of the input columns.
    #Then, we intersect between the tables overlaping with the input column to find those tables which satisfy all input columns.
    #Then we extract those columns from the result tables that have Functional Dependecy with the input coloumns.
    
    AugmentingCols_With_Keys=[]
    orig_extracted_cols_list = []
    Proposed_extracted_cols = []
    TablesList_Per_InputColumn = [] #This Array store the Table List of overlapping columns for each input column in each cell.
    Table_To_Column_Map = {} #It Maps each Table to its columns which have overlap with the input table columns(including their DMA Scores): {TableID->{TableColeID->(InputCol_Idx, DMA_SCORE}}
    InputCol_Idx = -1
    for column in Input_Table:
        
        InputCol_Idx += 1
       
        #First of all, the column values should be cleansed and redundant values should be deleted
        CleansedColumnValues = []
        
        # Cleansing the Key List + Unique
        for x in range(len(column)):
            CleansedColumnValues.append(string_Cleanse(column[x]))
    
        #Deleting redundant values
        queryKeys = []
        for item in CleansedColumnValues:
            if (item not in queryKeys):
                queryKeys.append(item)
    
        #Cheking the Posting Lists in order to find the tables containing the input table column values
        print("#Cheking the Posting Lists in order to find the tables containing the input table column value ...")
        
        DMA_MATCH = {} #DMA_MATCH tells each table score in terms of number of hits for input column value.
        with open(get_Constant("ColumnPostingList_Pickle_URL"), 'rb') as f3:
            PostingListDict = pickle.load(f3)
            DMA_MATCH = {}
            for queryKey in queryKeys:
                
                Hit_Columns=[]
                
                #No Supporting Approximate Match for each Query Key:
                '''Appriximate_Match_Threshold=0.3
                for word in PostingListDict:
                    if (Char_Level_Edit_Distance(queryKey, word)<Appriximate_Match_Threshold):
                        for col in PostingListDict[queryKey]:
                            Hit_Columns.append(col)'''
                
                if(queryKey in PostingListDict):
                    Hit_Columns = PostingListDict[queryKey]        
                
                #Calculating DMA_MATCH score:
                for col in Hit_Columns:
                    if (col=="995.-2"):
                        a=1
                    if (col not in DMA_MATCH):
                        DMA_MATCH[col] = 1
                    else:
                        DMA_MATCH[col] += 1
    
            # Populating ResultSet with those tables with DMA_Score >= Col_Overlap_TH (tableKey->(DMA Score))
            print("# Populating ResultSet with those tables with DMA_Score >= Col_Overlap_TH (tableKey->(DMA Score))")
                  
            with open(get_Constant("Deduplicated_Merged_Pickle_URL"), 'rb') as f3:
                WT_Corpus = pickle.load(f3)
                ResultSet = set()
                querySize = len(queryKeys)
                for colID in DMA_MATCH:
                    tableID = colID.split("-")[0]
                    matched_col_id=int(colID.split("-")[1])
                    col_size=len(list(set(WT_Corpus[tableID]["relation"][matched_col_id]))) - 1
                    if(col_size==0):
                        col_size=1
                    minsize = min(querySize, col_size)
                    #print(WT_Corpus[tableID]["relation"][0])
                    DMA_Score = (DMA_MATCH[colID] / minsize)
                    if(tableID=="995."):
                        print("DMA_Score:"+str(DMA_Score))
                    #if (DMA_Score >= Col_Overlap_TH and col_size>10):
                    if (DMA_Score >= Col_Overlap_TH):
                        ResultSet.add(tableID)
                        
                        if(tableID not in Table_To_Column_Map ):
                            Table_To_Column_Map[tableID] = {}
                            
                        #In case, a column with the same mapping exists in Table_To_Column_Map,
                        #Its score should be higher than the previous one
                        should_be_replaced = False
                        replace_ColID=-1
                        if(tableID in Table_To_Column_Map):
                            for splitted_colID in Table_To_Column_Map[tableID]:
                                if (Table_To_Column_Map[tableID][splitted_colID][0]==InputCol_Idx):
                                    if(DMA_Score>=Table_To_Column_Map[tableID][splitted_colID][1]):
                                            should_be_replaced = True
                                            replace_ColID  = splitted_colID
                        if(should_be_replaced):
                            Table_To_Column_Map[tableID].pop(replace_ColID,None)
                        Table_To_Column_Map[tableID][colID.split("-")[1]]=(InputCol_Idx, DMA_Score)
                                
                            
                        
                
                TablesList_Per_InputColumn.append(ResultSet)
                #print("(TablesList_Per_InputColumn): "+str((TablesList_Per_InputColumn)))
    
    #print("Table_To_Column_Map: "+str(Table_To_Column_Map))
    '''test_dictioanry = (Table_To_Column_Map["11599512_1_280388135214354946"])
    for data in test_dictioanry:
        print(data)
    return'''
    
    #Intersecting between TablesList_Per_InputColumn resultsets: 
    #It selects those tablels that have all needed columns that have overlap with the input columns 
    CommonTables = set.intersection(*TablesList_Per_InputColumn)
    #print("CommonTables:"+str(CommonTables))
    #Now selecting the columns from those tables which have Functional Dependecy with the overlapping columns:
    print("Now selecting the columns from those tables which have Functional Dependecy with the overlapping columns:")
    
    outwards_augmentingCols_with_keys= [] 
    inwards_augmentingCols_with_keys = []
    


    NONkey_tables={}
    num_of_all_tables=0
    num_of_NONkey_cols=0
    NONkey_col_list=[]
    num_of_all_cols=0


    #Assuming QueryTable is only one column: Collecting Non_Key_ColPairs: Assume A->B, A'->B' & A,B are Non-Key => Non_Key_ColPairs = [(A,B),(A',B')]
    Non_Key_ColPairs = []
    violation_detectability_threshold = 0.1
    WT_Corpus={}
    outwards_stat_dict={"num_of_nonKey_FD":0, "num_of_Key_FD":0, "num_of_No_FD":0, "num_of_rejected_No_FD":0, "rejected_colPairs":[], "rejected_colPairs":[]}
    inwards_stat_dict={"num_of_nonKey_FD":0, "num_of_Key_FD":0, "num_of_No_FD":0,"num_of_rejected_No_FD":0,  "rejected_colPairs":[], "rejected_colPairs":[], }
    #final_stat_dict = {"num_of_cols_extracted_from_key":0, "num_of_cols_extracted_from_nonKey_outwards":0, "num_of_cols_extracted_from_nonKey_coOccurence":0}
    
    #logging diferent types of accepted/rejected colPairs:
    inwards_key_cols_file = open("Column_Sel_Logs/inwards_key_cols.txt","w+")
    inwards_key_cols_cntr = 0
    
    inwards_nonKey_cols_file = open("Column_Sel_Logs/inwards_nonKey_cols.txt","w+")
    inwards_nonKey_cols_cntr = 0
    
    outwards_nonKey_cols_file = open("Column_Sel_Logs/outwards_nonKey_cols.txt","w+")
    outwards_nonKey_cols_cntr = 0
    
    outwards_key_cols_file = open("Column_Sel_Logs/outwards_key_cols.txt","w+")
    outwards_key_cols_cntr = 0
    
    inwards_rejected_co_occured_cols_file = open("Column_Sel_Logs/inwards_rejected_co_occured_cols.txt","w+")
    inwards_rejected_co_occured_cols_cntr = 0
    
    
    #Pre-computing the statistics and corpus size if the method is "co-occurence_pruning"
    statistics=[]
    corpus_size=0
    with open(get_Constant("Content_ACSDB_Pickle_URL"), 'rb') as f3:
        statistics = pickle.load(f3)
        set_cnt = set()
        for k in statistics:
            for elem in statistics[k]:
                #print(elem)
                #print(elem.split("-")[0])
                set_cnt.add(elem.split("-")[0])
        corpus_size = len(set_cnt)
        
    with open(get_Constant("Deduplicated_Merged_Pickle_URL"), 'rb') as f3:
        WT_Corpus = pickle.load(f3)
        for tableID in CommonTables:
            
            table =  WT_Corpus[tableID]["relation"]
            
            keyCol_index=-1
            if("keyColumnIndex" in WT_Corpus[tableID]):
                   keyCol_index=WT_Corpus[tableID]["keyColumnIndex"]
            
            #print(table)
            
            #Populating the KeyList IDs(Input Overlapping Columns) + #Populating the Key Mappings to Input Col + #Populating the Keys
            keys=[]
            keyIDList=[]
            KeyID_To_InputColID_List=[]
            
            #print(Table_To_Column_Map[table])
            Table_Mapping_Dict = Table_To_Column_Map[tableID]
            is_nonkey_table=False
            for Col_Idx in Table_Mapping_Dict:

                num_of_all_tables+=1
                #print(WT_Corpus[tableID])
                if("keyColumnIndex" not in WT_Corpus[tableID]):
                    NONkey_tables[tableID] = [str(Col_Idx)+"-Donno"]
                    is_nonkey_table=True
                else:
                    if(int(Col_Idx)!=WT_Corpus[tableID]["keyColumnIndex"]):
                        NONkey_tables[tableID] = [Col_Idx]
                        is_nonkey_table=True


                if(Table_Mapping_Dict[Col_Idx][0]) not in KeyID_To_InputColID_List:
                    keyIDList.append(int(Col_Idx))
                    keys.append(table[int(Col_Idx)])
                    KeyID_To_InputColID_List.append(Table_Mapping_Dict[Col_Idx][0])
            
                #print("####")
                '''print("tableID:"+str(tableID))
                #print("keys:"+str(keys))
                #print("KeyID_To_InputColID_List:"+str(KeyID_To_InputColID_List))
                print("Table_Mapping_Dict:"+(str(Table_Mapping_Dict)))
                print("WT_Corpus[tableID][keyColumnIndex]:"+str(WT_Corpus[tableID]["keyColumnIndex"]))
                print("is_nonkey_table:"+str(is_nonkey_table))
                print("keyIDList:"+str(keyIDList))
                if("45593932_0_6869375743660252130" in tableID):
                    sys.exit()'''

            augmenting_col_Idxes=[]
            if(is_nonkey_table==False):
                for i in range(len(table)):
                    if(i not in keyIDList):
                        augmenting_col_Idxes.append(i)
                    
            
                
            #Adding to  AugmentingCols_With_Keys: ((keys, KeyID_To_InputColID_List),(aug_Col, aug_Col_Unit): Just Like EAB that have the key Plus one Column
            
            for idx in augmenting_col_Idxes:
                AugmentingCols_With_Keys.append(((keys, KeyID_To_InputColID_List),(table[idx],WT_Corpus[tableID]["units"][idx]), tableID))
                orig_extracted_cols_list.append((tableID, idx, keyIDList))
                Proposed_extracted_cols.append(str(tableID)+"-"+str(idx))
                
                

    return  orig_extracted_cols_list, AugmentingCols_With_Keys , ""       


def MAnnheim_run(input_query_file):
    start = time.time()
    

    
    #QueryKeysExcelFile = "C:/Saeed_Local_Files/TestDataSets/T2D_233/Company.xlsx"
    #Integrate_Inwards_Outwards(0.3, Populate_QueryTable_From_ExcelFile(input_query_file)) 
    a= Mannheim_TableExtension(Populate_QueryTable_From_ExcelFile(input_query_file),0, False)
    print("\n\n Table Extension Result \n\n")
    print(a)
    print("\n\n")
    #print(c)
    #Create_Baseline_Unclustered(Populate_QueryKeys_From_ExcelFile(QueryKeysExcelFile), QueryKeysExcelFile.split("/")[-1].replace(".xlsx","") )

    
    #tests()
    
    end = time.time()
    elapsed = end - start
    print("elapsed time: "+str(elapsed))
 
if  (__name__ == "__main__"):
    #C:/Saeed_Local_Files/TestDataSets/T2D_233/Film.xlsx
    #C:/Saeed_Local_Files/TestDataSets/Test1/input2.xlsx
    
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/Country2.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/Cities.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/Company4.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/Film2.xlsx"
    
    
    #input_query_file = "C:/Users/new/Dropbox/1-Uni/Thesis/Meetings/5-Fall 2018/Sep_24/Games/Games_Query.xlsx"
    #run(input_query_file)
    #input_query_file = "C:/Users/new/Dropbox/1-Uni/Thesis/Meetings/5-Fall 2018/Sep_24/Film/Film_Query.xlsx"
    #run(input_query_file)
    #input_query_file = "C:/Users/new/Dropbox/1-Uni/Thesis/Meetings/5-Fall 2018/Sep_24/Company/Company_Query.xlsx"
    #run(input_query_file)
    #input_query_file = "C:/Users/new/Dropbox/1-Uni/Thesis/Meetings/5-Fall 2018/Sep_24/Country/Country_Query.xlsx"
    #run(input_query_file)
    
    
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/countries-rand10.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/countries-rand10.xlsx"
    input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/companies-rand10.xlsx"
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
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/cntries_test_1.xlsx"

    
    #tests()
    MAnnheim_run(input_query_file)
    #Test_Random_Queries()
    #PreProcess()
    #check_key_tagging_betw_mannheim_and_corpus()