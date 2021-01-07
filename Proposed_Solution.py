'''
This is the implementation of our proposed method.
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

contributed_column_eval = (0,0,0)

#In order to Log those FDs without any FD towards each other

no_FD_tbl_cntr=0


def PreProcess():
    #####
        #Preparing the data for being processed by table extension method
    #####
    
    #if(Dataset.startswith("TestDataSets")):
        
        
        '''#delete already created Json Files
        for file in os.listdir(get_Constant("JsonTablesFolder")):
            os.remove(get_Constant("JsonTablesFolder")+"/"+file)
        
        csvTables_to_jsonFiles(get_Constant("CsvTablesFolder"), get_Constant("JsonTablesFolder"))'''
    
        
        '''#update the units
        python2_command=" C:\Python27\python.exe C:/Users/new/Dropbox/1-Uni/CMPUT_605/Project/WinCode/Unit_Mgmt_Python2.7.py -f "+get_Constant("JsonTablesFolder")
        print(python2_command)
        os.system(python2_command)'''
        
        
        #KeyTagCorrection based on Heuristic in Mannheim
        Correcting_Tagged_Keys(get_Constant("JsonTablesFolder"))
        
        #Scraping "[" and "]" from tables
        Fix_Values(get_Constant("JsonTablesFolder"))
        
        
        #Create Deduplicated_Merged_Pickle_File
        Deduplicated_Merged_Json_Docs=[]
        for file in os.listdir(get_Constant("JsonTablesFolder")):
        
            with open(get_Constant("JsonTablesFolder")+"/"+file, 'r') as f1:
                jsonFile=json.load(f1)
                jsonFile["ID"] = file[:-5]
                #if(jsonFile["ID"]=="38"):
                    #a=1
                Deduplicated_Merged_Json_Docs.append(jsonFile)
            
        with open(get_Constant("Deduplicated_Merged_Json_URL"),'w') as outfile:
            json.dump(Deduplicated_Merged_Json_Docs, outfile)
          
            
        Deduplicated_Merged_Json_To_Pickle()
            
        Create_ColumnPostingList_Pickle_URL()
        
def Correcting_Tagged_Keys(JsonFiles_SourceFolder):
     '''
        #KeyTagCorrection based on Heuristic in Mannheim
     '''
     for file in os.listdir(JsonFiles_SourceFolder):
        
            with open(JsonFiles_SourceFolder+"/"+file, 'r') as f1:
                jsonFile=json.load(f1)
                hasKey = False
                if("hasKeyColumn" in jsonFile):
                    if(jsonFile["hasKeyColumn"]==True):
                        hasKey = True
                 
                if(hasKey):
                    jsonFile["units"][jsonFile["keyColumnIndex"]] == ["text"]
                    continue
                
                #If the you got here, it means the table does not have an appropriate key tagging,
                #so it needs to be tagged
                keyIdx = Mannheim_TaggedKey_Idx(jsonFile)
                if(keyIdx!=None):
                    jsonFile["hasKeyColumn"] = True
                    jsonFile["keyColumnIndex"] = keyIdx

                #Re-write the file:
                print(file)
                print(keyIdx)
                with open(JsonFiles_SourceFolder+"/"+file,'w') as outfile:
                        
                        json.dump(jsonFile, outfile)
                        
                        
                        
def Fix_Values(JsonFiles_SourceFolder):
     '''
        #KeyTagCorrection based on Heuristic in Mannheim
     '''
     for file in os.listdir(JsonFiles_SourceFolder):
        
            with open(JsonFiles_SourceFolder+"/"+file, 'r') as f1:
                jsonFile=json.load(f1)
                
                table = jsonFile["relation"]
                for col_idx in range(len(table)):
                    for row_idx in range(len(table[col_idx])):
                        table[col_idx][row_idx]=str(table[col_idx][row_idx]).replace("[","").replace("]","")
                #Re-write the file:
                print(file)

                with open(JsonFiles_SourceFolder+"/"+file,'w') as outfile:
                        
                        json.dump(jsonFile, outfile)

def check_key_tagging_betw_mannheim_and_corpus():
    print("check_key_tagging_betw_mannheim_tagged Started ...")
    for file in os.listdir(get_Constant("JsonTablesFolder")):
                    
            with open(get_Constant("JsonTablesFolder")+"/"+file, 'r') as f1:
                jsonFile=json.load(f1)
                mannheim_key_idx = Mannheim_TaggedKey_Idx(jsonFile)
                corpus_key_idx = jsonFile["keyColumnIndex"]
                if(mannheim_key_idx!=corpus_key_idx):
                    print(file)
                    print("keyColumnIndex: "+str(corpus_key_idx))
                    print("mannheim_key_idx"+str(mannheim_key_idx))

    print("check_key_tagging_betw_mannheim_tagged Finished ...")

def Mannheim_TaggedKey_Idx(jsonFile):
    '''
    #Mannheim Heuristic method for tagging the key of a table
    '''
    #If a table has a tagged key, and that key is string, then it returns the previous tagged key, since it assumes that the tagging is already true!

    hasKey = False
    if("hasKeyColumn" in jsonFile):
        if(jsonFile["hasKeyColumn"]==True):
            hasKey = True
                 
    if(hasKey):
        jsonFile["units"][jsonFile["keyColumnIndex"]] == ["text"]
        #return jsonFile["keyColumnIndex"]

    #tagging the key based on the heuristic that:
    '''If a table contains an attriute having a header containing the string name, this
        attribute will be chosen as subject attribute. Otherwise, the string
        attribute with the highest number of unique values is chosen as
        subject attribute. In cases where two or more attributes contain
        equally high numbers of unique values, the left-most attribute is
        chosen. Furthermore, we only consider attributes with at least 60%
        unique values.
    '''
    table = (jsonFile["relation"])
    units = jsonFile["units"]

    #Checking column header 'Name'
    if(jsonFile["hasHeader"]==True):
        for i in range(len(table)):
            if("name" in string_Cleanse(table[i][jsonFile["headerRowIndex"]])):
                return i

    #Checking the string attribute with the highest number of unique values
    col_duplicate_rate = []
    for i in range(len(table)):
        if(units[i]==["text"]):
            unique_vals = []
            for j in range(len(table[i])):
                if(j!=jsonFile["headerRowIndex"] and table[i][j] not in unique_vals):
                    unique_vals.append(table[i][j])
            col_size = len(table)-1 if jsonFile["hasHeader"] else len(table) 
            col_duplicate_rate.append((i,1-(len(unique_vals)/col_size)))

    #sorting the columns based on their uniqueness
    sorted_unique_cols = sorted(col_duplicate_rate, key=lambda item:(item[1],item[0]))
    if(sorted_unique_cols[0][1])<0.4:
        return sorted_unique_cols[0][0]

    #In case no appropriate column was found
    return None

def TableExtension(Input_Table, Input_idx, FD_Detection_Method, Solution_Type, Vertical_Extensions = False):
###
    # This function extends the query keys based on our proposed solution 
    # Note that for debugging purposes:
        #for atomic-valued columsn: a set of values are assigned for each cell in the result table, that the first value 
            # (which is the most frequent value correponding to the query value) is the final value for that cell. 
        #for set-valued columsn: a set of values are assigned for each cell in the result table, that all values are used
            #as the final values for that cell.
 ###
#This is the proposed Solution for Table Extension Task- The input is a Table,
#The algorithm first search for the tables based on Query key coverage, which can consist of composite keys
#Search process tries to detect those columns which are useful to extend the input with
#Then it tries to Group those detected columns
    ######
        #The goal of this function is to extend the input table
        #ATTN: IN THIS VERSION, ONLY SINGLE LHS FUNCTIONAL DEPENDENCIES ARE CONSIDERED!
    ######
    
    
    #Initializing Logger:
    #global GlobalString
    #Logger = open("C:/Saeed_Local_Files/Proposed_Sol/" + QueryName+"_Log.txt", "w", encoding="utf-8")

   
    #PreProcess()
    
    #Assuming input tables are single column
 
    
    
    
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
    
    #1- First finding the inwards and outwards FDs:
    outwards_augmentingCols_with_keys ,inwards_augmentingCols_with_keys , outwards_stat_dict, inwards_stat_dict = Search_For_Augmenting_Columns(Input_Table, Col_Overlap_TH, Satifiability_Error_Threshold, FD_Detection_Method,Solution_Type)
    
    Processed_Tables2=[]
    #As a Test, we want to ignore FD_Outwards_Extension and perform all column groupings in FD_inwards_Extension
    #FD_Outwards_Table, FD_Outwards_Units, Processed_Tables2  = FD_Outwards_Extension(Input_Table,outwards_augmentingCols_with_keys,  "0", Col_Overlap_TH, Satifiability_Error_Threshold, FD_Detection_Method, Processed_Tables,"", Vertical_Extensions)
    
    FD_Outwards_Table, FD_Outwards_Units, Processed_Tables2  = FD_Outwards_Extension(Input_Table,[],  "0", Col_Overlap_TH, Satifiability_Error_Threshold, FD_Detection_Method, Processed_Tables,"", Vertical_Extensions)
    #FD_Outwards_Table, FD_Outwards_Units, Processed_Tables2  = FD_Outwards_Extension(Input_Table,outwards_augmentingCols_with_keys, "0", Col_Overlap_TH, Satifiability_Error_Threshold, FD_Detection_Method, Processed_Tables,"", Vertical_Extensions)
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
    #if(FD_Detection_Method=="Error_Threshold"):
    #inwards_augmentingCols_with_keys.append(((keys, KeyID_To_InputColID_List),(table[row[0]],WT_Corpus[tableID]["units"][row[0]]), "to_be_checked_"+str(tableID)))
    #Check : What if we perform all the column Grouping in the style of Set-Valued Columns 
    

   
    #all_augmentingCols_with_keys= outwards_augmentingCols_with_keys + inwards_augmentingCols_with_keys
    
    '''print("\n###############\nall__augmentingCols_with_keys\n################\n")
    for elem in all__augmentingCols_with_keys:
        print(elem)
        print("\n")
    print("\n###############\nall__augmentingCols_with_keys\n################\n")
    #system.exit()'''
    #num_of_inwards_extension  = FD_inwards_Extension(Input_Table,inwards_augmentingCols_with_keys,  "0"+"_"+str(Input_idx), Col_Overlap_TH, Satifiability_Error_Threshold, FD_Detection_Method, Processed_Tables2, Vertical_Extensions)
    #num_of_inwards_extension  = FD_inwards_Extension(Input_Table,all_augmentingCols_with_keys,  "0"+"_"+str(Input_idx), Col_Overlap_TH, Satifiability_Error_Threshold, FD_Detection_Method, Processed_Tables2, Vertical_Extensions)
    num_of_inwards_extension  = FD_inwards_outwards_Extension(Input_Table,inwards_augmentingCols_with_keys,outwards_augmentingCols_with_keys,  "0"+"_"+str(Input_idx), Col_Overlap_TH, Satifiability_Error_Threshold, FD_Detection_Method, Processed_Tables2, Vertical_Extensions)

    
    if (len(FD_Outwards_Table)<len(Input_Table)):
        return [], [], outwards_stat_dict, inwards_stat_dict
        
        
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
    return integrated_inwards_outwards_extension, outwards_stat_dict, inwards_stat_dict

def Extended_Columns_Ranking( FD_Outwards_Table, integrated_inwards_outwards_extension, query_header):
    #For this version, the output of extended_column_ranking is just the sorted list of extended column with their input_relatednedd scores associated to them. 
    #Extended_Columns_Ranking( FD_Outwards_Table, integrated_inwards_outwards_extension)
    
    print("##### Extended_Columns_Ranking Started #### ")
    print(FD_Outwards_Table)
    print("\n\n\n")
    print(integrated_inwards_outwards_extension)
    
    #ACSDB Initialization:
    Header_ACSDB={}
    with open(get_Constant("Header_ACSDB_Pickle_URL"), 'rb') as outfile:
        Header_ACSDB = pickle.load(outfile)
    Content_ACSDB={}
    with open(get_Constant("Content_ACSDB_Pickle_URL"), 'rb') as outfile:
        Content_ACSDB = pickle.load(outfile)      
    
    #Ranking Scores:
    col_relatedness_scores = {}
    
    
    
    #### Calculate input relatedness for outwards: #######
    #!!Assigining heading for the inpu column!!! -> That should change to automatic!!!
    #FD_Outwards_Table[0][0] = "company"
    if(len(FD_Outwards_Table)>0):
        FD_Outwards_Table[0][0] = query_header
    
    i=1
    for i in range(1, len(FD_Outwards_Table)):
        col_relatedness_scores[str(i)+"-"+str(FD_Outwards_Table[i][0])] = Col_InputRelatedness_Outwards(FD_Outwards_Table[0], FD_Outwards_Table[i], Header_ACSDB, Content_ACSDB)    
    

    
    
    ###### Calculate input relatedness for inwards:  ########
    
    #checking each column, if the column contains '###', then the column after that is key inwards column, then 
    # the column relatedness score is calculated  as the average of header + average rows per each input row:
    for j in range(1, len(integrated_inwards_outwards_extension)):
        if (integrated_inwards_outwards_extension[j][0]=='###'):
            if(j+1<len(integrated_inwards_outwards_extension)): # This is not the last column
                i=i+1
                col_relatedness_scores[str(i)+"-"+str(integrated_inwards_outwards_extension[j+1][1])] = Col_InputRelatedness_Inwards(integrated_inwards_outwards_extension[0], integrated_inwards_outwards_extension[j+1], Header_ACSDB, Content_ACSDB)    
                
    
    print("#####  col_relatedness_scores #### ")
    #print(col_relatedness_scores):
    
    
    sorted_scores_cols = sorted(col_relatedness_scores.items(), key=lambda item:item[1][2], reverse=True)
    
    for col in sorted_scores_cols:
        print(str(col[0])+":"+" {:.2f}".format(col[1][0])+" , "+ "{:.2f}".format(col[1][1])+" , " + "{:.2f}".format(col[1][2]))
    #system.exit()
    print("#####  Extended_Columns_Ranking Finished #### ")
    return 




def New_Extended_Columns_Ranking(extended_table, query_header, calc_type, logfile_path, table_eval, final_eval):
    #For this version, the output of extended_column_ranking is just the sorted list of extended column with their input_relatednedd scores associated to them. 
    #calc_type:
        #0- Header_Consistency
        #1- Row_Consistency
        #2- (Header_Consistency+Row_Consistency)/2
    
    logfile = open(logfile_path, "w", encoding="utf-8")
    
    logfile.write("##### Extended_Columns_Ranking Started #### \n")
    #logfile.write(extended_table)
    #logfile.write("\n\n\n")
    
    #ACSDB Initialization:
    Header_ACSDB={}
    with open(get_Constant("Header_ACSDB_Pickle_URL"), 'rb') as outfile:
        Header_ACSDB = pickle.load(outfile)
    Content_ACSDB={}
    with open(get_Constant("Content_ACSDB_Pickle_URL"), 'rb') as outfile:
        Content_ACSDB = pickle.load(outfile)      
    
    #Ranking Scores:
    col_relatedness_scores = {}
    
    
    #### Calculate input relatedness: #######
    #!!Assigining heading for the input column!!! -> That should change to automatic!!!
    #AS an example : FD_Outwards_Table[0][0] = "company"
    if(len(extended_table)>0):
        extended_table[0][0] = query_header
    
    i=1
    for i in range(1, len(extended_table)):
        col_relatedness_scores[str(i)+"-"+str(extended_table[i][0])] = New_Col_InputRelatedness(extended_table[0], extended_table[i], Header_ACSDB, Content_ACSDB)    
    
    logfile.write("#####  col_relatedness_scores #### \n")
    #print(col_relatedness_scores):
    
    all_related_cols=set()
    for eval_record in table_eval:
        if(len(eval_record[2])>0):
            all_related_cols.add(eval_record[3])
        
    
    sorted_scores_cols = sorted(col_relatedness_scores.items(), key=lambda item:item[1][calc_type], reverse=True)
    correct=0
    
    for i in range(len(sorted_scores_cols)):
        col = sorted_scores_cols[i]
        result=""
        if(i<final_eval[0]):
            if(col[0] in all_related_cols):
                result="Y"
                correct+=1
            else:
                result="N"

            
            
        logfile.write(str(result)+" - "+str(i+1)+"- '"+str(col[0])+"' :"+" {:.2f}".format(col[1][0])+" , "+ "{:.2f}".format(col[1][1])+" , " + "{:.2f}".format(col[1][2])+"\n")
        
    logfile.write("\n\n------------------\n")
    logfile.write("number of corrects: "+str(correct)+"\n")
    logfile.write("number of all related columns: "+str(final_eval[0])+"\n")
    logfile.write("number of all extended columns: "+str(len(sorted_scores_cols))+"\n")
    r_precision = (correct/final_eval[0])
    logfile.write("R-Precision: "+str(r_precision)+"\n")
    logfile.write("------------------\n\n")
    #system.exit()
    
    logfile.write("#####  Extended_Columns_Ranking Finished #### \n")
    
    logfile.close()
    return 



def New_Col_InputRelatedness(query_col, extended_col, Header_ACSDB, Content_ACSDB):
    ####
        #This Function calculates input_Relatedness between two columns
        #Based on Header_Consistency_score and Average_Row_Consistency
    ####
   
    #Header_Consistency_score = Header_Or_Row_Consistency(query_col[0], extended_col[0], Header_ACSDB)
    Total_Header_Consistency=0
    header_Cntr=0
    for header_value in extended_col[0]:
        if(header_value =="atomic-valued" or header_value =="set-valued"):
            continue
        Total_Header_Consistency+= Header_Or_Row_Consistency(query_col[0], header_value, Content_ACSDB, "Exact_Match")
        header_Cntr+=1
                
    Average_Header_Consistency=0
    if(header_Cntr!=0):
        Average_Header_Consistency = Total_Header_Consistency/header_Cntr
    
    #Average_Row_Consistency of all the rows with the same approaximate key 
    
    Total_Row_Consistency=0
    row_Cntr=0
    for i in range(1,len(query_col)):
        row_average=0
        single_row_consistency_total=0
        num_of_eligible_vals=0
        for extendedColumn_value in extended_col[i]:
            single_row_consistency_total+= Header_Or_Row_Consistency(query_col[i], extendedColumn_value, Content_ACSDB, "Exact_Match")
            num_of_eligible_vals+=1
            if(extended_col[0][0]=="atomic-valued"):
                break
        if(len(extended_col[i])>0):
            #single_row_consistency_average = single_row_consistency_total/len(extended_col[i])
            single_row_consistency_average = single_row_consistency_total/num_of_eligible_vals
            Total_Row_Consistency+= single_row_consistency_average
        #print("Row_Consistency between "+str(EAB1[1][i])+" and "+ str(EAB2[1][j])+" is "+str(Total_Row_Consistency))
        row_Cntr+=1
                
    Average_Row_Consistency=0
    if(row_Cntr!=0):
        Average_Row_Consistency = Total_Row_Consistency/row_Cntr
        
    return [Average_Header_Consistency, Average_Row_Consistency, (Average_Header_Consistency+Average_Row_Consistency)/2]
    #return [Header_Consistency_score, Average_Row_Consistency, Header_Consistency_score]




def Col_InputRelatedness_Outwards(col1, col2, Header_ACSDB, Content_ACSDB):
    ####
        #This Function calculates input_Relatedness between two columns
        #Based on Header_Consistency_score and Average_Row_Consistency
        #Retuned Col_InputRelatedness is the average between the two scores
    ####
   
    Header_Consistency_score = Header_Or_Row_Consistency(col1[0], col2[0], Header_ACSDB)
    #print("Header_Consistency_score between "+str(EAB1[1][0])+" and "+ str(EAB2[1][0])+" is "+str(Header_Consistency_score))
    
    #Average_Row_Consistency of all the rows with the same approaximate key 
    
    Total_Row_Consistency=0
    row_Cntr=0
    for i in range(1,len(col1)):
                Total_Row_Consistency+= Header_Or_Row_Consistency(col1[i], col2[i], Content_ACSDB, "Exact_Match")
                #print("Row_Consistency between "+str(EAB1[1][i])+" and "+ str(EAB2[1][j])+" is "+str(Total_Row_Consistency))
                row_Cntr+=1
                
    Average_Row_Consistency=0
    if(row_Cntr!=0):
        Average_Row_Consistency = Total_Row_Consistency/row_Cntr
        
    #return [Header_Consistency_score, Average_Row_Consistency, (Header_Consistency_score+Average_Row_Consistency)/2]
    return [Header_Consistency_score, Average_Row_Consistency, Header_Consistency_score]

def Col_InputRelatedness_Inwards(col1, col2, Header_ACSDB, Content_ACSDB):
    ####
        #This Function calculates input_Relatedness between two columns
        #Based on Header_Consistency_score and Average_Row_Consistency
        #Retuned Col_InputRelatedness is the average between the two scores22222
    ####
   
    Header_Consistency_score = Header_Or_Row_Consistency(col1[0], col2[1], Header_ACSDB)
    #print("Header_Consistency_score between "+str(EAB1[1][0])+" and "+ str(EAB2[1][0])+" is "+str(Header_Consistency_score))
    
    #Average_Row_Consistency of all the rows with the same approaximate key 
    
    
    #fill col1 based on the filled cells in col2
    col1_copy = copy.copy(col1)
    for idx in range(1, len(col1_copy)):
        if(col1_copy[idx]==""):
           col1_copy[idx]=col1_copy[idx-1] 
            
    
    Total_Row_Consistency=0
    row_Cntr=0
    for i in range(1,len(col1)):
        if((col2[i]!="") and col2[i]!=col2[1]):
                #print(col1_copy[i], col2[i])
                Total_Row_Consistency+= Header_Or_Row_Consistency(col1_copy[i], col2[i], Content_ACSDB, "Exact_Match")
                #print("Row_Consistency between "+str(EAB1[1][i])+" and "+ str(EAB2[1][j])+" is "+str(Total_Row_Consistency))
                row_Cntr+=1
                
    Average_Row_Consistency=0
    if(row_Cntr!=0):
        Average_Row_Consistency = Total_Row_Consistency/row_Cntr
        
    #return [Header_Consistency_score, Average_Row_Consistency, (Header_Consistency_score+Average_Row_Consistency)/2]
    return [Header_Consistency_score, Average_Row_Consistency, Header_Consistency_score]
 



def ColumnPair_NPMI(colA, colB, Statistics, CorpusSize):
    
    sum_row_nPMI=0
    non_empty_rows = 0
    for i in range(1,len(colA)):
        Tuple = (string_Cleanse(colA[i]), string_Cleanse(colB[i]))
        if(Tuple[0]!="" and Tuple[1]!=""):
            sum_row_nPMI += Normalized_Row_PMI(Tuple, Statistics, CorpusSize)
            non_empty_rows+=1
    
    column_size = len(colA) - 1
    avg_row_nPMI = 0
    if(non_empty_rows>0):
        avg_row_nPMI = sum_row_nPMI/non_empty_rows
    
    final_nPMI = avg_row_nPMI
    
    return final_nPMI
        
    
    
def Recursive_Outwards_Extension(Input_Table, Input_idx, input_Ext_ID, input_parent_Ext_ID, Processed_Tables, Col_Overlap_TH, Satifiability_Error_Threshold):
    
    ######
        #Goal: Ricursively extract those columns that Input_table->X
    ######
    
    
    #Initializing Logger:
    #global GlobalString
    #Logger = open("C:/Saeed_Local_Files/Proposed_Sol/" + QueryName+"_Log.txt", "w", encoding="utf-8")
    
    #if(input_Ext_ID > MaxJoinDistance):
       #return
   
    #Assuming input tables are single column
 
        
    
    print("Start Recursive_Outwards_Extension ...")
    
    #Create a BCNF table in which each columns Ai other than input columns follow the FD: INPUT->Ai
    Outwards_orig_extracted_col_list, FD_Outwards_Table, FD_Outwards_Units, Processed_Tables2  = FD_Outwards_Extension(Input_Table, str(input_Ext_ID)+"("+str(input_parent_Ext_ID)+"_"+str(Input_idx)+")", Col_Overlap_TH, Satifiability_Error_Threshold, Processed_Tables)
    #print("Processed_Tables"+str(Processed_Tables))
    
    '''if(Processed_Tables2==None):
        Processed_Tables2=Processed_Tables'''
    
    '''#Create the largest possible BCNF tables in which there exists a column set A: A->INPUT, 
    FD_inwards_table_list, FD_inwards_unit_list, Processed_Tables3  = FD_inwards_Extension(Input_Table, "distance_"+str(JoinDistance)+"_"+str(Input_idx), Col_Overlap_TH, Satifiability_Error_Threshold, Processed_Tables2)
    if(Processed_Tables3==None):
        Processed_Tables3=Processed_Tables2'''
    
    
    #Integrate Candidate Input Columns
    outward_index_list=[]
    integrated_candidate_input_columns = []
    for i in range(len(Input_Table), len(FD_Outwards_Table)):
        if(FD_Outwards_Units[i]!=['text']):
            continue
        FD_Outwards_Table[i].pop(0) #getting rid of header
        integrated_candidate_input_columns.append(FD_Outwards_Table[i])
        outward_index_list.append(i)
        
    '''for list_idx in range(len(FD_inwards_table_list)):
        for table_idx in range(len(FD_inwards_table_list[list_idx])):
            if(FD_inwards_unit_list[list_idx][table_idx]!=['text']):
                continue
            FD_inwards_table_list[list_idx][table_idx].pop(0) #getting rid of header
            integrated_candidate_input_columns.append(FD_inwards_table_list[list_idx][table_idx])'''
        
    #Recursively perform this operationfor each column of the outwards table (other than input column) untill it reaches JoinDistance=MaxJoinDistance
    Ext_ID = input_Ext_ID
    for idx in range(len(integrated_candidate_input_columns)):
        Ext_ID = Ext_ID+1
        Ext_ID = Recursive_Outwards_Extension([integrated_candidate_input_columns[idx]], outward_index_list[idx], Ext_ID, input_Ext_ID, Processed_Tables2)
    
    
    if(len(integrated_candidate_input_columns)==0):
        return  Ext_ID-1
    else:
        return  Ext_ID
        
def Table_Overlap(Input_Table, Processed_Tables):
    max_overlap=0
    input_col=Input_Table[0]
    for table in Processed_Tables:
        overlap_cnt=0
        processed_col=table[0]
        for val1 in input_col:
            for val2 in processed_col:
                if(val1==val2):
                    overlap_cnt+=1
        min_size=min(len(input_col), len(processed_col))
        overlap=overlap_cnt/min_size
        if(overlap>max_overlap):
            max_overlap = overlap
            
    return max_overlap
                    
    #Logger.write(GlobalString)
    
def FD_Outwards_Extension(Input_Table, Outwards_AugmentingCols_With_Keys, QueryName, Col_Overlap_TH, Satifiability_Error_Threshold, FD_Detection_Method, Processed_Tables, input_header="", Vertical_Extensions=False):
    #return [],[],[],Processed_Tables ,[]
    
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
    #Outwards_orig_extracted_col_list, Augmenting_Cols, outwards_stat_dict = Search_For_Augmenting_Columns([Input_Table[0]], Col_Overlap_TH, Satifiability_Error_Threshold, FD_Detection_Method, inwards=0)
    #if(outwards_stat_dict=="Exit"):
        #return Outwards_orig_extracted_col_list,[],[],[], []
    

    
    #print("Augmenting_Columns")
    ##print(Augmenting_Cols)
    print("size of Augmenting_Cols is: "+repr(len(Outwards_AugmentingCols_With_Keys)))
    #Write_Dict_W_Table_To_File(Seed_Tables,"AD_seeds")
    
    
    
    #2- Clustering the EAB_tables extracted from the AD_SeedTables
    #Based on Agglomerative Clustering:
    
    
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
    MergeCluster_Threshold = 0.3
    Aug_Clusters  = Agglomerative_Cluster_for_Augmenting_Cols(Extension_Input_List,Outwards_AugmentingCols_With_Keys, Column_Grouping_Value_Dist_Threshold, MergeCluster_Threshold)
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
    
    if(len(ExtendedTable)>=len(Input_Table)): #If we have an actual extension, then write it to file
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
    
    
    return ExtendedTable, extended_table_units, Processed_Tables
  
def FD_inwards_Extension(Input_Table, Inwards_AugmentingCols_With_Keys, QueryName, Col_Overlap_TH, Satifiability_Error_Threshold, FD_Detection_Method, Processed_Tables, Vertical_Extensions = False):
    #return [], [], Processed_Tables
    
    print("Begin FD_inwards_Extension ...")
    
    
    #1- Searching for the columns that Input  table is functionally dependent on each of them.
   
    #First, Discover All entities in the query's domain, untill no new enity is dicovered.
    '''
    print("Discovering new entities ...")
    l1 = Input_Table[0]
    while True:
        l2  = Discover_Query_Rows([l1], Col_Overlap_TH)
        if(len(set(l1))==len(set(l2))):
            break
        l1 = l2
    print("End of Discovering new entities")
    all_discovered_query_table_rows = l2
    '''

    #orig_extracted_col_list, inwards_FD_cols, inwards_stat_dict = Search_For_Augmenting_Columns(Input_Table, Col_Overlap_TH, Satifiability_Error_Threshold, FD_Detection_Method, inwards=1)
    #print("Search_For_Augmenting_Columns inwards done!")

    print("size of Augmenting_Cols is: "+repr(len(Inwards_AugmentingCols_With_Keys)))
    
    '''for col in inwards_FD_cols:
        print(col)
    #print(inwards_cols_excluded_outwards)
    sys.exit()'''
    candidate_inwards_orig_cols=[]

    

            
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
            
    Extension_Input_List = [unique_input]
    
    MergeCluster_Threshold = 0.5
    Column_Grouping_Value_Dist_Threshold = 0.3
    Inwards_Aug_Clusters  = Agglomerative_Cluster_for_Inwards_Augmenting_Cols(Extension_Input_List,Inwards_AugmentingCols_With_Keys, Column_Grouping_Value_Dist_Threshold, MergeCluster_Threshold)
    
    #we should group each cluster and save each of them 
    extended_table_num=0
    print("len(Inwards_Aug_Clusters):")
    print(str(len(Inwards_Aug_Clusters))+"\n")
    for idx in range(len(Inwards_Aug_Clusters)):
        Extension_Input_List=[]
        header_list = []
        
        for inner_cluster_tuple_idx in range(len(Inwards_Aug_Clusters[idx][0])):
            #populte Extension_Input_List
            header_list.append(Inwards_Aug_Clusters[idx][0][inner_cluster_tuple_idx][0][0][0])
            
        #print(Inwards_Aug_Clusters[idx])
        Extension_Input_List=copy.copy(Inwards_Aug_Clusters[idx][0][0][0])
        Extension_Input_List[0].pop(0)
        ExtendedTable = Grouped_Columns_As_Table(Extension_Input_List, [Inwards_Aug_Clusters[idx]], 0.1)
        
        fuzyGrouped_headers = Octopus_FuzzyGroup(header_list,Column_Grouping_Value_Dist_Threshold)
        #Centroid_header = ""
        clusterVal_list=[]
        if (len(fuzyGrouped_headers )>0):
            #output the canonical value of each cluster
            #clusterVal_list=[]
            for c in fuzyGrouped_headers:
                clusterVal_list.append(c[0][0])
            #Predicted_Value = clusterVal_list
            #Centroid_header = fuzyGrouped_headers[0][0][0]
        ExtendedTable[0][0]= ["atomic-value"] + clusterVal_list
    
        print("\n##### print print(ExtendedTable):"+str(idx)+" #####\n")
        print(ExtendedTable)
        if(len(ExtendedTable)>len(Extension_Input_List)): #If we have an actual extension, then write it to file
            #Printing the output Extended Table into File
            print("Raft!")
            extended_table_num+=1
            outFile= get_Constant("OutputFolder")+"/" + QueryName+"_inwards_"+str(idx) +"_outward.csv"
            Write_Table_To_CSV(ExtendedTable, outFile)
    
    '''#test:
    print("# ... Begin Test ...\n")
    print("print(len(Inwards_Aug_Clusters))")
    print(str(len(Inwards_Aug_Clusters))+"\n")
    for idx in range(len(Inwards_Aug_Clusters)):
        #print(Inwards_Aug_Clusters[idx])
        for colPair in Inwards_Aug_Clusters[idx][0]:
            print("#"+str(idx))
            print(colPair)
            print("\n")
        
        print("\n\n\n")
    

    print("End FD_inwards_Extension ...")
    sys.exit()
    '''
    
    return extended_table_num





def FD_inwards_outwards_Extension(Input_Table, Inwards_AugmentingCols_With_Keys, Outwards_AugmentingCols_With_Keys, QueryName, Col_Overlap_TH, Satifiability_Error_Threshold, FD_Detection_Method, Processed_Tables, Vertical_Extensions = False):
    #return [], [], Processed_Tables
    
    print("Begin FD_inwards_Extension ...")
    
    
    #1- Searching for the columns that Input  table is functionally dependent on each of them.
   
    #First, Discover All entities in the query's domain, untill no new enity is dicovered.
    '''
    print("Discovering new entities ...")
    l1 = Input_Table[0]
    while True:
        l2  = Discover_Query_Rows([l1], Col_Overlap_TH)
        if(len(set(l1))==len(set(l2))):
            break
        l1 = l2
    print("End of Discovering new entities")
    all_discovered_query_table_rows = l2
    '''

    #orig_extracted_col_list, inwards_FD_cols, inwards_stat_dict = Search_For_Augmenting_Columns(Input_Table, Col_Overlap_TH, Satifiability_Error_Threshold, FD_Detection_Method, inwards=1)
    #print("Search_For_Augmenting_Columns inwards done!")

    print("size of Augmenting_Cols is: "+repr(len(Inwards_AugmentingCols_With_Keys)))
    
    '''for col in inwards_FD_cols:
        print(col)
    #print(inwards_cols_excluded_outwards)
    sys.exit()'''
    candidate_inwards_orig_cols=[]

    

            
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
            
    Extension_Input_List = [unique_input]
    
    MergeCluster_Threshold = 0.5
    Column_Grouping_Value_Dist_Threshold = 0.3
    
    num_of_outwards_extensions = Column_Grouping_And_Consolidation("atomic-valued",Extension_Input_List,Outwards_AugmentingCols_With_Keys,0,QueryName, Column_Grouping_Value_Dist_Threshold, MergeCluster_Threshold)
    num_of_inwards_extensions = Column_Grouping_And_Consolidation("set-valued",Extension_Input_List,Inwards_AugmentingCols_With_Keys,num_of_outwards_extensions, QueryName, Column_Grouping_Value_Dist_Threshold, MergeCluster_Threshold)
    
    

    
    return num_of_inwards_extensions + num_of_outwards_extensions



def Column_Grouping_And_Consolidation(Column_Type,Extension_Input_List,Input_AugmentingCols_With_Keys,previous_num_of_tables,QueryName, Column_Grouping_Value_Dist_Threshold, MergeCluster_Threshold):
    Aug_Clusters  = Agglomerative_Cluster_for_Inwards_Augmenting_Cols(Extension_Input_List,Input_AugmentingCols_With_Keys, Column_Grouping_Value_Dist_Threshold, MergeCluster_Threshold)
    
    #we should group each cluster and save each of them 
    extended_table_num=0
    print("len(Inwards_Aug_Clusters):")
    print(str(len(Aug_Clusters))+"\n")
    for idx in range(len(Aug_Clusters)):
        Extension_Input_List=[]
        header_list = []
        
        for inner_cluster_tuple_idx in range(len(Aug_Clusters[idx][0])):
            #populte Extension_Input_List
            header_list.append(Aug_Clusters[idx][0][inner_cluster_tuple_idx][0][0][0])
            
        #print(Inwards_Aug_Clusters[idx])
        Extension_Input_List=copy.copy(Aug_Clusters[idx][0][0][0])
        Extension_Input_List[0].pop(0)
        ExtendedTable = Grouped_Columns_As_Table(Extension_Input_List, [Aug_Clusters[idx]], 0.1)
        
        fuzyGrouped_headers = Octopus_FuzzyGroup(header_list,Column_Grouping_Value_Dist_Threshold)
        #Centroid_header = ""
        clusterVal_list=[]
        if (len(fuzyGrouped_headers )>0):
            #output the canonical value of each cluster
            #clusterVal_list=[]
            for c in fuzyGrouped_headers:
                clusterVal_list.append(c[0][0])
            #Predicted_Value = clusterVal_list
            #Centroid_header = fuzyGrouped_headers[0][0][0]
        
  
        ExtendedTable[0][0]= [Column_Type]+clusterVal_list
    
        print("\n##### print print(ExtendedTable):"+str(idx+previous_num_of_tables)+" #####\n")
        print(ExtendedTable)
        if(len(ExtendedTable)>len(Extension_Input_List)): #If we have an actual extension, then write it to file
            #Printing the output Extended Table into File
            print("Raft!")
            extended_table_num+=1
            outFile= get_Constant("OutputFolder")+"/" + QueryName+"_inwards_"+str(idx+previous_num_of_tables) +"_outward.csv"
            Final_ExtendedTable={}
            Write_Table_To_CSV(ExtendedTable, outFile)
    
    '''#test:
    print("# ... Begin Test ...\n")
    print("print(len(Inwards_Aug_Clusters))")
    print(str(len(Inwards_Aug_Clusters))+"\n")
    for idx in range(len(Inwards_Aug_Clusters)):
        #print(Inwards_Aug_Clusters[idx])
        for colPair in Inwards_Aug_Clusters[idx][0]:
            print("#"+str(idx))
            print(colPair)
            print("\n")
        
        print("\n\n\n")
    

    print("End FD_inwards_Extension ...")
    sys.exit()
    '''
    return extended_table_num

def Agglomerative_Cluster_for_Inwards_Augmenting_Cols(Input_Cols, Augmenting_Cols, Val_Dist_Threshold, MergeCluster_Threshold):
     ####
        #This Function is based on outwards clustering aug clustering, but the diffrence is that 
        #The first column should be clustered, rather than the second column 
    ####

    Final_Clusters=[]
    
    #Initializing Final_Clusters as Clusters of only one col.
    for i in range(0,len(Augmenting_Cols)):
        Final_Clusters.append(([(Augmenting_Cols[i][0][0], Augmenting_Cols[i][0][1], Augmenting_Cols[i][1][0])], Augmenting_Cols[i][1][1], [Augmenting_Cols[i][2]]))
       
    
    print("#Printing the initial clusters:\n")
    cntr=0
    for f in Final_Clusters:
        print("#------------------Initial Cluster #"+str(cntr)+"-----------------------------------#")
        print(f)
        print("\n")
        cntr+=1
    #sys.exit()
    cntr = 0
    while(len(Final_Clusters)>1):
        cntr+=1
        Diff_Dict={}
        print("Beginning of populating Diff_Dict for each pair of columns ...")
        for i in range(len(Final_Clusters)-1):
            for j in range(i+1, len(Final_Clusters)):
                Diff_Dict[(i,j)] = Augmenting_Col_Diff(Final_Clusters[i], Final_Clusters[j], Val_Dist_Threshold, 1)
        
        print("Inwards Diff_Dict:")
        print(Diff_Dict)
        print("End of populating Diff_Dict for each pair of columns")
        #print("Diff_Dict[(39,43)]:"+str(Diff_Dict[(39,43)])) 
        if(not Is_Mergable(Diff_Dict, MergeCluster_Threshold)):
            break
        #MustBeMerged_Tuple = min(Diff_Dict.items(), key=operator.itemgetter(1))[0]
        #print(MustBeMerged_Tuple)
        
        
        
        Final_Clusters = MergeClusters(Final_Clusters, Diff_Dict, MergeCluster_Threshold )
        
        
        print("Round "+str(cntr)+" Finished! ...")
        break
    print("\n\n\n result of Non-Filtered_Final_Clustered: \n\n\n")
    for c in Final_Clusters:
        print(c)
        print("\n\n!!!Next Cluster!!\n\n")
    #sys.exit()
    
    #Now we have to swap the key and attribute for each cluster, since these are inwards column,\
    #and the extended tables regarding each cluster should start with an inwards column
    
    Filtered_Final_Clustered = []
    for idx in range(len(Final_Clusters)):
        #swapping the key and attribute for each cluster:
        Extension_Input_List=[]
        #Convert the cluster format into the format of Filter_Sort function:
        AugmentingCols = []

        for inner_cluster_tuple_idx in range(len(Final_Clusters[idx][0])):
            aug_Col= copy.copy(Final_Clusters[idx][0][inner_cluster_tuple_idx][0])
            keys= copy.copy(Final_Clusters[idx][0][inner_cluster_tuple_idx][2])
            KeyID_To_InputColID_List = copy.copy(Final_Clusters[idx][0][inner_cluster_tuple_idx][1])
            aug_Col_Unit = copy.copy(Final_Clusters[idx][1])
            source_table_list = copy.copy(Final_Clusters[idx][2])
            
            #Final_Clusters[idx][0][inner_cluster_tuple_idx] = ([attrib], Final_Clusters[idx][0][inner_cluster_tuple_idx][1], key[0])
            
            #populte Extension_Input_List
            for val_i in range(len(keys)):
                if(keys[val_i] not in Extension_Input_List and keys[val_i].strip()!="" and val_i !=0):
                    Extension_Input_List.append(keys[val_i])
            
           
            #Structure of AugmentingCols_With_Keys is:((keys, KeyID_To_InputColID_List),(table[idx],WT_Corpus[tableID]["units"][idx]), tableID)
            AugmentingCols.append((([keys], KeyID_To_InputColID_List),(aug_Col[0], aug_Col_Unit),source_table_list[inner_cluster_tuple_idx]))
            
        
        
        #First, Sorting the input columns so that every time, we have the same output result from the clustering:
        sorted_Augmenting_Cols = sorted(AugmentingCols, key=lambda item:(len(item[0][0]), item[0][0][0]), reverse=True)    
    
        
        #Filter the Augmenting Columns on only those rows that match with the input cols and sort based on the input cols
        Extension_Input_List=[Extension_Input_List]
        Filtered_Augmenting_Cols =  Filter_Sort_Augmenting_Cols(Extension_Input_List, sorted_Augmenting_Cols, 0.1)
        filtered_aug_cluster = []
        for c in Filtered_Augmenting_Cols :
            filtered_aug_cluster.append((c[0][0],c[0][1],c[1][0]))
        #Final_Clusters.append(([(Filtered_Augmenting_Cols[i][0][0], Filtered_Augmenting_Cols[i][0][1], Filtered_Augmenting_Cols[i][1][0])],Filtered_Augmenting_Cols[i][1][1], [Filtered_Augmenting_Cols[i][2]]))
        Filtered_Final_Clustered.append((filtered_aug_cluster,aug_Col_Unit, source_table_list))
    print("\n\n\n result of Filtered_Final_Clustered: \n\n\n")
    for c in Filtered_Final_Clustered:
        print(c)
        print("\n\n!!!Next Cluster!!\n\n")
    #sys.exit()
    return Filtered_Final_Clustered


def Search_For_Augmenting_Columns(Input_Table, Col_Overlap_TH, Satifiability_Error_Threshold, FD_Detection_Method, Solution_Type):
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
            
                '''#print("####")
                print("tableID:"+str(tableID))
                #print("keys:"+str(keys))
                #print("KeyID_To_InputColID_List:"+str(KeyID_To_InputColID_List))
                print("Table_Mapping_Dict:"+(str(Table_Mapping_Dict)))
                print("WT_Corpus[tableID][keyColumnIndex]:"+str(WT_Corpus[tableID]["keyColumnIndex"]))
                print("is_nonkey_table:"+str(is_nonkey_table))
                print("keyIDList:"+str(keyIDList))
                if("45593932_0_6869375743660252130" in tableID):
                    sys.exit()'''

            

            outwards_augmenting_col_Idxes, inwards_augmenting_col_Idxes, outwards_rejected_colPairs, inwards_rejected_colPairs, num_of_outwards_No_FD,num_of_inwards_No_FD, num_of_rejected_No_FD, rejected_inwards_co_occured_col_idxes  = Retrieve_Cols_Having_FD_With_Input(table,keyIDList,Satifiability_Error_Threshold, violation_detectability_threshold ,  FD_Detection_Method, keyCol_index, Solution_Type)
            #augmenting_key_colIdxes, augmenting_nonKey_outwards_colIdxes, augmenting_nonKey_coOccurence_colIdxes, outwards_rejected_colPairs, inwards_rejected_colPairs, num_of_outwards_No_FD,num_of_inwards_No_FD, num_of_rejected_No_FD, rejected_inwards_co_occured_col_idxes  = Retrieve_Cols_Having_FD_With_Input(table,keyIDList,Satifiability_Error_Threshold, violation_detectability_threshold ,  FD_Detection_Method, keyCol_index)
            #outwards_augmenting_col_Idxes , inwards_augmenting_col_Idxes
            #, "num_of_cols_extracted_from_nonKey_outwards":0, "num_of_cols_extracted_from_nonKey_coOccurence":0

            #updating outwards_stat_dict and inwards_stat_dict
            if(is_nonkey_table):
                outwards_stat_dict["num_of_nonKey_FD"]+=len(outwards_augmenting_col_Idxes)
                #final_stat_dict["num_of_cols_extracted_from_key"]+=len(augmenting_key_colIdxes)
                if(FD_Detection_Method != "Column_Grouping" ):
                    for idx in outwards_augmenting_col_Idxes:
                        temp_table = [table[keyIDList[0]],table[idx]]
                        outwards_nonKey_cols_cntr += 1
                        outwards_nonKey_cols_file.write(str(outwards_nonKey_cols_cntr)+":\n")
                        outwards_nonKey_cols_file.write(TableContent_To_TabledStr(temp_table))
                    
                    if(len(outwards_augmenting_col_Idxes)>0):
                        outwards_nonKey_cols_file.write("-----\n")
                        outwards_nonKey_cols_file.write("Source Table"+ " ,tableID:"+str(tableID)+":\n")
                        outwards_nonKey_cols_file.write(TableContent_To_TabledStr(table))
                        outwards_nonKey_cols_file.write("-----\n")
                
                
            else:
                #if (is_key_table), then if is was Mannheim, all of the columns in that table (exept the key column) should be extracted
                #which will be saved in Mannheim_cols_with_Keys
                outwards_stat_dict["num_of_Key_FD"]+=len(outwards_augmenting_col_Idxes)
                outwards_key_cols_cntr += 1
                outwards_key_cols_file.write(str(outwards_key_cols_cntr)+", tableID:"+str(tableID)+":\n")
                outwards_key_cols_file.write(TableContent_To_TabledStr(table))
            
            has_inwards_nonKey_FD = 0
            has_inwards_key_FD = 0
            
            for index in inwards_augmenting_col_Idxes :
                temp_table = [table[keyIDList[0]],table[index]]
                colA = temp_table[0]
                colB = temp_table[1]
                nPMI_score = ColumnPair_NPMI(colA, colB, statistics, corpus_size)
                #Creating a column of row_nPMI_score for all rows in the binary table
                nPMI_col = []
                nPMI_additional_info = ()
                for row_idx in range(len(temp_table[0])):
                    row_val_pair = (string_Cleanse(temp_table[0][row_idx]),string_Cleanse(temp_table[1][row_idx]))
                    #row_nPMI_score = Normalized_Row_PMI(row_val_pair, statistics, corpus_size)
                    row_nPMI_score = Normalized_Row_PMI(row_val_pair, statistics, corpus_size)
                    intersect_table_num = Value_Probability(row_val_pair, statistics, corpus_size) * corpus_size
                    first_elem_table_num = Value_Probability(row_val_pair[0], statistics, corpus_size) * corpus_size
                    second_elem_table_num = Value_Probability(row_val_pair[1], statistics, corpus_size) * corpus_size
                    nPMI_additional_info=(row_nPMI_score,intersect_table_num,first_elem_table_num,second_elem_table_num)
                    nPMI_col.append(nPMI_additional_info)
                nPMI_augmented_temp_table = temp_table + [nPMI_col]
                    
                if(index != keyCol_index):
                    has_inwards_nonKey_FD = 1
                    inwards_stat_dict["num_of_nonKey_FD"]+=1
                    inwards_nonKey_cols_cntr+=1
                    inwards_nonKey_cols_file.write(str(inwards_nonKey_cols_cntr)+" ,nPMI_score: "+str(nPMI_score)+"\n")
                    inwards_nonKey_cols_file.write(TableContent_To_TabledStr(nPMI_augmented_temp_table))
                else:
                    has_inwards_key_FD = 1
                    inwards_stat_dict["num_of_Key_FD"]+=1
                    inwards_key_cols_cntr+=1
                    inwards_key_cols_file.write(str(inwards_key_cols_cntr)+" ,nPMI_score: "+str(nPMI_score)+":\n")
                    inwards_key_cols_file.write(TableContent_To_TabledStr(nPMI_augmented_temp_table))
            
            if(has_inwards_nonKey_FD):
                inwards_nonKey_cols_file.write("-----\n")
                inwards_nonKey_cols_file.write("Source Table"+ " ,tableID:"+str(tableID)+":\n")
                inwards_nonKey_cols_file.write(TableContent_To_TabledStr(table))
                inwards_nonKey_cols_file.write("-----\n")
                
            if(has_inwards_key_FD):
                inwards_key_cols_file.write("-----\n")
                inwards_key_cols_file.write("Source Table"+ " ,tableID:"+str(tableID)+":\n")
                inwards_key_cols_file.write(TableContent_To_TabledStr(table))
                inwards_key_cols_file.write("-----\n")
                
                
            for idx in rejected_inwards_co_occured_col_idxes:
                temp_table = [table[keyIDList[0]],table[idx]]
                colA = temp_table[0]
                colB = temp_table[1]
                nPMI_score = ColumnPair_NPMI(colA, colB, statistics, corpus_size)
                #Creating a column of row_nPMI_score for all rows in the binary table
                nPMI_col = []
                nPMI_additional_info = ()
                for row_idx in range(len(temp_table[0])):
                    row_val_pair = (string_Cleanse(temp_table[0][row_idx]),string_Cleanse(temp_table[1][row_idx]))
                    #row_nPMI_score = Normalized_Row_PMI(row_val_pair, statistics, corpus_size)
                    row_nPMI_score = Normalized_Row_PMI(row_val_pair, statistics, corpus_size)
                    intersect_table_num = Value_Probability(row_val_pair, statistics, corpus_size) * corpus_size
                    first_elem_table_num = Value_Probability(row_val_pair[0], statistics, corpus_size) * corpus_size
                    second_elem_table_num = Value_Probability(row_val_pair[1], statistics, corpus_size) * corpus_size
                    nPMI_additional_info=(row_nPMI_score,intersect_table_num,first_elem_table_num,second_elem_table_num)
                    nPMI_col.append(nPMI_additional_info)
                nPMI_augmented_temp_table = temp_table + [nPMI_col]
                inwards_rejected_co_occured_cols_cntr+=1
                inwards_rejected_co_occured_cols_file.write(str(inwards_rejected_co_occured_cols_cntr)+" ,nPMI_score: "+str(nPMI_score)+"\n")
                inwards_rejected_co_occured_cols_file.write(TableContent_To_TabledStr(nPMI_augmented_temp_table))
                
            if(len(rejected_inwards_co_occured_col_idxes)>0):
                inwards_rejected_co_occured_cols_file.write("-----\n")
                inwards_rejected_co_occured_cols_file.write("Source Table"+ " ,tableID:"+str(tableID)+":\n")
                inwards_rejected_co_occured_cols_file.write(TableContent_To_TabledStr(table))
                inwards_rejected_co_occured_cols_file.write("-----\n")
                
                

            if(len(outwards_rejected_colPairs)>0):
                    outwards_stat_dict["rejected_colPairs"] += outwards_rejected_colPairs

                    
            if(len(inwards_rejected_colPairs)>0):
                    inwards_stat_dict["rejected_colPairs"] += inwards_rejected_colPairs
                    
            outwards_stat_dict["num_of_No_FD"] += num_of_outwards_No_FD
            inwards_stat_dict["num_of_No_FD"] += num_of_inwards_No_FD
            inwards_stat_dict["num_of_rejected_No_FD"] += num_of_rejected_No_FD

          
                
            #Adding to  AugmentingCols_With_Keys: ((keys, KeyID_To_InputColID_List),(aug_Col, aug_Col_Unit): Just Like EAB that have the key Plus one Column
            
            for idx in outwards_augmenting_col_Idxes:
                #Checking those columns afterwards
                outwards_augmentingCols_with_keys.append(((keys, KeyID_To_InputColID_List),(table[idx],WT_Corpus[tableID]["units"][idx]), tableID))
                    
            for idx in inwards_augmenting_col_Idxes:
                #Checking those columns afterwards
                inwards_augmentingCols_with_keys.append(((keys, KeyID_To_InputColID_List),(table[idx],WT_Corpus[tableID]["units"][idx]), tableID))
                    
                    
            if(FD_Detection_Method=="Column_Grouping"):
                for row in outwards_rejected_colPairs:
                    outwards_augmentingCols_with_keys.append(((keys, KeyID_To_InputColID_List),(table[row[0]],WT_Corpus[tableID]["units"][row[0]]), "to_be_checked_"+str(tableID)))

                #for row in inwards_rejected_colPairs:
                    #inwards_augmentingCols_with_keys.append(((keys, KeyID_To_InputColID_List),(table[row[0]],WT_Corpus[tableID]["units"][row[0]]), "to_be_checked_"+str(tableID)))

    print("inwards_stat_dict[num_of_nonKey_FD]:")
    print(inwards_stat_dict["num_of_nonKey_FD"])
    if(FD_Detection_Method=="Column_Grouping" ):
        
        #Merging outwards_augmentingCols_with_keys with inwards_augmentingCols_with_keys:
        merged_outwards_inwards_cols_with_keys=[]
        for elem in outwards_augmentingCols_with_keys:
            merged_outwards_inwards_cols_with_keys.append(elem)
        #we are excluding inwards_augmentingCols_with_keys because it makes the violates the satisfiability of an FD.
        for elem in inwards_augmentingCols_with_keys:
            merged_outwards_inwards_cols_with_keys.append(elem)
        
        
        
        #outwards finalize_column_grouping_to_be_checked_cols
        #num_of_cols_before_finalizing = len(outwards_augmentingCols_with_keys)
        outwards_augmentingCols_with_keys, outwards_colGroup_rejected_colPairs  = finalize_column_grouping_to_be_checked_cols(outwards_augmentingCols_with_keys, Satifiability_Error_Threshold, violation_detectability_threshold , 0, merged_outwards_inwards_cols_with_keys)
        #outwards_augmentingCols_with_keys, outwards_colGroup_rejected_colPairs  = finalize_column_grouping_to_be_checked_cols(merged_outwards_inwards_cols_with_keys, Satifiability_Error_Threshold, violation_detectability_threshold , 0)
        #num_of_cols_after_finalizing = len(outwards_augmentingCols_with_keys)
        outwards_stat_dict["num_of_nonKey_FD"]+= (len(outwards_stat_dict["rejected_colPairs"])-len(outwards_colGroup_rejected_colPairs))
        outwards_stat_dict["rejected_colPairs"] = outwards_colGroup_rejected_colPairs
        
        outwards_nonKey_cols_cntr=0
        for i in range(len(outwards_augmentingCols_with_keys)):
            key = copy.copy(outwards_augmentingCols_with_keys[i][0][0][0])
            attrib = copy.copy(outwards_augmentingCols_with_keys[i][1][0])
            temp_table = [key,attrib]
            outwards_nonKey_cols_cntr += 1
            outwards_nonKey_cols_file.write(str(outwards_nonKey_cols_cntr)+":\n")
            outwards_nonKey_cols_file.write(TableContent_To_TabledStr(temp_table))
        
        #if(len(outwards_augmenting_col_Idxes)>0):
            #outwards_nonKey_cols_file.write("-----\n")
            #outwards_nonKey_cols_file.write("Source Table"+ " ,tableID:"+str(tableID)+":\n")
            #outwards_nonKey_cols_file.write(TableContent_To_TabledStr(table))
            #outwards_nonKey_cols_file.write("-----\n")
            
        
        #inwards finalize_column_grouping_to_be_checked_cols

        '''#num_of_cols_before_finalizing = len(inwards_stat_dict["rejected_colPairs"])
        inwards_augmentingCols_with_keys, inwards_colGroup_rejected_colPairs  = finalize_column_grouping_to_be_checked_cols(inwards_augmentingCols_with_keys, Satifiability_Error_Threshold, violation_detectability_threshold , 1, merged_outwards_inwards_cols_with_keys)
        #num_of_cols_after_finalizing = len(inwards_colGroup_rejected_colPairs)
        inwards_stat_dict["num_of_nonKey_FD"]+= (len(inwards_stat_dict["rejected_colPairs"])-len(inwards_colGroup_rejected_colPairs))
        inwards_stat_dict["rejected_colPairs"] = inwards_colGroup_rejected_colPairs'''


    return  outwards_augmentingCols_with_keys ,inwards_augmentingCols_with_keys , outwards_stat_dict, inwards_stat_dict    


def finalize_column_grouping_to_be_checked_cols(AugmentingCols_With_Keys, Satifiability_Error_Threshold, Input_Violation_Chance, Inwards, Merged_Augmenting_Cols):
    #we want to make sure those to be cheked columns pass the violation chance threshold
    
    #initilaizing column pairs without header
    contians_to_be_checked_col=False
    col_pairs=[]
    rejected_colPairs=[]
    #for i in range(len(AugmentingCols_With_Keys)):
    for i in range(len(Merged_Augmenting_Cols)):
        key=[]
        attrib=[]
        if(Inwards==0):
            key = copy.deepcopy(Merged_Augmenting_Cols[i][0][0][0])
            attrib = copy.deepcopy(Merged_Augmenting_Cols[i][1][0])
        else:# if it is inwards search, then key and attribute should be swapped with each other
            attrib = copy.deepcopy(Merged_Augmenting_Cols[i][0][0][0])
            key = copy.deepcopy(Merged_Augmenting_Cols[i][1][0])
        
        key.pop(0)
        attrib.pop(0)
        col_pairs.append((key,attrib))
        
    for i in range(len(AugmentingCols_With_Keys)):
        if(str(AugmentingCols_With_Keys[i][2]).startswith('to_be_checked_')):
            contians_to_be_checked_col=True
    
    if(contians_to_be_checked_col==False):
        return  AugmentingCols_With_Keys,rejected_colPairs
    
    final_AugmentingCols_With_Keys=[]
    

    #Cleansing the col_pairs:
    cleansed_col_pairs=[]
    for colpair in col_pairs:
        key = colpair[0]
        atr = colpair[1]
        for i in range(len(key)):
            key[i]= string_Cleanse(key[i])
            atr[i]= string_Cleanse(atr[i])
        cleansed_col_pairs.append((key,atr))

    for i in range(len(AugmentingCols_With_Keys)):
        cleansed_temp_col_pairs = copy.deepcopy(cleansed_col_pairs)
        if(str(AugmentingCols_With_Keys[i][2]).startswith('to_be_checked_')):
            colpair=([],[])
            if(Inwards==0):
                colpair = copy.deepcopy((AugmentingCols_With_Keys[i][0][0][0],AugmentingCols_With_Keys[i][1][0]))
            else:
                colpair = copy.deepcopy((AugmentingCols_With_Keys[i][1][0],AugmentingCols_With_Keys[i][0][0][0]))

            #cleanse col_pair:
            key = colpair[0]
            atr = colpair[1]
            for key_idx in range(len(key)):
                key[key_idx]= string_Cleanse(key[key_idx])
                atr[key_idx]= string_Cleanse(atr[key_idx])
            cleansed_col_pair = (key,atr)
            #print("i:"+str(i)+"\n")
            #print("len(cleansed_temp_col_pairs):"+str(len(cleansed_temp_col_pairs)))
            cleansed_temp_col_pairs.pop(i) #deleting the colpair from the list of pairs
            grouped_column_table = group_columnPairs(cleansed_col_pair,cleansed_temp_col_pairs, 0.1)
            sat_error = Satisfiability_Error(grouped_column_table, [0], 1)
            if(sat_error<=Satifiability_Error_Threshold):
                voilation_chance = Violation_Chance(grouped_column_table, [0])
                if(voilation_chance >=Input_Violation_Chance):
                    final_AugmentingCols_With_Keys.append((AugmentingCols_With_Keys[i][0],AugmentingCols_With_Keys[i][1], str(AugmentingCols_With_Keys[i][2]).replace("to_be_checked_","")))
                else:
                    rejected_colPairs.append((colpair[0][0],colpair[1][0],["v",voilation_chance]))
            else:
                rejected_colPairs.append((colpair[0][0],colpair[1][0],["e",sat_error]))
                
        else:
            final_AugmentingCols_With_Keys.append(AugmentingCols_With_Keys[i])
                
        
    return final_AugmentingCols_With_Keys, rejected_colPairs
    
def group_columnPairs(cleansed_input_colpair,cleansed_col_pairs, threshold):
    #group those columns with a minimum pair overlap
    print("in!")
    #initialize final table
    final_table=[copy.copy(cleansed_input_colpair[0]), copy.copy(cleansed_input_colpair[1])]
    
    
    for pair in cleansed_col_pairs:
        if(pair == cleansed_input_colpair):
            continue
        min_pair = ()
        max_pair = ()
        
        if(len(cleansed_input_colpair[0])>len(pair[0])):
            min_pair = pair
            max_pair = cleansed_input_colpair
        else:
            min_pair = cleansed_input_colpair
            max_pair = pair
        
        #print("min_pair:"+str(min_pair))
        #print("max_pair:"+str(max_pair))
        #print(len(min_pair[0]))
        #print(len(min_pair[1]))
        #print(len(max_pair[0]))
        #print(len(max_pair[1]))
        
        min_pair[0]
        num_of_ov = 0
        for i in range(len(min_pair[0])):
            for j in range(len(max_pair[0])):
                #print("i:"+str(i))
                #print("j:"+str(j))
                if(min_pair[0][i]==max_pair[0][j] and min_pair[1][i]==max_pair[1][j]):
                    num_of_ov += 1
                    break
    
        ov_ratio = num_of_ov/len(min_pair[0])
        if(ov_ratio > threshold):
            final_table[0]+=pair[0]
            final_table[1]+=pair[1]
    print("done!")
    return final_table
            
def Retrieve_Cols_Having_FD_With_Input(table,keyIDList,Satifiability_Error_Threshold, violation_detectability_threshold ,  FD_Detection_Method, Table_KeyCol_Idx, Solution_Type):
    '''
    #Retrieve Both outwards_augmenting_col_Idxes and  inwards_augmenting_col_Idxes
    #The point here is that each column of the table (excluding the keyIDList) have inwards or outwards or no functional dependency with the keyIDList (in case the satisfiability error is above the threhold).
    #In order to check if a column holds inwards FD or outwards FD with the query, either one with a lower Satisfiability_Error would hold the FD.
    #in case of equal Satisfiability_Error, the column would be assigned to outwards FD.
    '''

    #augmenting_key_colIdxes=[] 
    #augmenting_nonKey_outwards_colIdxes=[] 
    #augmenting_nonKey_coOccurence_colIdxes = []

    outwards_augmenting_col_Idxes=[]
    inwards_augmenting_col_Idxes=[]
    
    outwards_rejected_colPairs = [] 
    inwards_rejected_colPairs  = []
    
    tableIdxs_Set=set()
    for i in range(len(table)):
        tableIdxs_Set.add(i)
    #print("keyIDList: "+str(keyIDList))    
    
    #Non_Key_Col_Idxes meaning that those index(es) that are not mapped to the query column(s)
    Non_Key_Col_Idxes = tableIdxs_Set.difference(set(keyIDList))
    
    #pre-calculating the outwards_violation_chance_cnt
    outwards_violation_chance=0
    if (FD_Detection_Method=="Violation_Detectability" or FD_Detection_Method=="Column_Grouping"):
        outwards_violation_chance = Violation_Chance(table, keyIDList)
        
        
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
        
    num_of_inwards_no_FDs = 0
    num_of_outwards_no_FDs = 0
    num_of_rejected_no_FDs = 0
    
    rejected_inwards_co_occured_col_idxes = []
    
    #collecting all the columns extracted from a table with a key mappde query column (the same as Mannheim)
    #-- it is being handled
    
    #Checking wether the idx is holding inwards FD or outwards FD:
    for idx in Non_Key_Col_Idxes:
        is_FD_outwards=-1
        
        outwards_satisfiability_error = Satisfiability_Error(table, keyIDList, idx)
        inwards_satisfiability_error = Satisfiability_Error(table, [idx], keyIDList[0])
        print("outwards_satisfiability_error: "+str(outwards_satisfiability_error))
        print("inwards_satisfiability_error: "+str(inwards_satisfiability_error))
        print("")
        

        '''#if the table key column is the mapped query column, then accept all of the nin-key columns
        if(Table_KeyCol_Idx in keyIDList):
            is_FD_outwards=1
        else:
            #if (outwards_satisfiability_error <= inwards_satisfiability_error and (outwards_satisfiability_error <= Satifiability_Error_Threshold )):
            if (outwards_satisfiability_error <= Satifiability_Error_Threshold ):
                if(Solution_Type=="2"):
                    continue
                is_FD_outwards=1
            #elif (inwards_satisfiability_error <= outwards_satisfiability_error and inwards_satisfiability_error <= Satifiability_Error_Threshold):
            else:
                if(Solution_Type=="1"):
                    continue
                is_FD_outwards=0
                colA = table[keyIDList[0]]
                colB = table[idx]
                #nPMI_score = "N/A"
                nPMI_score = ColumnPair_NPMI(colA, colB, statistics, corpus_size)
                if(nPMI_score>0.1):  
                    inwards_augmenting_col_Idxes.append(idx)
                else:
                    rejected_inwards_co_occured_col_idxes.append(idx)'''
                    
    #if the table key column is the mapped query column, then accept all of the nin-key columns
    #if(Table_KeyCol_Idx in keyIDList):
    #    is_FD_outwards=1
    #else:
        #if (outwards_satisfiability_error <= inwards_satisfiability_error and (outwards_satisfiability_error <= Satifiability_Error_Threshold )):
        if (outwards_satisfiability_error <= Satifiability_Error_Threshold ):
            if(Solution_Type=="2"):
                continue
            is_FD_outwards=1
        #elif (inwards_satisfiability_error <= outwards_satisfiability_error and inwards_satisfiability_error <= Satifiability_Error_Threshold):
        else:
            if(Solution_Type=="1"):
                continue
            is_FD_outwards=0
            colA = table[keyIDList[0]]
            colB = table[idx]
            #nPMI_score = "N/A"
            nPMI_score = ColumnPair_NPMI(colA, colB, statistics, corpus_size)
            if(nPMI_score>0.1):  
                inwards_augmenting_col_Idxes.append(idx)
            else:
                rejected_inwards_co_occured_col_idxes.append(idx)
            
        #For Testing purpose: Log those ColPairs that have No FD towards each other (is_FD_outwards=-1)
        global no_FD_tbl_cntr
        if(is_FD_outwards==-1 and FD_Detection_Method=="Violation_Merging"):
        #if(is_FD_outwards==-1):
            satisfiability_error_when_violations_merged=0
            if(outwards_satisfiability_error<inwards_satisfiability_error):
                satisfiability_error_when_violations_merged = Satisfiability_Error_When_Violations_Merged(table, keyIDList, idx)
                if(satisfiability_error_when_violations_merged<Satifiability_Error_Threshold):
                    #is_FD_outwards=1
                    #num_of_outwards_no_FDs+=1
                    is_FD_outwards=0
                    num_of_inwards_no_FDs+=1
                else:
                    num_of_rejected_no_FDs+=1
                    
                    
            else:
                satisfiability_error_when_violations_merged = Satisfiability_Error_When_Violations_Merged(table, [idx], keyIDList[0])
                if(satisfiability_error_when_violations_merged<Satifiability_Error_Threshold):
                    is_FD_outwards=0
                    num_of_inwards_no_FDs+=1
                else:
                    num_of_rejected_no_FDs+=1
            
            
            
            if(is_FD_outwards!=-1):
                temp_table = [table[keyIDList[0]],table[idx]]
                no_FD_tbl_cntr+=1
                no_FD_file.write(str(no_FD_tbl_cntr)+":")
                no_FD_file.write(TableContent_To_TabledStr(temp_table))
            
            
        
        if (FD_Detection_Method=="Co-occurence_pruning" and (is_FD_outwards!=-1)):
            
            #only prune if this is a non-key table
            if((is_FD_outwards==1 and Table_KeyCol_Idx not in keyIDList) or (is_FD_outwards==0 and idx!=Table_KeyCol_Idx)):
                colA = table[keyIDList[0]]
                colB = table[idx]
                nPMI_score = ColumnPair_NPMI(colA, colB, statistics, corpus_size)
                if(nPMI_score<=0.1):
                    
                    if(is_FD_outwards==1):
                        rejected_colPair = (idx,table[idx][0], table[keyIDList[0]][0], ["nPMI",nPMI_score])
                        outwards_rejected_colPairs.append(rejected_colPair)
                    elif(is_FD_outwards==0):
                        rejected_colPair = (idx,table[idx][0], table[keyIDList[0]][0], ["nPMI",nPMI_score])
                        inwards_rejected_colPairs.append(rejected_colPair)
                    
                    continue
            

            if(is_FD_outwards==1):
                outwards_augmenting_col_Idxes.append(idx)
            #elif(is_FD_outwards==0):
                #inwards_augmenting_col_Idxes.append(idx)
        
                    
        else:
        # if it is not "Co-occurence_pruning"
            if (is_FD_outwards==1):
    
                if (FD_Detection_Method=="Violation_Detectability" or FD_Detection_Method=="Column_Grouping" ):
                    #Violation Detectability only appllies to non-key-mapped tables
                    if(Table_KeyCol_Idx not in keyIDList):#is_nonkey_table
                        if(outwards_violation_chance<violation_detectability_threshold):
                            rejected_colPair = (idx,table[keyIDList[0]][0],table[idx][0],["v",outwards_violation_chance])
                            outwards_rejected_colPairs.append(rejected_colPair)
                            continue
    
                outwards_augmenting_col_Idxes.append(idx)
                        
            elif (is_FD_outwards==0):
                if (FD_Detection_Method=="Violation_Detectability" or FD_Detection_Method=="Column_Grouping"):
                    #Violation Detectability only appllies to non-key-mapped tables
                    if(idx!=Table_KeyCol_Idx):#is_nonkey_table
                        inwards_violation_chance =  Violation_Chance(table, [idx])
                        if(inwards_violation_chance <violation_detectability_threshold):
                            rejected_colPair = (idx,table[idx][0], table[keyIDList[0]][0], ["v",inwards_violation_chance ])
                            inwards_rejected_colPairs.append(rejected_colPair)
                            continue
    
                #inwards_augmenting_col_Idxes.append(idx)

                    
    return outwards_augmenting_col_Idxes, inwards_augmenting_col_Idxes ,outwards_rejected_colPairs, inwards_rejected_colPairs, num_of_outwards_no_FDs, num_of_inwards_no_FDs, num_of_rejected_no_FDs, rejected_inwards_co_occured_col_idxes
    #return augmenting_key_colIdxes, augmenting_nonKey_outwards_colIdxes, augmenting_nonKey_coOccurence_colIdxes ,outwards_rejected_colPairs, inwards_rejected_colPairs, num_of_outwards_no_FDs, num_of_inwards_no_FDs, num_of_rejected_no_FDs, rejected_inwards_co_occured_col_idxes
    
    

    
    
    
def Agglomerative_Cluster_for_ColPairs(Non_Key_ColPairs, MergeCluster_Threshold, Val_Dist_Threshold):
     ####
        #This Function is used for Agglemorative Clustering for ColPairs.
        #In this sense that in the first step, each Column Pair is a cluster,
        #then those clusters with tuple OV higher than OV_Threshold will be merged with each other
        #Untill there would be no cluster with Col_Diff higher than Threshold

        #Currently this function is doing exact matches!
    ####
    
    #Sorting each ColPair based on their first and and 2nd element respectively
    #sorted_non_key_colpairs = []
    #for i in range(0,len(Non_Key_ColPairs)):
        #sorted_non_key_colpairs.append(sorted(Non_Key_ColPairs[i], key=lambda item:(item[1], item[2]))) 
    
    Final_Clusters=[]
    
    #Initializing Final_Clusters as Clusters of only one col.
    for i in range(0,len(Non_Key_ColPairs)):
        Final_Clusters.append(Non_Key_ColPairs[i])

    cntr = 0
    while(len(Final_Clusters)>1):
        cntr+=1
        Diff_Dict={}
        print("Agglomerative_Cluster_for_ColPairs Beginning of populating Diff_Dict for each pair of columns ...")
        for i in range(len(Final_Clusters)-1):
            for j in range(i+1, len(Final_Clusters)):
                Diff_Dict[(i,j)] = OV_ColPair_Diff(Final_Clusters[i], Final_Clusters[j], Val_Dist_Threshold)
        print("inwards Diff_Dict:")
        print(Diff_Dict)
        print("Agglomerative_Cluster_for_ColPairs End of populating Diff_Dict for each pair of columns")
        #print("Diff_Dict[(39,43)]:"+str(Diff_Dict[(39,43)])) 
        if(not Is_Mergable(Diff_Dict, MergeCluster_Threshold)):
            break
        #MustBeMerged_Tuple = min(Diff_Dict.items(), key=operator.itemgetter(1))[0]
        #print(MustBeMerged_Tuple)
        
        
        
        Final_Clusters = Merge_ColPair_Clusters(Final_Clusters, Diff_Dict, MergeCluster_Threshold )
        
        
        print("Round "+str(cntr)+" Finished! ...")
        break
    
    return Final_Clusters

def Agglomerative_Cluster_for_Single_Columns(Single_Col_List, MergeCluster_Threshold, Val_Dist_Threshold):
    ####
    #This Function is used for Agglomerative Clustering for Single Columns.
    #Based on Agglomerative ColPair
    #Currently this function is doing exact matches!
    #####
    
    #Sorting each ColPair based on their first and and 2nd element respectively
    #sorted_non_key_colpairs = []
    #for i in range(0,len(Non_Key_ColPairs)):
        #sorted_non_key_colpairs.append(sorted(Non_Key_ColPairs[i], key=lambda item:(item[1], item[2]))) 
    
    Final_Clusters=[]
    
    
    #First, Sorting the input columns so that every time, we have the same output result from the clustering:
    sorted_Single_Col_List = sorted(Single_Col_List, key=lambda item:(len(item), item[0]), reverse=True)
    
    
    cnt_UG=0
    print("Ungrouped candidate_input_cols:")
    for col in sorted_Single_Col_List:
        
        print(str(cnt_UG)+"-"+str(col)+"\n")
        cnt_UG+=1
        print("\n\n\n")
    
    #Initializing Final_Clusters as Clusters of only one col with their headers
    for i in range(0,len(sorted_Single_Col_List)):
        Final_Clusters.append((sorted_Single_Col_List[i][1:],[sorted_Single_Col_List[i][0]]))

    cntr = 0
    while(len(Final_Clusters)>1):
        cntr+=1
        Diff_Dict={}
        print("Agglomerative_Cluster_for_Single_Columns Beginning of populating Diff_Dict for each pair of columns ...")
        for i in range(len(Final_Clusters)-1):
            for j in range(i+1, len(Final_Clusters)):
                Diff_Dict[(i,j)] = OV_SingleCol_Diff(Final_Clusters[i], Final_Clusters[j], Val_Dist_Threshold)
        #print(Diff_Dict)
        
        print("Agglomerative_Cluster_for_Single_Columns End of populating Diff_Dict for each pair of columns")
        #print("Diff_Dict[(39,43)]:"+str(Diff_Dict[(39,43)])) 
        if(not Is_Mergable(Diff_Dict, MergeCluster_Threshold)):
            break
        #MustBeMerged_Tuple = min(Diff_Dict.items(), key=operator.itemgetter(1))[0]
        #print(MustBeMerged_Tuple)
        
        
        
        Final_Clusters = Merge_SingleCol_Clusters(Final_Clusters, Diff_Dict, MergeCluster_Threshold )
        
        
        print("Round "+str(cntr)+" Finished! ...")
        break
    
    return Final_Clusters



def OV_SingleCol_Diff(Col_with_header1, Col_with_header2, Val_Dist_Threshold):
    
    ###
    #Calculating the content diff
    
    #Currently based on exact matches!
    ###
    num_of_ov = 0
    col_with_smaller_size=[]
    col_with_larger_size=[]
    if(len(Col_with_header1)>len(Col_with_header2)):
        col_with_larger_size = Col_with_header1
        col_with_smaller_size = Col_with_header2
    else:
        col_with_larger_size = Col_with_header2
        col_with_smaller_size = Col_with_header1
        
    for i in range(len(col_with_smaller_size[0])):
        for j in range(len(col_with_larger_size[0])):
            #if(Char_Level_Edit_Distance(col_with_smaller_size[0][i],col_with_larger_size[0][j])<Val_Dist_Threshold):
            if(col_with_smaller_size[0][i]==col_with_larger_size[0][j] and col_with_larger_size[0][j]!=""):
                num_of_ov += 1
                break
    
    size = len(col_with_smaller_size[0])
    content_diff = 1 - num_of_ov/size
    #print("content_diff :"+str(content_diff ))

    
    #Calculating MinDist Headers
    #Assumih the heaser is in the first row
    MinDist_Headers = 1
    for i in range(len(col_with_smaller_size[1])):
        for j in range(len(col_with_larger_size[1])):
            if(col_with_smaller_size[1][i].strip()=="" or col_with_larger_size[1][j].strip()==""):
                continue
            Dist = Char_Level_Edit_Distance(col_with_smaller_size[1][i],col_with_larger_size[1][j])
            if(Dist< MinDist_Headers):
                MinDist_Headers = Dist
    
    #print("MinDist_Headers :"+str(MinDist_Headers ))
    Harmonic_Mean_Dist = 0
    if(MinDist_Headers+content_diff!=0):
        Harmonic_Mean_Dist = (2*MinDist_Headers*content_diff)/(MinDist_Headers+content_diff)
    
    #print("Harmonic_Mean_Dist:"+str(Harmonic_Mean_Dist))
    return Harmonic_Mean_Dist




def Merge_SingleCol_Clusters(Input_Clusters, Diff_Dict, MergeCluster_Threshold ):
    ####
        # This Functin Merges all Clusters with SingleCol lower than MergeCluster_Threshold (ColPair_Diff values are stored in Diff_Dict)
    ####


    #First off, we have to sort the Diff_Dict to know which clusters have the least Difference with each other:
    sorted_Diffs = sorted(Diff_Dict.items(), key=lambda item:item[0])
    #print(sorted_Diffs)

    #Merging All similar SingleCol into the same bucket:
    buckets=[]
    for i in range(len(sorted_Diffs)):
        (a,b) = sorted_Diffs[i][0] 
        if(Diff_Dict[(a,b)]<= MergeCluster_Threshold):
            a_Added = False
            for bucket in buckets:
                if(a in bucket):
                    a_Added = True
                    break
            b_Added = False
            for bucket in buckets:
                if(b in bucket):
                    b_Added = True
                    break
                
            if(a_Added==False or b_Added==False):
                added=False
                for bucket in buckets:
                    if((a in bucket) or (b in bucket)):
                        bucket.add(a)
                        bucket.add(b)
                        added=True
                        #print(buckets)
                        break
                if(added==False):
                    NewBucket = set()
                    NewBucket.add(a)
                    NewBucket.add(b)
                    buckets.append(NewBucket)
                    #print(buckets)
                
    print(buckets)
    for i in range(len(sorted_Diffs)):
        (a,b) = sorted_Diffs[i][0] 
        if(Diff_Dict[(a,b)]> MergeCluster_Threshold):
            
             Found = False
             for bucket in buckets:
                 if(a in bucket):
                     Found = True
                     break
             if(Found==False):
                 NewBucket = set()
                 NewBucket.add(a)
                 buckets.append(NewBucket)
                 #print(buckets) 
             
             Found = False   
             for bucket in buckets:
                 if(b in bucket):
                     Found = True
                     break
             if(Found==False):
                 NewBucket = set()
                 NewBucket.add(b)
                 buckets.append(NewBucket)
                 #print(buckets) 
                 
                
    print(buckets)         
            
          
    #Re-Creating the clusters based on the buckets:
    Final_Clusters = []
    for bucket in buckets:
        new_singleCol_data = []
        new_singleCol_header = []
        
        for cluster_indx in bucket:

            single_col_data = copy.copy(Input_Clusters[cluster_indx][0])
            single_col_header = copy.copy(Input_Clusters[cluster_indx][1])
            

            if (len(new_singleCol_data)==0):
                new_singleCol_data = single_col_data
                new_singleCol_header = single_col_header
            else:
                new_singleCol_data += single_col_data
                new_singleCol_header += single_col_header

        Final_Clusters.append((new_singleCol_data, new_singleCol_header))
     
    return Final_Clusters

def OV_ColPair_Diff(ColPair1, ColPair2, Val_Dist_Threshold, Type=0):
    
    num_of_not_nulls = 0
    num_of_ov = 0
    
    min_col_pair=copy.copy(ColPair1)
    max_col_pair=copy.copy(ColPair2)
    
    if(len(ColPair1[0])>len(ColPair2[0])):
         min_col_pair=copy.copy(ColPair2)
         max_col_pair=copy.copy(ColPair1)
        
    
    for i in range(len(min_col_pair[0])):
        if(min_col_pair[0][i].strip() != ""):
            num_of_not_nulls+=1
            
        for j in range(len(max_col_pair[0])):
            #print("umad")
            if(min_col_pair[0][i].strip()!="" and min_col_pair[1][i].strip()!="" and max_col_pair[0][j].strip()!="" and max_col_pair[1][j].strip()!="" ):
                #print("Tu umad")
                if(Type==0): # Default, Exact Match
                    if(min_col_pair[0][i]==max_col_pair[0][j]):
                        #num_of_not_nulls+=1
                        if (min_col_pair[1][i]==max_col_pair[1][j]):
                            num_of_ov += 1
                        break
                else:
                    if(Type==1): # Approximate  Match
                        if(string_Cleanse(min_col_pair[0][i])==string_Cleanse(max_col_pair[0][j])):
                            #print((min_col_pair[0][i],min_col_pair[1][i]))
                            #print((max_col_pair[0][j],max_col_pair[1][j]))
                            #print("--")
                            #num_of_not_nulls+=1
                            if (Edit_Distance_Ratio(min_col_pair[1][i],max_col_pair[1][j])<Val_Dist_Threshold):
                                num_of_ov += 1
                                #print("yes")
                            break
    #print((num_of_ov,num_of_not_nulls))
    if num_of_not_nulls!=0:        
        return 1 - num_of_ov/num_of_not_nulls
    else:        
        return 1
        


def Merge_ColPair_Clusters(Input_Clusters, Diff_Dict, MergeCluster_Threshold ):
    ####
        # This Functin Merges all Clusters with ColPair_Diff lower than MergeCluster_Threshold (ColPair_Diff values are stored in Diff_Dict)
    ####


    #First off, we have to sort the Diff_Dict to know which clusters have the least Difference with each other:
    sorted_Diffs = sorted(Diff_Dict.items(), key=lambda item:item[0])
    #print(sorted_Diffs)

    #Merging All similar ColPairs into the same bucket:
    buckets=[]
    for i in range(len(sorted_Diffs)):
        (a,b) = sorted_Diffs[i][0] 
        if(Diff_Dict[(a,b)]<= MergeCluster_Threshold):
            a_Added = False
            for bucket in buckets:
                if(a in bucket):
                    a_Added = True
                    break
            b_Added = False
            for bucket in buckets:
                if(b in bucket):
                    b_Added = True
                    break
                
            if(a_Added==False or b_Added==False):
                added=False
                for bucket in buckets:
                    if((a in bucket) or (b in bucket)):
                        bucket.add(a)
                        bucket.add(b)
                        added=True
                        #print(buckets)
                        break
                if(added==False):
                    NewBucket = set()
                    NewBucket.add(a)
                    NewBucket.add(b)
                    buckets.append(NewBucket)
                    #print(buckets)
                
    print(buckets)
    for i in range(len(sorted_Diffs)):
        (a,b) = sorted_Diffs[i][0] 
        if(Diff_Dict[(a,b)]> MergeCluster_Threshold):
            
             Found = False
             for bucket in buckets:
                 if(a in bucket):
                     Found = True
                     break
             if(Found==False):
                 NewBucket = set()
                 NewBucket.add(a)
                 buckets.append(NewBucket)
                 #print(buckets) 
             
             Found = False   
             for bucket in buckets:
                 if(b in bucket):
                     Found = True
                     break
             if(Found==False):
                 NewBucket = set()
                 NewBucket.add(b)
                 buckets.append(NewBucket)
                 #print(buckets) 
                 
                
    print(buckets)         
            
          
    #Re-Creating the clusters based on the buckets:
    Final_Clusters = []
    for bucket in buckets:
        New_ColPair = []
        
        for cluster_indx in bucket:

            keyCol = copy.copy(Input_Clusters[cluster_indx][0])
            atrCol = copy.copy(Input_Clusters[cluster_indx][1])

            if (len(New_ColPair)==0):
                New_ColPair.append(keyCol)
                New_ColPair.append(atrCol)
            else:
                #getting rid of the header name
                del keyCol[0]
                del atrCol[0]

                New_ColPair[0] += keyCol
                New_ColPair[1] += atrCol



        Final_Clusters.append(New_ColPair)
    
                    
    return Final_Clusters

def Discover_Query_Rows(Input_Table, Col_Overlap_TH):
    '''
        #This function performs the same functionality as Search_For_Augmenting_Columns, except for the functional dependency satisfiability,
        #Plus, returning extra input table rows as well as the previous rows
    '''
    
        
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
                    minsize = min(querySize, (len(WT_Corpus[str(tableID)]["relation"][0]) - 1))
                    DMA_Score = (DMA_MATCH[colID] / minsize)
                    if(tableID=="12193237_0_8699643798888088574"):
                        print("DMA_Score:"+str(DMA_Score))
                    if (DMA_Score >= Col_Overlap_TH):
                        ResultSet.add(tableID)
                        
                        if(tableID not in Table_To_Column_Map ):
                            Table_To_Column_Map[tableID] = {}
                            
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
    #print("Now selecting the columns from those tables which have Functional Dependecy with the overlapping columns:")
    #AugmentingCols_With_Keys=[] #These SubTables consist of the cols which have FD with input cols
    
    #Assuming the input table has only one column
    unclustered_discovered_entities = []
    
    with open(get_Constant("Deduplicated_Merged_Pickle_URL"), 'rb') as f3:
        WT_Corpus = pickle.load(f3)
        for tableID in CommonTables:
            
            table =  WT_Corpus[tableID]["relation"]
            
            #print(table)
            
            #Populating the KeyList IDs(Input Overlapping Columns) + #Populating the Key Mappings to Input Col + #Populating the Keys
            keys=[]
            keyIDList=[]
            KeyID_To_InputColID_List=[]
            
            #print(Table_To_Column_Map[table])
            Table_Mapping_Dict = Table_To_Column_Map[tableID]
            for Col_Idx in Table_Mapping_Dict:
                keyIDList.append(int(Col_Idx))
                keys.append(table[int(Col_Idx)])
                KeyID_To_InputColID_List.append(Table_Mapping_Dict[Col_Idx][0])
                
                #populating discovered_entities
                #Discarding Header Assuming header is in the first row
                for val_idx in range(1,len(table[int(Col_Idx)])):
                    unclustered_discovered_entities.append(table[int(Col_Idx)][val_idx])
                    
    #unclustered_keys_length = len(set(unclustered_discovered_entities))
    #discovered_entities = Octopus_FuzzyGroup_Centroid_List(unclustered_discovered_entities, 0.3)
    #print(discovered_entities)
    #Octopus_FuzzyGroup_Centroid_List(unclustered_discovered_entities, 0.3)
    return  unclustered_discovered_entities

def  Violation_Chance(Input_Table, Determinant_KeyIDList):
    '''
    #violation chance cnt is the number of duplicate key col(or determinant of the FD) over the number of col values
    '''
    violation_chance_cnt=0
    #calculating violation chance by counting number of duplicates in the key col
    processed_key_list=[]
    
    cleansed_input_table=[]
    for col_idx in Determinant_KeyIDList:
        cleansed_col=[]
        for val in Input_Table[col_idx]:
            cleansed_col.append(string_Cleanse(copy.deepcopy(val)))
        cleansed_input_table.append(cleansed_col)
    
    #save the header:
    header=""
    for l in range(len(Determinant_KeyIDList)):
            header+=str(cleansed_input_table[l][0])
    #skip the header by starting from 1
    for i in range(1,len(cleansed_input_table[0])):
        processed_key=""
        for l in range(len(Determinant_KeyIDList)):
            processed_key+=str(cleansed_input_table[l][i])
        if(processed_key not in processed_key_list and processed_key.strip()!="" and processed_key!=header):
            processed_key_list.append(processed_key)
            for j in range(i+1, len(cleansed_input_table[0])):
                to_be_tested_key = ""
                for k in range(len(Determinant_KeyIDList)):
                    to_be_tested_key+=str(cleansed_input_table[k][j])
                if(processed_key==to_be_tested_key):
                    violation_chance_cnt+=1

    col_size = len(cleansed_input_table[0])-1
    violation_chance=0
    if(col_size>0):
        violation_chance = violation_chance_cnt/col_size
        
    return violation_chance

def Satisfiability_Error(Input_Table, Determinant_KeyIDList, Dependent_Idx):
    '''
    #Returns the Satisfiability_Error of the FD: "Determinant_KeyIDList->Dependent_Idx"
    #Satisfiability Erroris calculated in this way that, if the two values in the Dependent_col differs, then their regarding Determinant_col values should differ as well.
    #And if not, it will add up to the violations
    '''
    
    #string_Cleanse the needed columns of Input_Table: columns in Determinant_KeyIDList and Dependent_Idx
    cleansed_inpt_table = copy.deepcopy(Input_Table)
    
    for l in Determinant_KeyIDList:
        for i in range(len(Input_Table[l])):
            cleansed_inpt_table[l][i] = (string_Cleanse(cleansed_inpt_table[l][i]))

    for i in range(len(cleansed_inpt_table[Dependent_Idx])):
        cleansed_inpt_table[Dependent_Idx][i] = (string_Cleanse(cleansed_inpt_table[Dependent_Idx][i]))
    
    violatingRowCnt=0
    violatingKeys=set()
    col = cleansed_inpt_table[Dependent_Idx]




    #assuming headers are in row_index 0
    for j in range(1,len(col)):
        key=""
        for l in Determinant_KeyIDList:
            key+=str(cleansed_inpt_table[l][j])
        
        # If the key of that row is analyzed, then there is no need for that row to be analyzed further
        if ((key) in violatingKeys):
            continue
        
        for k in range(j+1, len(col)):
            #print("in the loop1")
            if((col[j])!=(col[k])):
                #print("in the if1")
                Differ=0
                for l in Determinant_KeyIDList:
                    #Input_Table("ljk:"+str(l)+"-"+str(j)+"-"+str(k)+"-")
                    if((cleansed_inpt_table[l][j])!=(cleansed_inpt_table[l][k])):
                        #print("continue")
                        Differ=1
                    
                    #print(" FD_Holds=0")
                if(Differ==0):
                    if( (key)!=""):
                        violatingRowCnt+=1
                        violatingKeys.add((key))        
    
    col_size = len([val for val in cleansed_inpt_table[Determinant_KeyIDList[0]] if((val)!="")])-1
    Satisfiability_Error = 1
    if(col_size>0):
        Satisfiability_Error = (violatingRowCnt/col_size)
    return Satisfiability_Error 

def Satisfiability_Error_When_Violations_Merged(Input_Table, Determinant_KeyIDList, Dependent_Idx):
    '''
    #Difference between 'Satisfiability_Error_When_Violations_Merged' and 'Satisfiability_Error' is that, first we merge those row-pairs with violation if their NPMI>0, 
    and then, we compute the Satisfiability_Error
    '''
    violatingRowCnt=0
    violatingKeys=set()
    col = Input_Table[Dependent_Idx]
    
    new_determinant_col = []
    new_dependent_col = []
    
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


        #assumin headers are in row_index 0
        for j in range(1,len(col)):
            key=""
            for l in Determinant_KeyIDList:
                key+=str(Input_Table[l][j])
            
            # If the key of that row is analyzed, then there is no need for that row to be analyzed further
            if (key in violatingKeys):
                continue
            
            
            new_dependent_val = col[j]
            new_determinant_val = key
            
            
            for k in range(j+1, len(col)):
                #print("in the loop1")
                if(col[j]!=col[k]):
                    #print("in the if1")
                    Differ=0
                    for l in Determinant_KeyIDList:
                        #Input_Table("ljk:"+str(l)+"-"+str(j)+"-"+str(k)+"-")
                        if(Input_Table[l][j]!=Input_Table[l][k]):
                            
                            #print("continue")
                            Differ=1

                        #print(" FD_Holds=0")
                    if(Differ==0):
                        if( key.strip()!=""):
                            violatingRowCnt+=1
                            violatingKeys.add(key)
                            
                            Tuple = (string_Cleanse(col[k]),string_Cleanse(new_determinant_val))
                            nPMI = Normalized_Row_PMI(Tuple,statistics,corpus_size)
                            if(nPMI>0):
                                new_dependent_val += col[k]
                            else:
                                new_dependent_col.append(new_dependent_val)
                                new_determinant_col.append(new_determinant_val)
                                
                            
            new_dependent_col.append(new_dependent_val)
            new_determinant_col.append(new_determinant_val)
            
        merged_violations_table = [new_determinant_col,new_dependent_col]
        return Satisfiability_Error(merged_violations_table,[0],1)


def Agglomerative_Cluster_for_Augmenting_Cols(Input_Cols, Augmenting_Cols, Val_Dist_Threshold, MergeCluster_Threshold):
     ####
        #This Function is used for Agglemorative Clustering for Columns.
        #In this sense that in the first step, each colum is a cluster,
        #then those clusters with Col_Diff lower than the Threshold will be merged with each other
        #Untill there would be no cluster with Col_Diff lower than Threshold
    ####
    
    
    #First, Sorting the input columns so that every time, we have the same output result from the clustering:
    sorted_Augmenting_Cols = sorted(Augmenting_Cols, key=lambda item:(len(item[0][0]), item[0][0][0]), reverse=True)
    
    #Filter the Augmenting Columns on only those rows that match with the input cols and sort based on the input cols
    Filtered_Augmenting_Cols =  Filter_Sort_Augmenting_Cols(Input_Cols, sorted_Augmenting_Cols, 0.1)
    
    Final_Clusters=[]
    
    #Initializing Final_Clusters as Clusters of only one col.
    for i in range(0,len(Filtered_Augmenting_Cols)):
        Final_Clusters.append(([(Filtered_Augmenting_Cols[i][0][0], Filtered_Augmenting_Cols[i][0][1], Filtered_Augmenting_Cols[i][1][0])],Filtered_Augmenting_Cols[i][1][1], [Filtered_Augmenting_Cols[i][2]]))
        
    '''for f in Final_Clusters:
        print(f)
    sys.exit()'''
    cntr = 0
    while(len(Final_Clusters)>1):
        cntr+=1
        Diff_Dict={}
        print("Beginning of populating Diff_Dict for each pair of columns ...")
        for i in range(len(Final_Clusters)-1):
            for j in range(i+1, len(Final_Clusters)):
                Diff_Dict[(i,j)] = Augmenting_Col_Diff(Final_Clusters[i], Final_Clusters[j], Val_Dist_Threshold)
        #print("Diff_Dict:")
        print(Diff_Dict)
        print("\n")
        print("End of populating Diff_Dict for each pair of columns")
        #print("Diff_Dict[(39,43)]:"+str(Diff_Dict[(39,43)])) 
        if(not Is_Mergable(Diff_Dict, MergeCluster_Threshold)):
            break
        #MustBeMerged_Tuple = min(Diff_Dict.items(), key=operator.itemgetter(1))[0]
        #print(MustBeMerged_Tuple)
        
        
        
        Final_Clusters = MergeClusters(Final_Clusters, Diff_Dict, MergeCluster_Threshold )
        
        
        print("Round "+str(cntr)+" Finished! ...")
        break
    
    return Final_Clusters

def Filter_Sort_Augmenting_Cols(Input_Table, Augmenting_Cols, Val_Dist_Threshold):
    ####
    #This function filters the data of augmenting columns, 
    #selecting only those rows that match with the input col 
    ####
    
    Filtered_Augmenting_Cols=[]
    InputKeys=[]
    for Input_Row in range(0, len(Input_Table[0])):
        keyData=[]
        
        #Initialize Key List for each row
        for col in Input_Table:
            keyData.append(col[Input_Row])
        InputKeys.append(keyData)
            
    #Structure of AugmentingCols_With_Keys is: ((keys, KeyID_To_InputColID_List),(aug_Col, aug_Col_Unit): Just Like EAB that have the key Plus one Column
    for Augmenting_Col in Augmenting_Cols:
        keys = copy.deepcopy(Augmenting_Col[0][0])
        #print(keys)
        #print(keys)
        #for keycol in keys:
            #keycol.pop(0)
        Mapping_to_InputTable = Augmenting_Col[0][1]
        aug_col = copy.deepcopy(Augmenting_Col[1][0])
        #aug_col.pop(0)
            
        Filtered_sorted_aug_col = []
        #appending the header:
        Filtered_sorted_aug_col.append(aug_col[0])
        
        sorted_keys= []
        for j in range(len(keys)):
            col=[]
            #print(keys)
            col.append(keys[j][0])
            #print(InputKeys)
            for key in InputKeys:
                col.append(key[j])
            sorted_keys.append(col)
            
        for key_idx in range(0, len(Input_Table[0])):
            keyData = InputKeys[key_idx]
            extended_list = []
                
            '''#Search in the keys for the best!! keyData:
            min_dist = len(keys)*Val_Dist_Threshold
            min_idx = -1
            equal_list=[]
            for i in range(1, len(keys[0])):
                extended_val = ""
                if(str(aug_col[i]).strip()==""):
                        continue
               
                dist = 0
                for j in range(0,len(keys)):
                    dist += Char_Level_Edit_Distance(keys[j][i], keyData[Mapping_to_InputTable[j]])
                        
                if(dist<=min_dist):
                    min_dist = dist
                    min_idx = i
                    if(dist==0):
                        orig_key_list = []
                        for j in range(len(keys)):
                            orig_key_list.append(keys[j][min_idx])
                        
                        equal_list.append((min_idx, orig_key_list))

            #From those that are hit, pick the one that is the most related!
            if(len(equal_list)>1):
                #print("len(equal_list)>1")
                for (equal_idx,equal_keyList) in equal_list:
                    min_dist = len(Augmenting_Col[0])
                    min_idx = -1
                    dist = 0
                    for k in range(len(equal_keyList)):
                        dist += Char_Level_Edit_Distance(equal_keyList[k],keyData[Mapping_to_InputTable[k]])
                    if(dist<min_dist):
                        min_dist = dist
                        min_idx = equal_idx
                        
            #If there is a match for the keyData:
            if(min_idx!=-1):
                extended_val = aug_col[min_idx]
                keyData = tuple(keyData)
                
                concat_str = ""
                for j in range(len(keys)):
                        concat_str += keys[j][min_idx]+" "
                #print("match: "+str(concat_str)+" AND "+str(keyData))
                #if(concat_str.strip()=="super mario galaxy"):
                    #print(keys)'''
            
            #We want to gather all the values associated to the key
            for i in range(1, len(keys[0])): 
                
                keys_are_the_same = True
                for j in range(0,len(keys)):
                    if (keys[j][i] != keyData[Mapping_to_InputTable[j]]):
                        keys_are_the_same = False
                        break
                if(keys_are_the_same):
                    extended_list.append(aug_col[i])
                    
            
            #Filtered_sorted_aug_col.append(extended_val)
            Filtered_sorted_aug_col.append(extended_list)
        Filtered_Augmenting_Cols.append(((sorted_keys, Augmenting_Col[0][1]),(Filtered_sorted_aug_col,Augmenting_Col[1][1]), Augmenting_Col[2]))
    
    return Filtered_Augmenting_Cols
        
def Augmenting_Col_Diff(cluster1, cluster2, Val_Dist_Threshold, Type=0):
    #####
        #We define Columnn-Diff between 2 clusters as the Harmonic Mean of the distances between Min(Dist(Headers)) and  Min(Dist(Values))
        #However, If the they are from different units, they shouldn't be merged, so in this case, their Diif would be 1.
        #Type=0 is Default
    #####
    
    #Determine their Units, and If the units are NOT the same, then their Diff would be 1:
    if(cluster1[1]!=cluster2[1]):
        return 1
    
    #Calculating MinDist Headers
    #Assumih the heaser is in the first row
    MinDist_Headers = 1
    for Augmenting_Data_Col1 in cluster1[0]:
        #print(Augmenting_Data_Col1)
        if(str(Augmenting_Data_Col1[2][0]).strip()==""):
            continue
        for Augmenting_Data_Col2 in cluster2[0]:
            if(str(Augmenting_Data_Col2[2][0]).strip()==""):
                continue
            Dist = Char_Level_Edit_Distance(str(Augmenting_Data_Col1[2][0]), str(Augmenting_Data_Col2[2][0]))
            if(Dist< MinDist_Headers):
                MinDist_Headers = Dist
                
    #If the columns are of type numeric or date, most probably their value will be similar, so we ignore the value similarity in this case and
    #only return the Header similarity
    if(cluster1[1][0]=="numeric" or cluster1[1][0]=="date"):
        return MinDist_Headers
                    
   
    #Calculating Min_Dist Values
    MinDist_Values = 1
    for Augmenting_Data_Col1 in cluster1[0]:
        for Augmenting_Data_Col2 in cluster2[0]:
            Dist=0
            if(Type==0):
                Dist = Col_Value_Dist(Augmenting_Data_Col1[2], Augmenting_Data_Col2[2], Val_Dist_Threshold)
            else:
                if (Type==1): #Inwards Col Value
                    ColPair1 = (Augmenting_Data_Col1[2], Augmenting_Data_Col1[0][0])
                    ColPair2 = (Augmenting_Data_Col2[2], Augmenting_Data_Col2[0][0])
                    #print("\n Colpairs:")
                    #print(ColPair1)
                    #print(ColPair2)
                    
                    Dist = OV_ColPair_Diff(ColPair1, ColPair2, Val_Dist_Threshold, 1)#1 meaning approx  match
                    #print("Dist:"+str(Dist))
                    #print("\n")
            if(Dist< MinDist_Values):
                MinDist_Values = Dist
    
    
    #Rationale for calcualting Harmonic Mean is that if HEader_Distcance between 2 columns are pretty low, and their Value_Disctance is high,
    #then these The distance between these 2 columns should be low since low-distance in one of the dimensinos suffices.
    
    #Assume Header Does not have effect on the Harmonic mean:
    #MinDist_Headers = 0.3
    Harmonic_Mean_Dist = 0
    if(MinDist_Headers+MinDist_Values!=0):
        Harmonic_Mean_Dist = (2*MinDist_Headers*MinDist_Values)/(MinDist_Headers+MinDist_Values)
    
    #test:
    #Harmonic_Mean_Dist = MinDist_Values
    
    return (Harmonic_Mean_Dist)
    #return MinDist_Values

def Col_Value_Dist(inputCol1, inputCol2, Val_Dist_Threshold):
    ####
        #This Function calculates the Disctance between the values of 2 columns based on OVerlap Distance.
    ####
    
    overlap_cnt=0
    non_null_size = 0
    for i in range(1,len(inputCol1)):
        print("inputCol1[i])"+str(inputCol1[i]))
        print("inputCol2[i]"+str(inputCol2[i]))
        if(inputCol1[i].strip()!="" and inputCol2[i].strip()!=""):
            non_null_size+=1
            if(Edit_Distance_Ratio(inputCol1[i],inputCol2[i])<Val_Dist_Threshold):
                overlap_cnt+=1
    
    overlap_Sim = 0
    if(non_null_size!=0):
        overlap_Sim = overlap_cnt/non_null_size
    
    return 1- overlap_Sim



def MergeClusters(Input_Clusters, Diff_Dict, MergeCluster_Threshold ):
    ####
        # This Functin Merges all Clusters with EAB_Diff lower than MergeCluster_Threshold (EAB_Diff values are stored in Diff_Dict)
    ####


    #First off, we have to sort the Diff_Dict to know which clusters have the least Difference with each other:
    sorted_Diffs = sorted(Diff_Dict.items(), key=lambda item:item[0])
    #print(sorted_Diffs)

    #Merging All similar EAB into the same bucket:
    buckets=[]
    for i in range(len(sorted_Diffs)):
        (a,b) = sorted_Diffs[i][0] 
        if(Diff_Dict[(a,b)]<= MergeCluster_Threshold):
            a_Added = False
            for bucket in buckets:
                if(a in bucket):
                    a_Added = True
                    break
            b_Added = False
            for bucket in buckets:
                if(b in bucket):
                    b_Added = True
                    break
                
            if(a_Added==False or b_Added==False):
                added=False
                for bucket in buckets:
                    if((a in bucket) or (b in bucket)):
                        bucket.add(a)
                        bucket.add(b)
                        added=True
                        #print(buckets)
                        break
                if(added==False):
                    NewBucket = set()
                    NewBucket.add(a)
                    NewBucket.add(b)
                    buckets.append(NewBucket)
                    #print(buckets)
                
    print(buckets)
    for i in range(len(sorted_Diffs)):
        (a,b) = sorted_Diffs[i][0] 
        if(Diff_Dict[(a,b)]> MergeCluster_Threshold):
            
             Found = False
             for bucket in buckets:
                 if(a in bucket):
                     Found = True
                     break
             if(Found==False):
                 NewBucket = set()
                 NewBucket.add(a)
                 buckets.append(NewBucket)
                 #print(buckets) 
             
             Found = False   
             for bucket in buckets:
                 if(b in bucket):
                     Found = True
                     break
             if(Found==False):
                 NewBucket = set()
                 NewBucket.add(b)
                 buckets.append(NewBucket)
                 #print(buckets) 
                 
                
    print(buckets)         
            
          
    #Re-Creating the clusters based on the buckets:
    Final_Clusters = []
    for bucket in buckets:
        New_Cluster = []
        SourceTableIDList = []
        for cluster_indx in bucket:
            for Augmenting_Data_Col in Input_Clusters[cluster_indx][0]:
                New_Cluster.append(Augmenting_Data_Col)
                
            for sourceTableID in Input_Clusters[cluster_indx][2]:
                SourceTableIDList.append(sourceTableID)
                
        Final_Clusters.append((New_Cluster,Input_Clusters[cluster_indx][1], SourceTableIDList))
    
                    
    return Final_Clusters

def Is_Mergable(Diff_Dict, Merge_Threshold):
    for key in Diff_Dict:
        if(Diff_Dict[key]<= Merge_Threshold):
            return True
    
    return False

def Grouped_Columns_As_Table(All_Discovered_Input_Rows, Aug_Clusters, Val_Dist_Threshold):
    ####
        #This Function Groups the Clustered column together and returns an Extended Table
    ####
    
    print("Begin Grouped_Columns_As_Table ...")
    
    #The Final Extended Table
    Extended_Table=[]
    for col in All_Discovered_Input_Rows:
        Extended_Table.append([""]+col)
    
    #print(All_Discovered_Input_Rows)
    #print("\n#########\n")
    #print(Aug_Clusters)
    #Column Extension for each Clustered set:
    for cluster in Aug_Clusters:
        
        #print(cluster)
        
        #Since the key rows in each cluster are sorted,
        #then, we can perform the grouping by just one pass.
        #plus, the input keys are included in the rows.
  
    
        #Aggregated_Col is a dictionary from "QueryKEy" -> Values extracted from the eabs in one cluster.
        Aggregated_Col={}
    

            
        for Augmenting_Col in cluster[0]:

            aug_col = copy.deepcopy(Augmenting_Col[2])
            aug_col.pop(0)
            
            for i in range(len(aug_col)):
            
                #Initialize Key List for each row
                keyData = []
                for col in All_Discovered_Input_Rows:
                    keyData.append(col[i])
                keyData = tuple(keyData)
                    
                

                if(keyData in Aggregated_Col):
                    Aggregated_Col[keyData]+=aug_col[i]
                else:
                    if(aug_col[i]!=[]):
                        Aggregated_Col[keyData] = aug_col[i]

        #print(Aggregated_Col)
        #After creating Aggregated_Col, we fuse the result list 
        Fused_Col = {}
        for key in Aggregated_Col:
            
            
            
            
            '''# Value Concatenation Method:
            valueList=""
            for val in Aggregated_Col[key]:
                valueList+=str(val)+"|"
            valueList = valueList[:-1]
            Fused_Col[key] = valueList'''
           
            
            #Value Fusion Method
            FuzzyGroup = Octopus_FuzzyGroup(Aggregated_Col[key], 0.3) 
            #print("FuzzyGroup :")
            #print(FuzzyGroup )
            Predicted_Value= ""
            if(len(FuzzyGroup)>0):
                clusterVal_list=[]
                
                #output the canonical value of each cluster
                for c in FuzzyGroup:
                    clusterVal_list.append(c[0][0])
            #Predicted_Value= FuzzyGroup[0][0][0]
                Predicted_Value = clusterVal_list
                
            Fused_Col[key] = Predicted_Value
            #print("Predicted_Value:"+str(Predicted_Value))
            
        
        
        #print(Fused_Col)
        #Add_Fused_Column: 
        
        #Concatenated Headers:
        '''Header = ""
        for Augmenting_Col in cluster[0]:
            Header+=str(Augmenting_Col[2][0])+"|"
        Header=Header[:-1]'''
        
        
        #FuzzyGrouped Header 
        headeList = []
        for Augmenting_Col in cluster[0]:
            headeList.append(Augmenting_Col[2][0])
            FuzzyGroup = Octopus_FuzzyGroup(headeList, 0.3) 
            Predicted_Value= []
            if(len(FuzzyGroup)>0):
                #Predicted_Value= FuzzyGroup[0][0][0]
                for c in FuzzyGroup:
                    Predicted_Value.append(c[0][0])
        Header = Predicted_Value
        #print("Predicted Header:"+str(Header))
        
        New_Col=[Header]
        for Input_Row in range(1, len(Extended_Table[0])):
            keyData=[]
            
            #Initialize Key List for each row
            for col in All_Discovered_Input_Rows:
                keyData.append(col[Input_Row-1])
            keyData=tuple(keyData)
            
            if(keyData in Fused_Col):
                New_Col.append(Fused_Col[keyData])
            else:
                New_Col.append("")
                
        Extended_Table.append(New_Col)
        
    #cleanse Extended Table: Delete rows with null key
    
    
    #print("\n\n"+str(Extended_Table)+"\n\n")
    
    null_keys_idx=[]
    for i in range(1,len(Extended_Table[0])):
        if(Extended_Table[0][i] is None):
            continue
        if(Extended_Table[0][i].strip()==""):
            null_keys_idx.append(i)
            
    cntr=0
    for i in null_keys_idx:
        for col in Extended_Table:
            col.pop(i-cntr)
        cntr+=1
            
        
    print("End Grouped_Columns_As_Table")

    
    


    #Delete those rows with coverage lower than threshold
    high_row_coverage_extended_table = []

    for row_idx in range(1, len(Extended_Table[0])):
        non_null_val=0
        row=[]
        for col_idx in range(len(Extended_Table)):
            val = Extended_Table[col_idx][row_idx]
            row.append(val)
            if(str(val).strip()!="" and str(val).strip().lower()!="null"):
                non_null_val += 1
        if(non_null_val/len(Extended_Table)>=0.0001):
            high_row_coverage_extended_table.append(row)



    #Adding first row
    first_row = []
    for i in range(len(Extended_Table)):
        first_row.append(Extended_Table[i][0])

    sorted_high_row_coverage_et = [first_row] + sorted(high_row_coverage_extended_table, key=lambda item:len([x for x in item if str(x).strip()!=""]), reverse=True)

    rebuilt_extended_table = []
    for val in first_row:
        rebuilt_extended_table.append([val])
    for row_idx in range(1, len(sorted_high_row_coverage_et)):
        for col_idx in range(len(sorted_high_row_coverage_et[row_idx])):
            rebuilt_extended_table[col_idx].append(sorted_high_row_coverage_et[row_idx][col_idx]) 


    key_cols = []
    for i in range(len(All_Discovered_Input_Rows)):
        key_cols.append(rebuilt_extended_table[i])
        
    for i in range(len(All_Discovered_Input_Rows)):
        del rebuilt_extended_table[0]

    
    
    #Delete those columns with coverage lower than Threshold:
    high_column_coverage_extended_table = []
    for i in range (len(rebuilt_extended_table)):
        col = rebuilt_extended_table[i]
        non_null_val = 0
        for val in col:
            if(str(val).strip()!="" and str(val).strip().lower()!="null"):
                non_null_val+=1
        if(non_null_val/len(Extended_Table[0])>0.00001):
            high_column_coverage_extended_table.append(col)


    
    #Sort columns based on recall
    Extended_Table_Sorted = key_cols + sorted(high_column_coverage_extended_table, key=lambda item:len([x for x in item if str(x).strip()!=""]), reverse=True)

    #sys.exit()
    return Extended_Table_Sorted
                                                         
def Proposed_Integrate_Inwards_Outwards(Col_Overlap_TH, input_table):
    ######
    #Integrates the Outward tables together, Based on Joinning tables on their FK-PK relations.
    #Col_Overlap_TH is used for inwards integration
    ######
    
    #Final Extension
    integrated_extension=[]
    
    tables_Dict={}
    for file in os.listdir(get_Constant("OutputFolder")):
        
        
        #print(file)
        #print(table)
        
        if("0_outward.csv"==file):
            table = Convert_CSVFile_To_Table(get_Constant("OutputFolder")+"/"+file)
            tables_Dict[-1] = {0:(0,table)}
            
            
        elif("inwards" not in file):
            table = Convert_CSVFile_To_Table(get_Constant("OutputFolder")+"/"+file)
            Ext_ID = int(file.split('(')[0])
            parent_Ext_ID = int(file.split('(')[1].split("_")[0])
            join_col_idx = int(file.split('_')[1].replace(")",""))
            
            
            if(parent_Ext_ID in tables_Dict):
                tables_Dict[parent_Ext_ID].update({Ext_ID:(join_col_idx,table)})
            else:
                tables_Dict[parent_Ext_ID] = {Ext_ID:(join_col_idx,table)}
            
    
    integrated_extension = {}
    parent_ID = 0
    if((parent_ID-1) in tables_Dict):
        if(parent_ID in tables_Dict[parent_ID-1]):
            integrated_extension = tables_Dict[parent_ID-1][parent_ID][1]
    
    #print("####")
    #print(tables_Dict)
    #print("####")
          
    
    if(-1  in tables_Dict):
        integrated_outwards_extension = Recursive_Outwards_Table_Joiner(tables_Dict, integrated_extension, parent_ID) 
        
        #Write to File
        outFile= get_Constant("OutputFolder")+"/integrated_outwards.csv"
        Write_Table_To_CSV(integrated_outwards_extension, outFile)
    else:
        integrated_outwards_extension=[]
        
    
    
    val_diff_threshold = 0.1
    integrated_inwards_outwards_extension = New_Inwards_Table_Joiner_Transformed_to_SetValues(integrated_outwards_extension, input_table, Col_Overlap_TH, val_diff_threshold) 
    
    #Write to File
    integrated_inwards_outwards_csv_file= get_Constant("OutputFolder")+"/integrated_inwards_outwards.csv"
    Write_Table_To_CSV(integrated_inwards_outwards_extension, integrated_inwards_outwards_csv_file)
    
    #Convert to Excel File
    Convert_CSV_TO_XLSX(integrated_inwards_outwards_csv_file)
    Style_Integerated_XLSX(integrated_inwards_outwards_csv_file[:-4]+".xlsx", len(integrated_outwards_extension))
          
    #return integrated_extension, integrated_inwards_outwards_extension
    return integrated_inwards_outwards_extension

def Recursive_Outwards_Table_Joiner(tables_Dict, integrated_extension, parent_ID):
    #Joinning tables on their FK-PK relations:
    
    
    #Exit condition:
    if(parent_ID not in tables_Dict):
        return integrated_extension

    additional_cols=[]
    #parent_table = tables_Dict[parent_ID-1][parent_ID][1]
    for Ext_ID in tables_Dict[parent_ID]:
        
        (t_join_idx, t) = tables_Dict[parent_ID][Ext_ID]
        joinnig_idx=-1
        if(parent_ID==0):
            joinnig_idx+=1
            
        integrated_ext_cursor = joinnig_idx + t_join_idx
        joinning_cols=[]
            
        #Adding the headers of the new cols
        for k in range(1,len(t)):
            joinning_cols.append([t[k][0]])
            
        #Filling the data of the new cols
        for i in range(len(integrated_extension[integrated_ext_cursor])):
            join_key = integrated_extension[integrated_ext_cursor][i]
            
            if(join_key.strip()==""):
                for k in range(1,len(t)):
                    joinning_cols[k-1].append("")
            else:
                    
                for j in range(len(t[0])):
                    if(join_key==t[0][j] and join_key.strip()!="" and str(t[0][j]).strip()!="" ):
                        for k in range(1,len(t)):
                            joinning_cols[k-1].append(t[k][j])
                        break
                
        
        temp_additional_cols = Recursive_Outwards_Table_Joiner(tables_Dict, joinning_cols, Ext_ID)
        for col in temp_additional_cols:
            additional_cols.append(col)
            
    return integrated_extension+additional_cols


def Inwards_Table_Joiner(integrated_extension, input_table, Col_Overlap_TH, Value_Diff_Threshold):
    ####
    #Integrates inwards extensions with the outwards extensions.
    ####
    inwardsIncluded_extension=copy.deepcopy(integrated_extension)
    
    #Number of integrated extended table untill now
    cumulative_col_cnt = len(integrated_extension)
    
    Hashtag="###"
    
    for file in os.listdir(get_Constant("OutputFolder")):
        
        if("inwards" in file): 
            #Add the Hashtag Column between each inwards table
            
                        
            table = Convert_CSVFile_To_Table(get_Constant("OutputFolder")+"/"+file)
            input_col_idx = Find_Overlapped_Col(table, input_table)
            if(input_col_idx==-1):
                print("Raft tu Find_Overlapped_Col(table, input_table)")
                print(file)
                continue
            
            
            HashtagCol=[]
            
            if(len(inwardsIncluded_extension)==0):
                for col in input_table:
                    inwardsIncluded_extension.append(copy.copy(col))
                
            for i in range(len(inwardsIncluded_extension[0])):
                HashtagCol.append(Hashtag)
            inwardsIncluded_extension.append(HashtagCol)
            
            #Number of integrated extended table untill now
            cumulative_col_cnt = len(inwardsIncluded_extension)
            
            
            '''print("\n####TEST####")
            print("input_col_idx: "+str(input_col_idx))
            print("####TEST####\n")'''
            

            #Adding the headers of inwards table to each row of the integrated_extension
            for k in range(len(table)):
                new_col = []
                if (k!=input_col_idx):
                    for i in range(len(inwardsIncluded_extension[0])):
                        if(inwardsIncluded_extension[0][i]!=""):
                            new_col.append(table[k][0])
                        else:
                            new_col.append("")
                            
                    inwardsIncluded_extension.append(new_col)
                        
                
            i=0
            while(i<len(inwardsIncluded_extension[0])-1):
                i+=1
                input_key = inwardsIncluded_extension[0][i]

                if(input_key==""):
                    continue
                match_cnt=0
                #print("file:"+str(file))
                #print("input_col_idx:"+str(input_col_idx))
                #print("input_table:"+str(input_table))
                #print("Col_Overlap_TH:"+str(Col_Overlap_TH))
                
                #If the table cell is a list, then we want to check all elements, to make sure 
                # whether the input_key is inside the cell or not.
                for j in range(len(table[input_col_idx])):
                    table_mapped_key = table[input_col_idx][j]
                    found = False
                    if isinstance(table_mapped_key, list):
                        for val in table_mapped_key:
                            if(Edit_Distance_Ratio(val,input_key)<Value_Diff_Threshold):
                                found = True
                                break
                    else:
                        if(Edit_Distance_Ratio(table_mapped_key,input_key)<Value_Diff_Threshold):
                                found = True
                            
                    if(found):
                        match_cnt+=1
                        
                        #Add a row below if table is full
                        if(i+match_cnt>=len(inwardsIncluded_extension[0])):
                            for k in range(len(inwardsIncluded_extension)):
                                if(inwardsIncluded_extension[k][i+match_cnt-1]!= Hashtag):
                                    inwardsIncluded_extension[k].insert(i+match_cnt, "")
                                else:
                                    inwardsIncluded_extension[k].insert(i+match_cnt, Hashtag)
                                    
                                
                            
                        
                        #Add emtpty row beneath the input key untill needed
                        if(inwardsIncluded_extension[0][i+match_cnt]!=""):
                            for k in range(len(inwardsIncluded_extension)):

                                if k<cumulative_col_cnt:
                                    if(inwardsIncluded_extension[k][i+match_cnt-1]!= Hashtag):
                                        inwardsIncluded_extension[k].insert(i+match_cnt, "")
                                    else:
                                        inwardsIncluded_extension[k].insert(i+match_cnt, Hashtag)
                                        
                                elif(k-(cumulative_col_cnt)>=input_col_idx):
                                    inwardsIncluded_extension[k].insert(i+match_cnt, table[k-cumulative_col_cnt+1][j])
                                else:
                                    inwardsIncluded_extension[k].insert(i+match_cnt, table[k-cumulative_col_cnt][j])
                                    
                        else:
                            for k in range(len(inwardsIncluded_extension)):

                                if k<cumulative_col_cnt:
                                    continue
                                
                                if(k-cumulative_col_cnt>=input_col_idx ):

                                        
                                        inwardsIncluded_extension[k][i+match_cnt] =  table[k-cumulative_col_cnt+1][j]
                                else:
                                    #print("i+match_cnt:"+str(i+match_cnt))
                                    #print("len(inwardsIncluded_extension[k])"+str(len(inwardsIncluded_extension[k])))
                                    inwardsIncluded_extension[k][i+match_cnt] =  table[k-cumulative_col_cnt][j]
                            
        

    #Add Last Hashtag col
    HashtagCol=[]
    for i in range(len(inwardsIncluded_extension[0])):
        HashtagCol.append(Hashtag)
    inwardsIncluded_extension.append(HashtagCol)
    return inwardsIncluded_extension


def New_Inwards_Table_Joiner_Transformed_to_SetValues(integrated_extension, input_table, Col_Overlap_TH, Value_Diff_Threshold):
    ####
    #Integrates inwards extensions with the outwards extensions.
    ####
    inwardsIncluded_extension=copy.deepcopy(integrated_extension)
    
    #Number of integrated extended table untill now
    cumulative_col_cnt = len(integrated_extension)
    
    for file in os.listdir(get_Constant("OutputFolder")):
        
        if("inwards" in file): 
    
            table = Convert_CSVFile_To_Table(get_Constant("OutputFolder")+"/"+file)
            input_col_idx = Find_Overlapped_Col(table, input_table)
            if(input_col_idx==-1):
                #print("Raft tu Find_Overlapped_Col(table, input_table)")
                #print(file)
                continue

            
            if(len(inwardsIncluded_extension)==0):
                for col in input_table:
                    inwardsIncluded_extension.append(copy.copy(col))
            
            #Number of integrated extended table untill now
            cumulative_col_cnt = len(inwardsIncluded_extension)

            #Adding the headers of inwards table to the headers of the integrated_extension
            value_container=[] #value_container is used to store the associated values for each key, in order for fuzzy grouping
            for k in range(len(table)):
                new_col = []
                if (k!=input_col_idx):
                    for i in range(len(inwardsIncluded_extension[0])):
                        if(i==0):
                            new_col.append(table[k][0])
                        else:
                            new_col.append("")
                            
                    inwardsIncluded_extension.append(new_col)
                    
                    #initializing the container column:
                    container_col=copy.copy(new_col)
                    for i in range(len(container_col)):
                        container_col[i]=[]
                    value_container.append(container_col)
                        
                
            i=0
            while(i<len(inwardsIncluded_extension[0])-1):
                i+=1
                input_key = inwardsIncluded_extension[0][i]

                if(input_key==""):
                    continue
                match_cnt=0
                #print("file:"+str(file))
                #print("input_col_idx:"+str(input_col_idx))
                #print("input_table:"+str(input_table))
                #print("Col_Overlap_TH:"+str(Col_Overlap_TH))
                
                #If the table cell is a list, then we want to check all elements, to make sure 
                # whether the input_key is inside the cell or not.
                for j in range(len(table[input_col_idx])):
                    table_mapped_key = table[input_col_idx][j]
                    found = False
                    if isinstance(table_mapped_key, list):
                        for val in table_mapped_key:
                            if(Edit_Distance_Ratio(val,input_key)<Value_Diff_Threshold):
                                found = True
                                break
                    else:
                        if(Edit_Distance_Ratio(table_mapped_key,input_key)<Value_Diff_Threshold):
                                found = True
                            
                    if(found):

                        #populate the value_container with the associated found values for each mapped key
                        for k in range(len(inwardsIncluded_extension)):
                            if k<cumulative_col_cnt:
                                continue

                            if(k-cumulative_col_cnt>=input_col_idx):
                                value_container[k-cumulative_col_cnt][i].append(table[k-cumulative_col_cnt+1][j])
                            else:
                                value_container[k-cumulative_col_cnt][i].append(table[k-cumulative_col_cnt][j])


            #populating the fuzzy-grouped values
            for i in range(len(inwardsIncluded_extension[0])):
                input_key = inwardsIncluded_extension[0][i]
                if(input_key==""):
                    continue
                #calcualting the fuzzy groupings for each [inner]table's column
                for k in range(len(inwardsIncluded_extension)):
                    if k<cumulative_col_cnt:
                        continue

                    fuzzy_gropued_result = Octopus_FuzzyGroup(value_container[k-cumulative_col_cnt][i],Value_Diff_Threshold)
                    
                    #output the canonical value of each cluster
                    clusterVal_list=[]
                    for cluster in fuzzy_gropued_result:
                        clusterVal_list.append(cluster[0][0])
                    
                    inwardsIncluded_extension[k][i] = clusterVal_list

    return inwardsIncluded_extension

def Find_Overlapped_Col(table, input_table):
    ####
    #Find the indx in which that column is the actual input col[For Now, we assume that the input table is just one column]
    # *** For now, we are returning col with the maximum overlap
    ####
    
    #In inwards tables, there may be duplicates in each column
    #So We have to convert table columns to sets
    table_as_Set = []
    for col in table:
        new_set_col=set()
        for elem in col:
            if isinstance(elem, list):
                for val in elem:
                    new_set_col.add(val)
            else:
                new_set_col.add(elem)
                
        table_as_Set.append(new_set_col)
        
            
    colIdx_max=-1
    ov_max=0
    for i in range(len(table)):
        match_cntr=0
        for input_val in input_table[0]:
            for val in table_as_Set[i]:
                if(Char_Level_Edit_Distance(string_Cleanse(val),string_Cleanse(input_val))<0.1):
                    match_cntr+=1
                    break
        #if((match_cntr/len(table_as_Set[i]))>ov_max):
        if((match_cntr/len(input_table[0]))>ov_max):
            colIdx_max =  i
            ov_max = match_cntr/len(input_table[0])
            
    return colIdx_max

def Style_Integerated_XLSX(ExcelFile, outwards_cols_cnt):
    wb = openpyxl.load_workbook(ExcelFile)
    sheet = wb.worksheets[0]
    max_rows = sheet.max_row
    
    Hashtag="###"
    
    
    
    #Bolding First Line Headers:
    for col_idx in range(1, outwards_cols_cnt+1):
        sheet.cell(row=1, column=col_idx).font = openpyxl.styles.Font(bold=True)
        
    #Bolding inwards Headers:
    for row_idx in range(1, sheet.max_row+1):
        for col_idx in range(outwards_cols_cnt+1, sheet.max_column+1):
            if((sheet.cell(row=row_idx, column=1).value)!=None and (sheet.cell(row=row_idx, column=col_idx).value) != Hashtag):
                sheet.cell(row=row_idx, column=col_idx).font = openpyxl.styles.Font(bold=True)
    
    
    

    
    
    '''
    #Merging the cells in cols before outwards_cols_cnt
    for col_idx in range(1, outwards_cols_cnt+1):
        i=2
        while i<=max_rows:
            start_merge_row=i
            while(sheet.cell(row=i+1, column=col_idx).value==None and (i+1)<=max_rows):
                i+=1
                print("Inside - i:"+str(i))
                print("Inside - sheet.max_row:"+str(sheet.max_row))
               
                #print(i)
                #print(sheet.max_row)
            end_merge_row=i
            if(start_merge_row!=end_merge_row):
                print("Merge: "+"str(col_idx)"+str(col_idx)+"start_row: "+str(start_merge_row)+"end_row: "+str(end_merge_row))
                sheet.merge_cells(start_row=start_merge_row, start_column=col_idx, end_row=end_merge_row, end_column=col_idx)
            i+=1
    '''
            
            
    '''
    #Merging the cells in cols after outwards_cols_cnt
    max_column = sheet.max_column
    row_idx=1
    while row_idx <= max_column:
        i = outwards_cols_cnt+1
        while i<=max_column:
            start_merge_col=i
            #i+=1
            while(sheet.cell(row=row_idx, column=i).value==None and (i)<=max_column):
                i+=1

            end_merge_col=i-1
            if(start_merge_col<=end_merge_col):
                #print("Merge: "+"str(col_idx)"+str(col_idx)+"start_row: "+str(start_merge_row)+"end_row: "+str(end_merge_row))
              
                start_merge_row = row_idx
                while(sheet.cell(row=row_idx+1, column=start_merge_col).value==None and (row_idx)<=max_rows):
                    row_idx +=1
    
                end_merge_row=row_idx
                if(start_merge_row>end_merge_row):
                    end_merge_row = start_merge_row
                
                    
                sheet.merge_cells(start_row=start_merge_row, start_column=start_merge_col, end_row=end_merge_row, end_column=end_merge_col)
            i+=1
        row_idx +=1
    '''
    
        
    

    wb.save(ExcelFile)

def tests():
    
    '''Augmenting_Col1=((["Query_Header1"],{1:2, 2:3}),(["Header1", "val1", "val2"],'text'))
    Augmenting_Col2=((["Query_Header2"],{1:2, 2:3}),(["Header2", "val1", "val2"],'text'))
    Augmenting_Col3=((["Query_Header3"],{1:2, 2:3}),(["Header3", "val1", "val2"],'text'))
    
    Augmenting_Cols=[Augmenting_Col1, Augmenting_Col2, Augmenting_Col3]
    
    #cluster2 = [eab3]
    setA=set(['a','b','c'])
    setB=set(['a','b'])
    setC=set(['a','b','c','d'])
    sets = [setA, setB, setC]
    colID="125-1"
    setD = set(colID.split("-")[1])
    TestDict = {1:{2:3}}
    TestDict[1][4]=5
    table=[[1,1,3,4,5],[" ","",6, 7, 8],["x","y","x","x","y"],["1","1","1","1","1"]]
    
    
    CommonTables = set.intersection(*sets)
    #(Retrieve_Cols_Having_FD_With_Input(table, [2,0]))
    QueryKeysExcelFile = "C:/Saeed_Local_Files/Logs/Mapped_to_DBPedia/Queries/Country_Queries_Table_2.xlsx"
    #table=Populate_QueryTable_From_ExcelFile(QueryKeysExcelFile)
    #print((Search_For_Augmenting_Columns(table, 0.1))[0][1])
    #print(table)
    #ClusteredEABs = Agglomerative_Cluster_for_Augmenting_Cols(Augmenting_Cols, 0.3, 0.3)
    #Populating the Keys
    keys=[]
    col1=[1,2,3]
    keys.append(col1)
    #print(keys)
    #print("Retrieve_Cols_Having_FD_With_Input : "+str(Retrieve_Cols_Having_FD_With_Input(table, [2,3], 0.6)))
    print(Char_Level_Edit_Distance("PlayStation 2, Xbox, Windows", "PLAYSTATION 3"))'''
    #QueryKeysExcelFile =  "C:/Saeed_Local_Files/TestDataSets/T2D_233/Company.xlsx"
    #Integrate_Inwards_Outwards(0.1, Populate_QueryTable_From_ExcelFile(QueryKeysExcelFile))
    Input_Table=[["key1", "key2", "key3"]]
    Augmenting_Col1=(([["Query_Header1","key1","hichi","hichi"]],{0:0}),(["Header1", "val1", "hichi", "hichi"],'text'),0)
    Augmenting_Col2=(([["Query_Header2","hichi","key2","hichi"]],{0:0}),(["Header2", "hichi", "val2", "hichi"],'text'),0)
    Augmenting_Col3=(([["Query_Header3","hichi","hichi","key3"]],{0:0}),(["Header3", "hichi", "hichi", "val3"],'text'),0)
    
    Augmenting_Cols=[Augmenting_Col1, Augmenting_Col2, Augmenting_Col3]
    Val_Dist_Threshold = 0.15
    #print(Filter_Sort_Augmenting_Cols(Input_Table, Augmenting_Cols, Val_Dist_Threshold))
    
    test_fuzzy_group_list = ["Iran","Islamic Republic of Iran","USA", "Canada", "USA", "canada", "United Kingdom", "united kingdoms"]
    #print(Octopus_FuzzyGroup_Centroid_List(test_fuzzy_group_list, 0.3) )
    input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/Country2.xlsx"
    column=Populate_QueryTable_From_ExcelFile(input_query_file)[0]
    # Cleansing the Key List + Unique
    CleansedColumnValues = []
    for x in range(len(column)):
        CleansedColumnValues.append((string_Cleanse(column[x]),column[x]))
    
    unique_input=[]
    #Deleting redundant values
    queryKeys = []
    for (cleansed_item, item) in CleansedColumnValues:
        if (cleansed_item not in queryKeys):
            queryKeys.append(cleansed_item)
            unique_input.append(item)
    #print(unique_input)
    #unclustered_discovered_entities = (Discover_Query_Rows([unique_input], 0.3))
    #discovered_entities = Octopus_FuzzyGroup_Centroid_List(unclustered_discovered_entities, 0.3)
    #print(Octopus_FuzzyGroup_Centroid_List(discovered_entities, 0.15))
    
    #Write to File
    #integrated_inwards_outwards_csv_file= get_Constant("OutputFolder")+"/integrated_inwards_outwards.csv"
    #Write_Table_To_CSV(integrated_inwards_outwards_extension, integrated_inwards_outwards_csv_file)
    
    #Convert to Excel File
    #Convert_CSV_TO_XLSX(integrated_inwards_outwards_csv_file)
    #Style_Integerated_XLSX(integrated_inwards_outwards_csv_file[:-4]+".xlsx", 3)
    
    col1=["qwerty","Iran", "America", "Canada", "Bahamas"]
    col2=["asdf","Americas","Angula", "The Bahama"]
    col3=["zxcv","Argentina","skjhsd",""," "]
    
    
    '''lusters = (Agglomerative_Cluster_for_Single_Columns([col1,col2,col3],0.4, 0.3))
    print(clusters)
    for c in clusters:
        print(Octopus_FuzzyGroup_Centroid_List(c[0], 0.3))
        print(Octopus_FuzzyGroup(c[1],0.3)[0][0][0])'''
        
    print(Octopus_FuzzyGroup_Centroid_List(col3,0.3))
    
def run(input_query_file):
    start = time.time()
    

    
    #QueryKeysExcelFile = "C:/Saeed_Local_Files/TestDataSets/T2D_233/Company.xlsx"
    #Integrate_Inwards_Outwards(0.3, Populate_QueryTable_From_ExcelFile(input_query_file)) 
    a, out_stat, in_stat= TableExtension(Populate_QueryTable_From_ExcelFile(input_query_file),0,"Error_Threshold","1+2" ,False)
    #Error_Threshold
    #Violation_Detectability
    #Column_Grouping
    #
    print("\n\n Table Extension Result \n\n")
    print("out_stat:")
    print(out_stat)
    print("\n\n")
    print("in_stat")
    print(in_stat)
    #Create_Baseline_Unclustered(Populate_QueryKeys_From_ExcelFile(QueryKeysExcelFile), QueryKeysExcelFile.split("/")[-1].replace(".xlsx","") )

    
    #tests()
    
    end = time.time()
    elapsed = end - start
    print("elapsed time: "+str(elapsed))


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
            
    
     
    
            
    result_list=[]
    cntr = 0
    while cntr < int(len(corpus_col_list)/10):
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
        
        if(len(queryKeys)>=5 and len(queryKeys)<=100):
            cntr+=1
            queryTable = [queryKeys]
            contributed_column_eval = TableExtension(queryTable,0, False)
            contributed_percentage = 0
            if(contributed_column_eval[0]+contributed_column_eval[1]+contributed_column_eval[2]==0):
                contributed_percentage = 0
            else:
                contributed_percentage = ((contributed_column_eval[1]+contributed_column_eval[2])/(contributed_column_eval[0]+contributed_column_eval[1]+contributed_column_eval[2]))*100


            
            result_list.append((contributed_column_eval, [header]+queryKeys, contributed_percentage))
        
           
    
    #Logging the results:
    #First, let's sort the result based on the contributed_percentage:
    sorted_result_list = sorted(result_list, key=lambda item:(item[2]), reverse=True)
    Mannheim_picks_sum=0
    outwards_contrib_sum=0
    inwards_contrib_sum=0
    contrib_col_perc_sum = 0
    random_contributed_cols_eval_logger = open( "C:/Users/new/Dropbox/1-Uni/Thesis/Meetings/5-Fall 2018/Oct_1/random_contributed_cols_eval.txt", "w", encoding="utf-8")
    cntr=0
    for i in range(len(sorted_result_list)):
        cntr+=1
        contributed_column_eval = sorted_result_list[i][0]
        contibuted_column_percentage = sorted_result_list[i][2]
        input_query = sorted_result_list[i][1]

        random_contributed_cols_eval_logger.write("\n-------------------------------------------------------------------------------------\n")
        random_contributed_cols_eval_logger.write(str(cntr)+"- Query:"+str(input_query))

        random_contributed_cols_eval_logger.write("\n num_of_cols Mannheim(from key-col):"+str(contributed_column_eval[0])+", num_of_cols outwards_FD(from non-key col):"+str(contributed_column_eval[1])+", num_of_cols inwards_FD:"+str(contributed_column_eval[2])+", Columns Contributed:"+"{:.2f}".format(contibuted_column_percentage)+"%  \n")
        
        Mannheim_picks_sum += contributed_column_eval[0]
        outwards_contrib_sum += contributed_column_eval[1]
        inwards_contrib_sum += contributed_column_eval[2]
        contrib_col_perc_sum += contibuted_column_percentage

            

    Mannheim_picks_avg = Mannheim_picks_sum/len(sorted_result_list)
    outwards_contrib_avg =outwards_contrib_sum/len(sorted_result_list)
    inwards_contrib_avg = inwards_contrib_sum/len(sorted_result_list)
    contrib_col_perc_avg =contrib_col_perc_sum/len(sorted_result_list)

    random_contributed_cols_eval_logger.write("\n Mannheim_picks_avg:"+"{:.2f}".format(Mannheim_picks_avg))
    random_contributed_cols_eval_logger.write("\n outwards_contrib_avg:"+"{:.2f}".format(outwards_contrib_avg))
    random_contributed_cols_eval_logger.write("\n inwards_contrib_avg:"+"{:.2f}".format(inwards_contrib_avg))
    random_contributed_cols_eval_logger.write("\n contrib_col_perc_avg:"+"{:.2f}".format(contrib_col_perc_avg)+"% ")
    random_contributed_cols_eval_logger.close()
 
    
    
def test2():
    
    input_colpair=(["key1","k11","k12","k13"],["attrib1","a11","a12","a13"])
    colpair2 =(["key2","k21","k21","k23","k21"],["attrib2","a21","a22","a23","a76"])
    colpair3 =(["key3","k31","k32","k31","k44"],["attrib3","a31","a32","a13","a89"])
    colpair4 =(["key4","k41","k42","k43"],["attrib4","a41","a42","a43"])
    test_table=[colpair2[0],colpair2[1], colpair3[0],colpair3[1]]
    print(Violation_Chance(test_table, [2]))
    
    #print(group_columnPairs(input_colpair,[input_colpair,colpair2, colpair3, colpair4 ], 0.1))
    
def test_Column_Ranking():
    Extended_Columns_Ranking( FD_Outwards_Table, integrated_inwards_outwards_extension)
    
    
if  (__name__ == "__main__"):
    #C:/Saeed_Local_Files/TestDataSets/T2D_233/Film.xlsx
    #C:/Saeed_Local_Files/TestDataSets/Test1/input2.xlsx
    
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/Country2.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/Cities.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/Company4.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/Film2.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/city-test.xlsx"
    #input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/cntries_test_1.xlsx"
    
    
    #input_query_file = "C:/Users/new/Dropbox/1-Uni/Thesis/Meetings/5-Fall 2018/Sep_24/Games/Games_Query.xlsx"
    #run(input_query_file)
    #input_query_file = "C:/Users/new/Dropbox/1-Uni/Thesis/Meetings/5-Fall 2018/Sep_24/Film/Film_Query.xlsx"
    #run(input_query_file)
    #input_query_file = "C:/Users/new/Dropbox/1-Uni/Thesis/Meetings/5-Fall 2018/Sep_24/Company/Company_Query.xlsx"
    #run(input_query_file)
    #input_query_file = "C:/Users/new/Dropbox/1-Uni/Thesis/Meetings/5-Fall 2018/Sep_24/Country/Country_Query.xlsx"
    #run(input_query_file)
    
    
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
    
    
    
    
    
    
    
    #Currency-All
    #"C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/All-Countries.xlsx"
    #"C:/Saeed_Local_Files/TestDataSets/T2D_233/sample_queries/10_Countries.xlsx"
    #test3
    

    #Proposed_Integrate_Inwards_Outwards(0.3, Populate_QueryTable_From_ExcelFile(input_query_file)) 

    
    #test2()
    
    run(input_query_file)
    #no_FD_file.close()
    #Test_Random_Queries()
    #PreProcess()
    #check_key_tagging_betw_mannheim_and_corpus()