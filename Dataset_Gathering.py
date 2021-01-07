'''
This file contains a set of data cleaning functionalities to deal with the webtable corpus.
'''

import json
import pickle
import copy
import  tarfile
import os
import sys
from CommonUtilities import *
from multiprocessing import Process
from multiprocessing import Pool
from WDC_Analysis import *
import random
import csv
import openpyxl
from openpyxl.styles import Font, Fill
import tldextract


def Filter_WebTables_From_EAB_Pickle():
    with open(get_Constant("Old_EAB_Merged_Pickle_URL"), 'rb') as f3:
        dict = pickle.load(f3)
        
        Result={}
        for key in dict:
            word1="team"
            #word2 = "nba"
            if ((word1 in str(dict[key]["title"]).lower()) or (word1 in str(dict[key]["pageTitle"]).lower())):
                #if ((word2 in str(dict[key]["pageTitle"]).lower())):
                        Result[key] = dict[key]

    print("len: "+str(len(Result)))
    with open(get_Constant("EAB_Merged_Pickle_URL"), 'wb') as outfile:
        pickle.dump(Result, outfile)
        
def Filter_HighQuality_EAB():
    with open(get_Constant("Old_EAB_Merged_Json_URL"), 'r') as f3:
        dict = json.load(f3)
        print("len: "+str(len(dict)))
        Result=[]
        for key in dict:
            if(len(key["relation"][0])>4):
                Result.append(key)
                
                
def Filter_Random_HighQuality_N_Tables_EAB(N):
    with open(get_Constant("Old_EAB_Merged_Json_URL"), 'r') as f3:
        dict = json.load(f3)
        Max_Table_Num = (len(dict))
        Result=[]
        Cntr=0
        randNums=[]
        while (Cntr<N):
            random_Inx = int(random.random()*Max_Table_Num)
            if(random_Inx not in randNums):
                randNums.append(random_Inx)
                tempTable = dict[random_Inx]
                if(len(tempTable["relation"][0])>4):
                    Result.append(tempTable)
                    Cntr+=1

    print("len: "+str(len(Result)))
    with open(get_Constant("EAB_Merged_Json_URL"), 'w') as outfile:
        json.dump(Result, outfile)
    JsonToPickle()
    

def Filter_WebTables_From_Json():
    with open(get_Constant("Merged_Json_URL"), 'r') as f1:
        dict=json.load(f1)
        Result = []
        words=["maroon 5", "rihanna", "adele","justin timberlake", "linkin park", "david guetta", "ed sheeran", "justin bieber", "bruno mars", "cold play", "selena gomez", "micheal jackson", "celine dion", "mariah carey"]
        for temp in dict:
            #for temp in iter:
                #print(len(temp))
                if ((temp["hasKeyColumn"]) and (temp["tableOrientation"] == "HORIZONTAL")and (len(temp["relation"][0])>4)):
                    
                    for i in range(len(words)):
                        word = words[i]
                        if ((word in str(temp["pageTitle"]).lower()) or (word in str(temp["title"]).lower())  or (
                        word in str(temp["textBeforeTable"]).lower()) or (
                        word in str(temp["textAfterTable"]).lower())):
                            Result.append(temp)
                            break
                        
                    
                    '''word1="hollywood"
                    word2="director"
                    if ((word1 in str(temp["pageTitle"]).lower()) or (word1 in str(temp["title"]).lower())  or (
                        word1 in str(temp["textBeforeTable"]).lower()) or (
                        word1 in str(temp["textAfterTable"]).lower())):
                        if ((word2 in str(temp["pageTitle"]).lower()) or (word2 in str(temp["title"]).lower()) or (
                            word2 in str(temp["textBeforeTable"]).lower()) or (
                            word2 in str(temp["textAfterTable"]).lower())):
                            Result.append(temp)'''
                            
                                    
                     #   if ((word2 in str(temp["pageTitle"]).lower()) ):

                      #  if ((word2 in str(temp["pageTitle"]).lower()) or (word2 in str(temp["textBeforeTable"]).lower()) or (word2 in str(temp["textAfterTable"]).lower())):

        print(len(Result))
        with open(get_Constant("Filter_Json_URL"), 'w') as outfile:
            json.dump(Result, outfile)

def Fetch_Dataset_from_pickle():
    with open('camera.pickle', 'rb') as f3:
        dict = pickle.load(f3)
        print(len(dict))

def Fetch_Dataset_from_Json():
    with open('C:/Saeed_Local_Files/camera_from_tar00.json', 'r') as f1:
        Result = []
        for iter in f1:
            temp = json.loads(iter)
            Result.append(temp)
        print(len(Result[0]))

def Get_Data_From_Tar(TarFileName):
    print("C:/Saeed_Local_Files/WDC_Data/Untarred_WDC_Data/"+TarFileName+".tar")
    tar = tarfile.open("C:/Saeed_Local_Files/WDC_Data/Untarred_WDC_Data/"+TarFileName+".tar")
    #print(len(tar.getmembers()))
    Result=[]
    for member in tar:
        f = tar.extractfile(member)
        if (member.offset!=0):
            content = f.read()
            temp=json.loads(content.decode("utf-8"))
            word2 = "music"
            word1 = "album"
            if ((word1 in str(temp["pageTitle"]).lower()) or (word1 in str(temp["title"]).lower())  or (
                word1 in str(temp["textBeforeTable"]).lower()) or (
                word1 in str(temp["textAfterTable"]).lower())):
                if ((word2 in str(temp["pageTitle"]).lower()) or (word2 in str(temp["title"]).lower()) or (
                    word2 in str(temp["textBeforeTable"]).lower()) or (
                    word2 in str(temp["textAfterTable"]).lower())):
                    
                    Result.append(temp)


        #print(content)
        #sys.exit()

    with open(get_Constant("Json_Files_Directory")+"/from_tar_"+TarFileName+".json", 'w') as outfile:
        json.dump(Result, outfile)
    tar.close()
    
    
    
    
    
def Get_Data_From_Tar_Filter_By_Query_Keys(TarFileName, QueryKeys, Overlap_Threshold):
    print("E:/WDC_Data/Untarred_WDC_Data/"+TarFileName+".tar")
    tar = tarfile.open("E:/WDC_Data/Untarred_WDC_Data/"+TarFileName+".tar")
    #print(len(tar.getmembers()))
    
    lower_stripped_querykeys=[]
    for val in QueryKeys:
        lower_stripped_querykeys.append(str(val).lower().strip())
    query_len = len(lower_stripped_querykeys)
    
    Result=[]
    file_cnt=0
    for member in tar:
        f = tar.extractfile(member)
        if (member.offset!=0):
            file_cnt+=1
            addable=False
            content = f.read()
            temp=json.loads(content.decode("utf-8"))
            table = temp["relation"]
            
            cardinality=len(table[0])-1
            for col in table:
                hit_cnt=0
                for val in col:
                    if(str(val).lower().strip() in lower_stripped_querykeys):
                        hit_cnt+=1
                minimum=min(cardinality, query_len)
                if(minimum!=0):
                    #if(hit_cnt>0):
                    
                    #a=1
                    
                    if(hit_cnt/minimum>=Overlap_Threshold):
                        addable=True
                        break
                if(addable):
                    break
                            
                            
            if(addable):   
                Result.append(temp)
            
            if(file_cnt%1000==0):
                print("file_cnt: "+str(file_cnt))
                print("len(Result)"+str(len(Result)))


        #print(content)
        #sys.exit()

    with open(get_Constant("Json_Files_Directory")+"/from_tar_"+TarFileName+".json", 'w') as outfile:
    #with open(get_Constant("Json_Files_Directory")+"/Filtered.json", 'w') as outfile:
        print("len(Result)"+str(len(Result)))
        json.dump(Result, outfile)
    tar.close()

'''class myThread (threading.Thread):
   def __init__(self,  filename):
      threading.Thread.__init__(self)
      self.filename = filename
   def run(self):
      print ("Starting " + self.filename)
      Get_Data_From_Tar(self.filename)
      print ("Exiting " + self.filename)'''


def Iterate_All_Tar_Files(QueryKeys, Overlap_Threshold):
    if not os.path.exists('C:/Saeed_Local_Files/'+str(Dataset)):
        os.makedirs('C:/Saeed_Local_Files/'+str(Dataset))
    pool = Pool(processes=8)
    for file in os.listdir("E:/WDC_Data/Untarred_WDC_Data"):
        if file.endswith(".tar"):
            filename = file[:-4]
            filename=filename.replace('.','')
            filename=filename.replace('_', '')
            pool.apply_async(Get_Data_From_Tar_Filter_By_Query_Keys, [filename,QueryKeys, Overlap_Threshold]) 
            
            #p = Process(target=Get_Data_From_Tar, args=(filename,))
            #p.start()
            
           # try:
                
            #    _thread.start_new_thread( Get_Data_From_Tar, (filename, ) )
                
           #except:
            #    print ("Error: unable to start thread")

    pool.close()
    pool.join()

def Integrate_All_ScanData():
    sum=0
    for file in os.listdir("C:/Saeed_Local_Files/camera_json_files"):
        with open("C:/Saeed_Local_Files/camera_json_files/"+file, 'r') as f1:
            dict=json.load(f1)
            cnt=len(dict[0])
            print(cnt)
            sum+=cnt
    print(sum)



def Get_Data_From_Folder():
    dirs = os.listdir("C:/Saeed_Local_Files/0")
    for file in dirs:
        print(file)

def test():
    with open('Sample_EAB.pickle', 'rb') as f3:
        dict1 = pickle.load(f3)
        print(len(dict1))
    with open('../sample.json', 'r') as f1:
        Result = []
        for iter in f1:
            temp = json.loads(iter)
            Result.append(temp)
    print(len(Result))

def Merge_Json_Files():
    Result = []
    sum=0
    for file in os.listdir(get_Constant("Json_Files_Directory")):
            if (file.endswith(".json") and file.startswith("from_tar_")):

                    with open(get_Constant("Json_Files_Directory")+"/"+file, 'r') as f1:
                        print(file)
                        jsonFiles=json.load(f1)
                        for jsonTable in jsonFiles:
                            Result.append(jsonTable)
                        cnt=len(jsonFiles)
                        #print(cnt)
                        sum+=cnt
    print(sum)
    #with open(get_Constant("Merged_Json_URL"),'w') as outfile:
    with open(get_Constant("Json_Files_Directory")+"/Filtered.json", 'w') as outfile:
        json.dump(Result, outfile)
        
        
   
                    
        
def Get_Count_TarFiles():
    sum = 0
    for file in os.listdir("C:/Saeed_Local_Files/WDC_Data/Untarred_WDC_Data"):
        if (file.endswith(".tar") ):
            temp=file
            if (int(temp[:-4])>=49):
                tar = tarfile.open("C:/Saeed_Local_Files/WDC_Data/Untarred_WDC_Data/"+file)
                print(file)
                cnt = len(tar.getmembers())
                print(cnt)
                sum += cnt
    print(sum)

def Insert_Json_To_EAB_Merged_Json_URL(relation):
     
    with open(get_Constant("EAB_Merged_Json_URL"), 'r') as f3:
        dict = json.load(f3)
        a=1
        jsonfile={
            "ID":"-6",
            "relation": relation,
            "pageTitle": "Saeed Page Title",
            "title": "",
            "url": "www.saeed.com",
            "hasHeader": True,
            "headerPosition": "FIRST_ROW",
            "tableType": "RELATION",
            "tableNum": 0,
            "s3Link": "saeed",
            "recordEndOffset": 0,
            "recordOffset": 0,
            "tableOrientation": "HORIZONTAL",
            "TableContextTimeStampBeforeTable": "TimecontextBefor",
            "TableContextTimeStampAfterTable": "TimecontextAfter",
            "lastModified": "ye roozi",
            "textBeforeTable": "ContextBefore",
            "textAfterTable": "ContextAfter",
            "hasKeyColumn": True,
            "keyColumnIndex": 0,
            "headerRowIndex": 0
        }
        dict.append(jsonfile)
        with open(get_Constant("EAB_Merged_Json_URL"), 'w') as outfile:
            json.dump(dict, outfile)



def Filter_HighQuality_Json_Tables():
   Json_tables_List=[]
    
    #Populating Json_tables_List
   with open(get_Constant("Merged_Json_URL"), 'r') as f1:
        dict = json.load(f1)
        
        ID=0
        print(len(dict))

        for temp in dict:
            #print(len(iter))
            #for temp in iter:
                if ((temp["hasKeyColumn"]) and (temp["tableOrientation"] == "HORIZONTAL") and (len(temp["relation"][0])>4)):
                    table = temp
                    table["ID"]=ID
                    Json_tables_List.append(table)
                    
                    
                    ID+=1
                    
        with open(get_Constant("Filter_Json_URL"),'w') as outfile:
            json.dump(Json_tables_List, outfile)
                     

#Deduplicate High Quality Tables
def Deduplicate_High_Quality_Json_Tables():
    Json_tables_List=[]
    
    #Populating Json_tables_List
    with open(get_Constant("Filter_Json_URL"), 'r') as f1:
        dict = json.load(f1)
        
        ID=0
        print(len(dict))
        for temp in dict:
            #print(len(iter))
            #for temp in iter:
                if ((temp["hasKeyColumn"]) and (temp["tableOrientation"] == "HORIZONTAL") and (len(temp["relation"][0])>4)):
                    table = temp
                    table["ID"]=ID
                    Json_tables_List.append(table)
                    
                    
                    ID+=1
                    
    print("start!")
    print("number of Original Tables: "+str(len(Json_tables_List)))
    #Deduplication Process:
    Duplicate_IDs=[]
    cnt=0
    for table1 in Json_tables_List:
        cnt+=1
        print("table1:"+str(table1["ID"])+" cnt: "+str(cnt)+" - Number of Duplicates: "+str(len(Duplicate_IDs)))
        #print(table1["relation"])
        #print(TableContent_To_Str(table1["relation"]))
        if(table1["ID"] not in Duplicate_IDs):
            for table2 in Json_tables_List:
                if(table2["ID"] not in Duplicate_IDs and table2["ID"]!= table1["ID"] ):
                    if(TableContent_To_Str(table1["relation"])==TableContent_To_Str(table2["relation"])):
                        Duplicate_IDs.append(table2["ID"])
                        print(table2["ID"])
    
    print("number of Duplicates are: "+str(len(Duplicate_IDs)))
    
    Result=[]
    
    
    for table in Json_tables_List:
        if(table["ID"] not in Duplicate_IDs):
            Result.append(table)
            
    
    with open(get_Constant("Deduplicated_Merged_Json_URL"),'w') as outfile:
        json.dump(Result, outfile)
        

def Stiching_Json_Tables():
    #####
    #Stitch those tables with the same key from the same Domain(Top-Level-Domain)
    #####

    json_tables_list=[]
    
    #Populating Json_tables_List
    with open(get_Constant("Deduplicated_Merged_Json_URL"), 'r') as f1:
        json_tables_list = json.load(f1)
        
    
    #Embedding the domain of the URL for each json_table
    for json_table in json_tables_list:
        json_table["domain"] = tldextract.extract(json_table["url"])[1]
    
    
    print("start Stitching!")
    table_num_before_stitching = len(json_tables_list)
    print("number of tables before stitching: "+str(table_num_before_stitching))

    #Stitching Process:
    stiched_tables_list = []
    
    stitched_table_index = 0
    while stitched_table_index<len(json_tables_list):

        
        stiched_table = json_tables_list[stitched_table_index]
        
        stitched_table_domain = stiched_table["domain"]
        stiched_table_header_row = 0
        if(stiched_table["hasHeader"]):
            stiched_table_header_row = stiched_table["headerRowIndex"]
        stitched_table_key_idx = stiched_table["keyColumnIndex"]
        stiched_table_key_header = stiched_table["relation"][stitched_table_key_idx][stiched_table_header_row]
        
        if(stiched_table_key_header.strip()==""):
            stiched_tables_list.append(stiched_table)
            stitched_table_index+=1
            continue
            

           
        stiched_table_headers = []
        for i in range(len(stiched_table["relation"])):
            stiched_table_headers.append(stiched_table["relation"][i][stiched_table_header_row])
        sorted_stitched_table_headers = sorted(stiched_table_headers)

        
        if(stiched_table["ID"]==38):
            a=1


        #Iterate through the remaining tables to find all those tables with the same key in one domain
        temp_index=stitched_table_index+1
        while temp_index<len(json_tables_list):

            

            temp_table = json_tables_list[temp_index]
            temp_table_header_row = 0
            if(temp_table["hasHeader"]):
                temp_table_header_row = (temp_table["headerRowIndex"])
            #if(temp_table["domain"]!=stitched_table_domain or (len(temp_table["relation"])!=len(stiched_table["relation"]))):
            temp_table_key_idx = temp_table["keyColumnIndex"]
            temp_table_key_header = temp_table["relation"][temp_table_key_idx][temp_table_header_row]
            


            if(temp_table["domain"]!=stitched_table_domain or temp_table_key_header!=stiched_table_key_header):
                temp_index+=1
                continue



            '''temp_table_headers = []
            for i in range(len(temp_table["relation"])):
                temp_table_headers.append(temp_table["relation"][i][temp_table_header_row])
            sorted_temp_table_headers = sorted(temp_table_headers)

            same=True
            for i in range(len(sorted_stitched_table_headers)):
                if(sorted_stitched_table_headers[i]!=sorted_temp_table_headers[i]):
                    same=False
                    break
            if(same==False):
                temp_index+=1
                continue
            #Here, it means that the key of both tables in the same TLS are the same
            #Now, temp_table should be Stiched into the stiched table

            #Stitching the data
            for i in range(len(stiched_table["relation"])):
                for j in range(len(temp_table["relation"][i])):
                    if(j==temp_table_header_row):
                        continue
                    stiched_table["relation"][i].append(temp_table["relation"][i][j])'''

            #Here, it means that the key of both tables in the same TLS are the same
            #Now, temp_table should be Stiched into the stiched table
            
            if(temp_table["ID"]==41):
                a=1
            
            #Stitching the data
            stiched_table_cardinality = len(stiched_table["relation"][stitched_table_key_idx])
            picked_cols=[]
            for i in range(len(temp_table["relation"])):
                col_found=False
                for j in range(len(stiched_table["relation"])):
                    if(j not in picked_cols and len(stiched_table["relation"][j])==stiched_table_cardinality and stiched_table["relation"][j][stiched_table_header_row]==temp_table["relation"][i][temp_table_header_row] and stiched_table["relation"][j][stiched_table_header_row].strip()!=""):
                        picked_cols.append(j)
                        for k in range(len(temp_table["relation"][i])):
                            if(k==temp_table_header_row):
                                continue
                            stiched_table["relation"][j].append(temp_table["relation"][i][k])
                        col_found = True
                        break
                if(col_found==False):
                    new_col=[]
                    #Fill in the null values except the header
                    for k in range(stiched_table_cardinality):
                        if (k==stiched_table_header_row):
                            new_col.append(temp_table["relation"][i][temp_table_header_row])
                        else:
                            new_col.append("")
                    #After null values, Fill the actual values
                    for k in range(len(temp_table["relation"][i])):
                        if (k==stiched_table_header_row):
                            continue
                        else:
                            new_col.append(temp_table["relation"][i][k])
                    
                    stiched_table["relation"].append(new_col)
                    
    
            #Fill non-mapped columns will nulls
            stitched_table_new_card = len(stiched_table["relation"][stitched_table_key_idx])
            for i in range(len(stiched_table["relation"])):
                if(len(stiched_table["relation"][i])!=stitched_table_new_card):
                    for j in range(len(temp_table["relation"][0])-1):
                        stiched_table["relation"][i].append("")

            
            #Stitching pageTitle
            if(stiched_table["pageTitle"]!=temp_table["pageTitle"]):
                stiched_table["pageTitle"]+="|"+str(temp_table["pageTitle"])

            #Stitching textBeforeTable
            if(stiched_table["textBeforeTable"]!=temp_table["textBeforeTable"]):
                stiched_table["textBeforeTable"]+="|"+str(temp_table["textBeforeTable"])

            #Stitching textAfterTable
            if(stiched_table["textAfterTable"]!=temp_table["textAfterTable"]):
                stiched_table["textAfterTable"]+="|"+str(temp_table["textAfterTable"])

            #deleting the temp table
            del json_tables_list[temp_index]

        stiched_tables_list.append(stiched_table)
        stitched_table_index+=1


    print("number of Stitched Tables are: "+str(len(stiched_tables_list)))
   
    with open(get_Constant("Stitched_Json_URL"),'w') as outfile:
        json.dump(stiched_tables_list, outfile)
    
    
   
def Deduplicated_Merged_Json_To_Pickle():
    with open(get_Constant("Deduplicated_Merged_Json_URL"), 'r') as f3:
        dict={}
        JsonCollection = json.load(f3)
        print("len Json: "+str(len(JsonCollection)))
        for obj in JsonCollection:
            if obj["ID"] in dict:
                print(obj["ID"])
            if obj["ID"] =="41":
                print(obj["ID"])

            dict[str(obj["ID"])]=obj
        print("len dict: "+str(len(dict)))
        with open(get_Constant("Deduplicated_Merged_Pickle_URL"), 'wb') as outfile:
            pickle.dump(dict, outfile)
            print("len pickle: "+str(len(dict)))
            print("JsonToPickle Finished")
            
def datachunk_name_cleanse():
    for file in os.listdir(get_Constant("Json_Files_Directory")):
            if (file.endswith(".json") and ("from_tar" in file)):
                    expectedFileName = file[-15:]
                    expectedFileName = expectedFileName[:8] + '_' + expectedFileName[-7:]
                    print(expectedFileName)
                    os.rename(get_Constant("Json_Files_Directory")+"/"+file, get_Constant("Json_Files_Directory")+"/"+expectedFileName )
                    
                    
def Unmerge_MergedJson(MergedJsonURL, OutputFolder):
    print("hey")
    with open(MergedJsonURL, 'r') as f3:
            JsonCollection = json.load(f3)
            
            Cntr=0
            for obj in JsonCollection:
                #print(obj)
                OutputFile = OutputFolder+"/"+str(obj["ID"])+".json"
                with open(OutputFile, 'w') as outfile:
                    json.dump(obj, outfile)
                Cntr+=1
                

def Deduplicated_Merged_Json_To_CSV(TargetFolder):
    print("JsonToCSV Started") 
    with open(get_Constant("Deduplicated_Merged_Json_URL"), 'r') as f3:
        dict={}
        JsonCollection = json.load(f3)
        print("len Json: "+str(len(JsonCollection)))
        for obj in JsonCollection:
            table = obj["relation"]
            ID=obj["ID"]
            with open(TargetFolder+"/"+str(ID)+".csv", "w",encoding='utf8',newline='') as csvFile:
                writer = csv.writer(csvFile, quoting=csv.QUOTE_ALL)
                for i in range(len(table[0])):
                    row=[]
                    for j in range(len(table)):
                        row.append(table[j][i])
                    #print(row)
                    writer.writerow(row)
        
    print("JsonToCSV Finished")   

def T2D_ClassMap_Attrib_Map(ClassMap_CSV_File, Attribute_Map_Folder):#Populating ClassMap Dictionary: Mapping betw. TableName -> its assigned Class
    ClassMap_Dict={}
    #wb = openpyxl.load_workbook(ClassMap_CSV_File)
    #sheet = wb.get_sheet_by_name(ClassMap_CSV_SheetName)
    with open(ClassMap_CSV_File) as ClassCSVFile:
        csv_Content = csv.reader(ClassCSVFile)
        for row in csv_Content:
            Table_Raw_Name = row[0]
            Table_Name = Table_Raw_Name.replace(".tar.gz","")
            ClassMap_Dict[Table_Name] = row[1]
        
    #Populating AttribMap Dictionary: Mapping betw. Table Name -> its Columns Properties.
    AttribMap_Dict={}
    for file in os.listdir(Attribute_Map_Folder):
        Table_Raw_Name = file
        Table_Name = Table_Raw_Name.replace(".csv","")
        with open(Attribute_Map_Folder+"/"+file) as AttribCSVFile:
            csv_Content = csv.reader(AttribCSVFile)
            Attrib_List=[]
            for row in csv_Content:
                Attrib = row[0]
                if ("http://dbpedia.org/ontology/" in Attrib):
                    Attrib = Attrib.replace("http://dbpedia.org/ontology/","")
                if ("http://www.w3.org/2000/01/" in Attrib):
                    Attrib = Attrib.replace("http://www.w3.org/2000/01/","")
                Attrib_List.append(Attrib)
                
            AttribMap_Dict[Table_Name] = Attrib_List
            
    
    #Returning Dict Array: First element is Class_Map, Second element is Attrib_Map
    Dict_Arr=[]
    Dict_Arr.append(ClassMap_Dict)
    Dict_Arr.append(AttribMap_Dict)
    return Dict_Arr



def Analyze_T2D_Gold_Standard():
    #Parameter Initialization:
    ClassMap_CSV_File = "C:/Users/new/Downloads/classes_instance_233.csv"
    #ClassMap_CSV_SheetName = 'classes_instance_233'
    Attribute_Map_Folder = "C:/Users/new/Downloads/attributes_instance_233"
    OutputCSVFile="C:/Users/new/Dropbox/1-Uni/Thesis/Reports/T2D_Analysis_233.csv"
    #OutputExcelSheetName="T2D_Analysis"
    
    #Setting ClassMap_Dict and AttribMap_Dict
    Dict_Arr = T2D_ClassMap_Attrib_Map(ClassMap_CSV_File, Attribute_Map_Folder)
    ClassMap_Dict = Dict_Arr[0]
    AttribMap_Dict = Dict_Arr[1]
                
    #Creating the Final Analysis Excel File    
    with open(OutputCSVFile, "w",encoding='utf8',newline='') as csvFile:
        writer = csv.writer(csvFile, quoting=csv.QUOTE_ALL) 
        for TableName in ClassMap_Dict:
            for Attrib in AttribMap_Dict[TableName]:
                row=[]
                row.append(TableName)
                row.append(ClassMap_Dict[TableName])
                row.append(Attrib)
                row.append(1)
                writer.writerow(row)
                
def T2D_GoldStandard_Related_Columns(Dataset):
        #Parameter Initialization:
    ClassMap_CSV_File = "C:/Users/new/Downloads/classes_instance_233.csv"
    #ClassMap_CSV_SheetName = 'classes_instance_233'
    Attribute_Map_Folder = "C:/Users/new/Downloads/attributes_instance_233"

    
    #Setting ClassMap_Dict and AttribMap_Dict
    Dict_Arr = T2D_ClassMap_Attrib_Map(ClassMap_CSV_File, Attribute_Map_Folder)
    ClassMap_Dict = Dict_Arr[0]
    AttribMap_Dict = Dict_Arr[1]
                
    #Outputting All Attribute Related to the input Dataset:
    #Meaning Those Attributes in Tables that are Assignde To Class of the input Dataset
    Related_Attributes=set()
    for TableName in ClassMap_Dict:
        for Attrib in AttribMap_Dict[TableName]:
            if(ClassMap_Dict[TableName]==Dataset):
                if(Attrib!="rdf-schema#label"):
                   #Related_Attributes.add("http://dbpedia.org/ontology/"+Attrib)
                   Related_Attributes.add(string_Cleanse(Attrib))
                   
    return(Related_Attributes)
    
    
    
def T2D_GoldStandard_Related_Tables(Dataset):
        #Parameter Initialization:
    ClassMap_CSV_File = "C:/Users/new/Downloads/classes_instance_233.csv"
    #ClassMap_CSV_SheetName = 'classes_instance_233'
    Attribute_Map_Folder = "C:/Users/new/Downloads/attributes_instance_233"

    
    #Setting ClassMap_Dict and AttribMap_Dict
    Dict_Arr = T2D_ClassMap_Attrib_Map(ClassMap_CSV_File, Attribute_Map_Folder)
    ClassMap_Dict = Dict_Arr[0]
                
    #Outputting All Tables Related to the input Dataset:
    
    Related_Tables=set()
    for TableName in ClassMap_Dict:
        
            if(ClassMap_Dict[TableName]==Dataset):
                
                   Related_Tables.add(TableName)
                   
    return(Related_Tables)
    
                
def T2D_Merge_Json_Files():
    Result = []
    sum=0
    for file in os.listdir(get_Constant("Json_Files_Directory")+"/table_instances_233_json"):
            if (file.endswith(".json")):
                    with open(get_Constant("Json_Files_Directory")+"/table_instances_233_json"+"/"+file, 'r') as f1:
                        print(file)
                        cnt=1#len(jsonFile)
                        sum+=cnt
                        jsonFile=json.load(f1)
                        jsonFile["ID"]=file.replace(".json","")
                        #for jsonTable in jsonFiles:
                        Result.append(jsonFile)
                        
                        #print(cnt)
                        
    print(sum)
    with open(get_Constant("Merged_Json_URL"),'w') as outfile:
        json.dump(Result, outfile)  
def Dataset_Other_Stats():
    f1 = open("C:/Saeed_Local_Files/TestDataSets/T2D_233/T2D_233_All.txt", 'r', encoding="utf8")
    set1=set()
    statdict = {}
    for l in f1.readlines():
        if ("Labelled Class:" in l):
            class1 = (l.split()[-1])
            if(class1 in statdict):
                statdict[class1]+=1
            else:
                statdict[class1]=1
    total = 0 
    for e in statdict:
        print(e,statdict[e])
        total+=statdict[e]
        
    print("total:",total)
    
    

if (__name__ == "__main__"):
    #Fetch_Dataset()
    #Filter_WebTables_From_Json()
    #Filter_WebTables_From_EAB_Pickle()
    #Fetch_Dataset_from_Json()
    #Get_Data_From_Tar()
    #Get_Data_From_Folder()
    #test()
    
    input_query_file = "C:/Saeed_Local_Files/TestDataSets/T2D_233/cities.xlsx"
    #Iterate_All_Tar_Files(Populate_QueryTable_From_ExcelFile(input_query_file), 0.3)
    #Get_Data_From_Tar_Filter_By_Query_Keys("15",Populate_QueryTable_From_ExcelFile(input_query_file)[0], 0.3)
    #Merge_Json_Files()
    #Get_Count_TarFiles()
    #Merge_Json_Files()
    #Integrate_All_ScanData()
    #print("! This os Windows!!!")
    #Filter_HighQuality_EAB()
    #Filter_Random_HighQuality_N_Tables_EAB(200)
    #Merge_Json_Files()
    #datachunk_name_cleanse()
    #Deduplicate_High_Quality_Json_Tables()
    #Stiching_Json_Tables()
    #Unmerge_MergedJson(get_Constant("Stitched_Json_URL"), get_Constant("JsonTablesFolder"))
    #Unmerge_MergedJson(get_Constant("Stitched_Json_URL"), get_Constant("JsonTablesFolder"))
    #for i in range(0,10):
    #print(i)
    #Deduplicated_Merged_Json_To_Pickle()
    #MergedJsonURL = get_Constant("Deduplicated_Merged_Json_URL")
    #OutputFolder = "C:/Saeed_Local_Files/WDC_Data/Untarred_WDC_Data/00/Camera_Brand"
    #OutputFolder = "C:/Saeed_Local_Files/WDC_Data/CSV/"+Dataset
    #Unmerge_MergedJson(MergedJsonURL ,OutputFolder )
    #Deduplicated_Merged_Json_To_CSV(OutputFolder)
    #Analyze_T2D_Gold_Standard()
    #T2D_Merge_Json_Files()
    #print(len(T2D_GoldStandard_Related_Columns("Country")))
    #T2D_Merge_Json_Files()
    #T2D_Merge_Json_Files()
    #Deduplicated_Merged_Json_To_Pickle()
    
    #update the units
    '''python2_command=" C:\Python27\python.exe C:/Users/new/Dropbox/1-Uni/CMPUT_605/Project/WinCode/Unit_Mgmt_Python2.7.py -f "+get_Constant("JsonTablesFolder")
    print(python2_command)
    os.system(python2_command)'''
    
    Dataset_Other_Stats()
    
    
    

