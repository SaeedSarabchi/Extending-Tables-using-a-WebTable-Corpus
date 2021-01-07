'''
This file is used for functionalities related to annotating each column of our web table corpus as: 'numeric', 'date' or 'text'
'''
import json
import sys
import os
import copy
#import io
import codecs
globalString=""
import argparse

def TagUnits(JsonFolderPath):
    #####
        #This Function Tags each Column, its Type and Update the Json file | Type:["string" or "numeric" or "date"], If Numeric, Tagging the "unit" as well. (Needs Experiment???)
	        #- For Tagging the Type: tag each value, Then the most common tag is assigned.
	        #- For Numeric Units: First, Checking The Column Values, If parsed numeric, and the Unit was present , Then we tag the most common unit, else, we search for the unit in the header, Else we tag it as "unit": "dimensionless"
    #####
    
    #Logging all the columns tagged
    global globalString

    for file in os.listdir(JsonFolderPath):
        if (file.endswith(".json")):
            
            #if (file.replace(".json","")<"2567"):
                #continue
            print(file)

            with codecs.open(JsonFolderPath + "/" + file, 'r',encoding='utf-8', errors='ignore') as f1:

                jsonFile = json.load(f1)

                #Getting the webtable data:
                relation = jsonFile["relation"]

                #This List that tells which unit is assigned to which column based on the orders of the columns
                unitList=[]

                #Assigning a Unit for each Column
                for col in relation:
                    colData = copy.copy(col)
                    ColHeader=colData.pop(0)
                    #if (jsonFile["hasHeader"]):
                        #ColHeader = colData.pop(jsonFile["headerRowIndex"])
                    unit_Tag = Tag_Unit_for_Column(colData, ColHeader, file)
                    unitList.append(unit_Tag)

                jsonFile["units"]=unitList
                with codecs.open(JsonFolderPath + "/" + file, 'w', encoding='utf-8') as outfile:
                    
                    json.dump(jsonFile, outfile)

    #Saving the log
    Textfile_UnitMgmt_Log = codecs.open("C:/Saeed_Local_Files/Proposed_Sol/Tag_Col_Units_Log.txt", "w", encoding="utf-8")
    Textfile_UnitMgmt_Log.write(globalString)

def Tag_Unit_for_Column(colData, ColHeader, fileName):
    ####
    #This Function Tags each Coulumn as either text, numberic or date
    #The assigned Tag would be the most common one between the values
    ####

    global globalString

    globalString+=("File Name: " +fileName+" Header: "+ColHeader)
    globalString += "\n"+str(colData)

    #This list conatins the unit tag for each value
    valueTagList = []

    for value in colData:
        valueTag = Tag_Unit_for_Value(value)
        valueTagList.append(valueTag)

    FinalTag = most_common(valueTagList)

    # in case of numeric without unit, See if the unit is contained in the Header
    if (FinalTag == ["numeric", "dimensionless", "dimensionless"]):
        extractedUnit = Extract_Unit_From_Text(ColHeader)
        if(extractedUnit!="Null"):
            FinalTag = extractedUnit

    globalString+=("\n Final Tag: "+str(FinalTag))
    globalString+= ("\n#####################\n")

    return FinalTag


from quantulum import parser
from nltk.tokenize import word_tokenize

def Tag_Unit_for_Value(value):
    ####
        #This Function Tags each function as [text], [numeric, unit] or [date]
    ####

    # Here we assume that dates are comprised of multiple numbers, so 2017 is not considered as a date, instead it is considered as numeric.
    # Try if the value is date:
    # First checks if the value has more than one word, then it checks if it is a date or not, to avoid assigning all values with one numbers as dates
    word_count = len(word_tokenize(value.replace("/"," ").replace("-"," ").replace(".","").replace(",","")))
    if(word_count>1):
        date_Result = is_date(value)
        if (date_Result == True):
            return ["date"]

    try:
        #Try if the value is numeric:


        quants = parser.parse(value.lower())
        unit = quants[0].unit.name
        entity = quants[0].unit.entity.name
        return ["numeric", unit, entity]


    except:
        pass


    #If Not numeric and Not date, Then the value is considered text
    return ["text"]


def Extract_Unit_From_Text(text):
    ####
        #Testing each subsequent of the input text for containing a unit
    ####

    cleansedText = text.replace(".","").lower()
    tokens = word_tokenize(cleansedText)
    SubSeq = Find_All_Subsequences(tokens)
    for s in SubSeq:
        try:
            quants = parser.parse("1 "+s)
            unit = quants[0].unit.name
            entity = quants[0].unit.entity.name
            if(unit!="dimensionless"):
                return ["numeric",unit, entity]
        except:
            pass

    #If nothing found, then:
    return "Null"

def Find_All_Subsequences(List):
    SubsequenceList=[]
    for n in (2,1):
        res = zip(*(List[i:] for i in range(n)))
        for s in res:
            subseq=""
            for elem in s:
                subseq+=" "+elem
            SubsequenceList.append(subseq)

    return SubsequenceList

from dateutil.parser import parse

def is_date(string):
    try:
        #print(string)
        #if(len(string)>100):
            #return False
        res = parse(string)
        if(res==None):
            return False

        return True
    except ValueError:
        return False




#most_common function is re-used from StackOverflow: https://stackoverflow.com/questions/1518522/python-most-common-element-in-a-list:
import itertools
import operator

def most_common(L):
  # get an iterable of (item, iterable) pairs
  SL = sorted((x, i) for i, x in enumerate(L))
  # print 'SL:', SL
  groups = itertools.groupby(SL, key=operator.itemgetter(0))
  # auxiliary function to get "quality" for an item
  def _auxfun(g):
    item, iterable = g
    count = 0
    min_index = len(L)
    for _, where in iterable:
      count += 1
      min_index = min(min_index, where)
    # print 'item %r, count %r, minind %r' % (item, count, min_index)
    return count, -min_index
  # pick the highest-count/earliest item
  return max(groups, key=_auxfun)[0]


def unit_Tests():
    #unit_Test1 for most_common:
    L1 = ["hi", "bye", "hi", "bye", "salam", "salam", "salam","khodahafez"]
    MostCommon = most_common(L1)
    if(MostCommon=="salam"):
        print("MostCommon1 Passed")
    else:
        print("MostCommon1 Failed!!!")

    #unit_Test2 for most_common:
    L2 = [["hi", "bye"], ["hi"], ["bye"], ["salam"], ["salam", "salam"],["khodahafez"], ["salam", "salam"], ["salam", "salam"], ["hi"]]
    MostCommon = most_common(L2)
    if(MostCommon==["salam", "salam"]):
        print("MostCommon2 Passed")
    else:
        print("MostCommon2 Failed!!!")

    #unitTes1t for is_date:
    Res = is_date("5/23/2010")
    if(Res == True):
        print("is_date1 Passed")
    else:
        print("is_date1 Failed!!!")

    # unitTest2 for is_date:
    Res = is_date("1994-11-05T08:15:30-05:00")
    if (Res == True):
        print("is_date2 Passed")
    else:
        print("is_date2 Failed!!!")

    # unitTest3 for is_date:
    Res = is_date("two sdhj sd")
    if (Res == False):
        print("is_date3 Passed")
    else:
        print("is_date3 Failed!!!")

    # unitTest1 for Tag_Unit_for_Value:
    valueTag = Tag_Unit_for_Value(u'9500')
    if (valueTag == ["numeric","dimensionless", "dimensionless"]):
        print("Tag_Unit_for_Value1 Passed")
    else:
        print("Tag_Unit_for_Value1 Failed!!!")

    # unitTest2 for Tag_Unit_for_Value:
    valueTag = Tag_Unit_for_Value("twenty one grams")
    if (valueTag == ["numeric","gram", "mass"]):
        print("Tag_Unit_for_Value2 Passed")
    else:
        print("Tag_Unit_for_Value2 Failed!!!")

    # unitTest3 for Tag_Unit_for_Value:

    #November 5, 1994, 8:15:30 am, US Eastern Standard Time
    valueTag = Tag_Unit_for_Value("5/23/2010")
    if (valueTag == ["date"]):
        print("Tag_Unit_for_Value3 Passed")
    else:
        print("Tag_Unit_for_Value3 Failed!!!")

    # unitTest4 for Tag_Unit_for_Value:
    valueTag = Tag_Unit_for_Value("kjhdkjd")
    if (valueTag == ["text"]):
        print("Tag_Unit_for_Value4 Passed")
    else:
        print("Tag_Unit_for_Value4 Failed!!!")


    # unitTest1 for Find_All_Subsequences:
    tokens = word_tokenize("Area(Sq Km)".lower())
    subseqs = Find_All_Subsequences(tokens)
    if (subseqs == [' area (', ' ( sq', ' sq km', ' km )', ' area', ' (', ' sq', ' km', ' )']):
        print("Find_All_Subsequences1 Passed")
    else:
        print("Find_All_Subsequences1 Failed!!!")


    # unitTest1 for Extract_Unit_From_Text:
    extractedUnit = Extract_Unit_From_Text("Are(Sq. Km.)")
    if (extractedUnit == ["numeric","square kilometre", "area"]):
        print("Extract_Unit_From_Text1 Passed")
    else:
        print("Extract_Unit_From_Text1 Failed!!!")


    # unitTest2 for Extract_Unit_From_Text:
    extractedUnit = Extract_Unit_From_Text("Are(population)")
    if (extractedUnit == "Null"):
        print("Extract_Unit_From_Text2 Passed")
    else:
        print("Extract_Unit_From_Text2 Failed!!!")


    # unitTest1 for Tag_Unit_for_Column:
    FinalUnit = Tag_Unit_for_Column(["skjh","khskd","hjsgjs"],"kjhdsf kjd","test")
    if (FinalUnit == ["text"]):
        print("Tag_Unit_for_Column1 Passed")
    else:
        print("Tag_Unit_for_Column1 Failed!!!")

    # unitTest2 for Tag_Unit_for_Column:
    FinalUnit = Tag_Unit_for_Column(["12/13/1988","12-Jan-1988","1398"],"kjhdsf kjd","test")
    if (FinalUnit == ["date"]):
        print("Tag_Unit_for_Column2 Passed")
    else:
        print("Tag_Unit_for_Column2 Failed!!!")

    # unitTest3 for Tag_Unit_for_Column:
    FinalUnit = Tag_Unit_for_Column(["7687","6878","78.98"],"kjhdsf kjd","test")
    if (FinalUnit == ["numeric","dimensionless","dimensionless"]):
        print("Tag_Unit_for_Column3 Passed")
    else:
        print("Tag_Unit_for_Column3 Failed!!!")

    # unitTest3 for Tag_Unit_for_Column:
    FinalUnit = Tag_Unit_for_Column(["7687","6878","78.98"],"Area(Sq. Km.)","test")
    if (FinalUnit == ["numeric","square kilometre", "area"]):
        print("Tag_Unit_for_Column3 Passed")
    else:
        print("Tag_Unit_for_Column3 Failed!!!")




if (__name__ == "__main__"):
    #TagUnits("C:\Saeed_Local_Files\TestDataSets\city_dataset\json_tables")
    #valueTag = Tag_Unit_for_Value(u'9500')
    #unit_Tests()
    input_parser = argparse.ArgumentParser()
    input_parser.add_argument('-f', '--folder_name', type=str, default="", help='folder name')
    args = input_parser.parse_args()
    TagUnits(args.folder_name)