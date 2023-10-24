#!/usr/bin/python
from xml.etree import ElementTree as ET
import pandas as pd
from datetime import date
from datetime import datetime
import datetime
import numpy as np
import os
import sys
import csv
import re
import fnmatch
import math
import uuid
import chardet

"""
xmlGenerate.py



######################################################################################

v4.4 Written by Zack Mason on 9/19/2023
- Fixed a ton of stupid bugs. Previously this application only handled one row per manifest file.
  Now it handles an infinite number of rows per manifest file. 
1. Removed .tar from metadata file output
2. File Identifier in DistributionURL now has extension - also switched to the NCRMP_SFM_FIXED filename
3. Removed quotes from filesize.
4. Parent Identifier - added fixed or stratified after year
5. Parent Identifier link - fixed ampersand issues 
-  https://data.noaa.gov/metaview/page?xml=NOAA/NESDIS/ncei/ncei-nc/iso/xml/C00005.xml&view=getDataView&header=none
6. Converted file size from bytes to MB and added MB formatting

######################################################################################

v3.3 Written by Zack Mason on 10/31/2022
- Added some comments
- Updated the xml writing functions to actually write the full xml file
- Changed the name of the output xml to mirror the correct input file
- Parsed ship name from mission ID and added ship lookup table
- Added parent record ID and metadata landing page link
- Modified the getData function to be applicable to more use cases
- Updated date formatting to yyyy-mm-dd with padded zeros in case of single digits

######################################################################################

v2.2 Written by Zack Mason on 10/6/2022
- Removed various functions that are no longer needed.
- Added method to grab data based on column header and row number
- Added method to evaluate row number based on site name.
- Added method to get site name from file name found in data file

#######################################################################################
v1.1 Written by Zack Mason on 7/4/2022

When run, this application will call the setup() method and ask the user
which use case is appropriate. Currently only option 2 is functional. The
other options were included as I wasn't sure how this application would need
to be used and now I have a better idea. 

#######################################################################################
TO - DO

1. Right now the dictionary file for keywords and region names is read in locally - it is assumed it is in the working directory.
   This should likely be changed?

2. Add TAR file module to see contents of TAR files?
    6A. Use this as a start:
    
import tarfile,os
import sys

my_tar = tarfile.open('my_tar.tar.gz')
my_tar.extractall('./my_folder') # specify which folder to extract to
my_tar.close()


3. You forgot to add an xml parser! Not sure this is necessary anymore but could be useful going forward.
    3a. Try to use namespaces. Maybe something like this: 
    
    https://stackoverflow.com/questions/61141671/how-to-find-and-edit-tags-in-xml-files-with-namespaces-using-elementtree
    
        from xml.etree import ElementTree as ET
        tree = ET.parse(adiPath)
        rating = tree.find(".//{*}Rating")  # Find the Rating element in any namespace
        rating.text = "999"

4. VERIFY parent record link - Example of NCEI WAF - URL will be ncei/coral/xml/iso or something. Structure so we have a landing page per big record
4a. https://data.noaa.gov/metaview/page?xml=NOAA/NESDIS/ncei/paleo//iso/xml/noaa-borehole-1004878.xml&view=getDataView&header=none



################################################################################################################################

HOW TO USE THIS SCRIPT

1. This script requires a few different CSV files to run correctly. These function as lookup tables. These could likely be condensed
   into one excel file if the code to access them was modified accordingly. 
1a. Here is the list of files that should be in your working directory: 
    - islandLookup = 'islandLookup.csv'
    - projectLookup = "projectLookup.csv"
    - regionKeywordLookupTable = 'Region_Keywords.csv'
    - shipLookup = "shipLookup.csv"
    - dictionaryFileName = 'SfM_Dictionary.csv'

2. This program reads from an xml metadata template and replaces placeholders within that template. This template
   is currently set as being stored in the working directory where you run this script. Here is the file name:
   'xmlTemplate.xml.' This can be easily modified if desired.

3. Ensure that your working directory contains all the lookup tables mentioned in step 1a and the xml metadata 
   template mentioned in step 2. It should also contain the .mnf files that you would like to parse through. It 
   will parse through each of them so make sure that the only .mnf files in this directory are ones that you would 
   like to process.

4. This script accepts a CRCP project number as a command line input. For example, if I wanted to specify 
   that the data in question were all from project 743 I would run this program like this: python xmlGenerate.py 743
4a. The default project is number 743, the National Coral Reef Monitoring Program. There is no need to specify the
    project number unless it differs from this, however there is no harm in including it, as in the previous example.
4b. If you specify a project number that the program does not recognize, it will ask you for the corresponding
    project name. It will then ingest the name and number and write them to a project lookup table in your working
    directory.

5. Upon startup this program will ask if you want to continue or exit the program. Enter 1 to continue or 2 to exit.

6. This program will create one xml metadata file for each mnf class manifest file that it finds. The xml metadata
   file will have the same file name as the mnf file that it corresponds to.

"""

# Declare some variables here to be used elsewhere in the program.
"""ALL lookup tables should likely be defined here. Need to modify this

    - islandLookup = 'islandLookup.csv'
    - projectLookup = "projectLookup.csv"
    - regionKeywordLookupTable = 'Region_Keywords.csv'
    - shipLookup = "shipLookup.csv"
    - dictionaryFileName = 'SfM_Dictionary.csv'
    - fixedLookup
    - strsLookup
"""

dictionaryFileName = 'SfM_Dictionary.csv'
islandLookup = 'islandLookup.csv'
fixedLookup = 'fixedLookup.csv'
strsLookup = 'strsLookup.csv'
uuidLookup = 'uuidLookup.csv'
badFileList = []

try:
    crcpProjectNumber = sys.argv[1]
except:
    crcpProjectNumber = 743


# If myDirectory is left like this you can really only run this script from your current working directory.
# This does allow the user to run this application anywhere they have access though. I think it works as-is.
myDirectory = os.getcwd()
    
def getFileList(myDirectory):
    """This function gets a list of mnf files from the user's current directory.
    Specifically, the os.listdir command gets a list of everything in the directory
    and then this function iterates over that list and grabs everything where the
    filename ends with .mnf. These are then added to another list.

    Args:
        myDirectory (string): This directory is the user's current working 
        directory.

    Returns:
        list: This is a list of mnf files in the user's current directory.
    """
    dataFiles = []
    myFiles = os.listdir(myDirectory)
    for file in myFiles:
        if file.endswith(".mnf"):
            dataFiles.append(file)
    return dataFiles

def getLonLat(columnName,fileName,minOrMax):
    df = pd.read_csv(fileName)
    column = df[columnName]
    max_value = column.max()
    min_value = column.min()
    if minOrMax == "min":
        return min_value
    elif minOrMax == "max":
        return max_value
    else:
        return "INVALID INPUT"

def getDateRange(file,minOrMax):
    """This function is passed a filepath string. It then converts that file into a dataframe.
    once that dataframe is converted, it takes the date column from the dataframe and converts
    all values into datetime objects. Then it grabs the converted date column and gets the
    minimum and maximum values. Once those are identified it converts them back to string 
    values and combines them to create a range.

    Args:
        file (string): string containing a file path extracted from a list of all files in 
        the user's current directory.

    Returns:
        string: This dateRange string contains two dateTimes converted to strings. Basically these
        are the earliest and latest dates found in the date column of the file passed to this function.
    """
    
    df = pd.read_csv(file)
    df['DATE'] =  pd.to_datetime(df['DATE'])
    column = df["DATE"]
    max_value = column.max()
    min_value = column.min()

    dateRange = str(min_value) + " - " + str(max_value)
    del df
    if minOrMax == "min":
        return min_value
    elif minOrMax == "max":
        return max_value
    else:
        return dateRange

def dateConvert(dateString,format):
    date_object = datetime.datetime.strptime(dateString, format)

    monthString = str(date_object.month)
    if len(monthString) <2:
        monthString = "0" + monthString
    
    dayString = str(date_object.day)
    if len(dayString)<2:
        dayString = "0" + dayString
    newDate = str(date_object.year) + '-' + monthString + '-' + dayString
    return newDate

def getUUID(csvFileName):
    myUUID= ""
    try:
        fileNameRowNumber = getRowNumber(str(csvFileName),uuidLookup,"File Name")
        myUUID = getData(fileNameRowNumber,uuidLookup,"uuid")
        print("FILENAME AND UUID FOUND. UUID: " + str(myUUID))
    except:
        myUUID = uuid.uuid4()
        #print("UUID GENERATED: " + str(myUUID))
        data = [str(csvFileName),str(myUUID)]
        with open(uuidLookup, 'a') as f:
            writer = csv.writer(f)
            writer.writerow(data)
    return myUUID

def detect_encoding(file):
    detector = chardet.universaldetector.UniversalDetector()
    with open(file, "rb") as f:
        for line in f:
            detector.feed(line)
            if detector.done:
                break
    detector.close()
    return detector.result

def editTemplateForReal(myTemplate, surveyDate,islandKeywords,islandOceanKeywords,missionStart,missionEnd,siteName,eastLon,westLon,northLat,southLat,islandFullName,regionName,fileSize,csvFileName,year,parentRecordID,regionCountryKeyword,regionOceanKeyword,shipName,currentDate,tarFileName,gcmdKeyword,myUUID):
    projectLookup = "projectLookup.csv"


    charenc = detect_encoding(myTemplate)
    charenc = charenc['encoding']
    print("Encoding:", charenc)

    with open(myTemplate,'r',encoding=charenc) as newXmlTemplate:
        print("TEMPLATE OPEN")
        templateText = newXmlTemplate.read()
    
    #landingPageLink = 'https://data.noaa.gov/metaview/page?xml=NOAA/NESDIS/ncei/paleo//iso/xml/[*INSERT*].xml\&view=getDataView\&header=none'
    landingPageLink = 'https://data.noaa.gov/waf/NOAA/NESDIS/ncei/coral/iso/[*INSERT*].xml\&amp;view=getDataView\&amp;header=none'
    landingPageLink = landingPageLink.replace('[*INSERT*]',parentRecordID)

    crcpProjectKeyword = ""
    if str(crcpProjectNumber) == "743":
        crcpProjectKeyword = "National Coral Reef Monitoring Program (NCRMP)"
    else:
        try:
            projectRowNumber = getRowNumber(int(crcpProjectNumber),projectLookup,"projectNumber")
            #("This is your project row number: " + str(projectRowNumber))
            crcpProjectKeyword = getData(projectRowNumber,projectLookup,"projectName")
            #print("THIS IS YOUR PROJECT NAME: " + crcpProjectKeyword)
        except:
            crcpProjectKeyword = input("\nProject information not found. Please enter the project name for project number " + str(crcpProjectNumber) + ":\n")
            data = [int(crcpProjectNumber),str(crcpProjectKeyword)]
            with open(projectLookup, 'a') as f:
                writer = csv.writer(f)
                writer.writerow(data)
            with open(projectLookup, 'r') as csvFile:
                reader = csv.reader(csvFile)
                for item in reader:
                    print(item)
    surveyDate = dateConvert(surveyDate,'%m/%d/%Y')
    myDict = {'[*CRCPProjectNumber*]':str(crcpProjectNumber),'[*Date*]':str(currentDate),'[*SurveyDate*]':str(surveyDate),
    '[*CoRISPlaceCountry*]':str(regionCountryKeyword),'[*CoRISPlaceOcean*]':str(regionOceanKeyword),'[*MissionStartTime*]':str(missionStart),
    '[*MissionEndTime*]':str(missionEnd),'[*SiteName*]':str(siteName),'[*SiteWestLon*]':str(westLon),'[*SiteEastLon*]': str(eastLon),'[*SiteSouthLat*]': str(southLat),
    '[*SiteNorthLat*]':str(northLat),'[*IslandName*]':str(islandFullName),'[*Region*]':regionName,'[*FileSize*]':str(fileSize),'[*ImageStartTime*]':str(surveyDate),
    '[*ImageEndTime*]':str(surveyDate),'[*FileIdentifier*]':str(tarFileName),'[*SfMSiteFileName*]':str(tarFileName),'[*Year*]':str(year),'[*DistributorFormat*]':"JPEG",
    '[*CRCPProjectKeyword*]':str(crcpProjectKeyword),'[*parent metadata ID*]':str(parentRecordID),'[*parent metadata landing page link or DOI link*]': str(landingPageLink),
    '[*CoRISPlaceIslandCountry*]': str(islandKeywords),'[*CoRISPlaceIslandOcean*]':str(islandOceanKeywords),'[*KeywordShipName*]':str(shipName),'[*GCMD_PlaceKeyword]':str(gcmdKeyword),
    '[*UUID*]':str(myUUID)}
    
    print(myDict)

    for entry in myDict:
        templateText = templateText.replace(entry,myDict[entry])

    findThis = '\[\*.*\*\]'
    thingsToReplace = re.findall(findThis, templateText)
    sortedList = []
    [sortedList.append(x) for x in thingsToReplace if x not in sortedList]
    for x in sortedList:
        print(x)
    return templateText

def generateFilename(row):
    """
    This function is currently unused.
    """
    myFilename = (f"{row['filename']}")
    #print("Here is the autogenerated filename: " + myFilename)
    updatedFilename = input("If you'd like, enter a different filename now (without the extension). Otherwise, hit ENTER.\n")
    if len(updatedFilename)>0:
        myFilename = updatedFilename
        #print("Filename updated successfully.\n")
    return myFilename

def convert_size(size_bytes):
    myFileSize = round(size_bytes/(1024.0 * 1024.0))
    sizeString = str(myFileSize)
    return sizeString

def getFileSize(data):
    fileSize = str(data).split(",")
    fileSize = fileSize[2]
    fileSize = fileSize.replace("'","")
    fileSize = convert_size(int(fileSize))
    return fileSize



def oneRecordPerFile():
    """
    myFileList - List of .mnf files found in current working directory.
        This is generated by the getFileList function.

    file - Current file in iteration of the myFileList list of mnf files.

    mnfData - This is the string extracted from the current file in the
        iteration of the myFileList list. 

    siteName - Site name code extracted from the filename listed in each
        mnf file. This will be used to extract relevant data from the 
        data csv file.

    """
    myFileList = getFileList(myDirectory)
    currentDate = date.today()
    myLookup = ""

    #print("\nTHIS IS YOUR FILE LIST: " + str(myFileList))
    for file in myFileList:
        dataCount = 0
        #print("STARTING WITH THIS FILE: " + str(file))
        mnfDataList = parseMnf(file)
        listLength = str(len(mnfDataList))
        print("YOU SHOULD SEE THIS NUMBER OF RECORDS: " + listLength)
        #print("HERE IS YOUR MNF DATA: " + str(mnfDataList))
        for mnfData in mnfDataList:
            dataCount +=1
            myTemplate = ""
            if "fixed" in str(mnfData).lower():
                print("FIXED RECORD FOUND")
                myLookup = fixedLookup
                myTemplate = "xmlTemplate_fixed.xml"
            elif "strs" in str(mnfData).lower():
                print("STRS RECORD FOUND")
                myLookup = strsLookup
                myTemplate = "xmlTemplate_StRS.xml"
            
            print("\nCURRENTLY WORKING ON FILE NUMBER: " + str(dataCount))
            csvFileName = getFileName(mnfData)
            #print("FOUND THIS FILE: " + str(csvFileName))
            
            if "NCRMP" not in csvFileName:
                csvFileName = csvFileName.replace("CRMP","NCRMP")

            siteName = getSiteName(str(csvFileName))
            fileSize = getFileSize(mnfData)

            missionStart = getDateRange(myLookup,"min")
            startString = str(missionStart).split(" ")
            missionStart = startString[0]
            missionEnd = getDateRange(myLookup,"max")
            endString = str(missionEnd).split(" ")
            missionEnd = endString[0]

            try:
                rowNumber = getRowNumber(siteName,myLookup,'SITE')
            except:
                rowNumber = False

            if rowNumber is not False:
                mission = getData(rowNumber,myLookup,"MISSION")
                numberOfImages = getData(rowNumber,myLookup,"NUMBER OF IMAGES")
                island = getData(rowNumber,myLookup,"ISLAND")
                surveyDate = getData(rowNumber,myLookup,"DATE")

                dateString = str(surveyDate)
                count = len(dateString)
                year = str(surveyDate[count-4])+str(surveyDate[count-3])+str(surveyDate[count-2])+str(surveyDate[count-1])
                fixedOrRandom = ""
                print("THIS IS THE FILENAME YOU're LOOKING FOR: " + str(csvFileName))
                if "fixed" in str(csvFileName).lower():
                    fixedOrRandom = "_Fixed"
                elif "strs" in str(csvFileName).lower():
                    fixedOrRandom = "_StRS"
                parentRecordID = mission + "_" + year + fixedOrRandom + "_sfm"

                eastLon = getData(rowNumber,myLookup,"LONGITUDE")
                westLon = getData(rowNumber,myLookup,"LONGITUDE")
                southLat = getData(rowNumber,myLookup,"LATITUDE")
                northLat = getData(rowNumber,myLookup,"LATITUDE")
            else:
                myDict = {csvFileName:"rowNumber issue"}
                print("Skipped processing on this file: " + str(csvFileName))
                badFileList.append(myDict)
            try:
                dictionaryRowNumber = getRowNumber(island,dictionaryFileName,'Island_Code')
            except:
                dictionaryRowNumber = False

            if dictionaryRowNumber is not False:
                islandKeywords = getData(dictionaryRowNumber,dictionaryFileName,'CoRIS Region')
                islandOceanKeywords = getData(dictionaryRowNumber,dictionaryFileName,'CoRIS Ocean')
                islandFullName = getData(dictionaryRowNumber,dictionaryFileName,'ISLAND')
                gcmdKeyword = getData(dictionaryRowNumber,dictionaryFileName, 'GCMD Keyword')
            else:
                myDict = {csvFileName:"dictionaryRowNumber issue"}
                print("Skipped processing on this file: " + str(csvFileName))
                badFileList.append(myDict)
            
            try:
                islandRowNumber = getRowNumber(island,islandLookup,'Island_Code')
            except:
                islandRowNumber = False

            if islandRowNumber is not False:
                regionName = getData(islandRowNumber,islandLookup,'Region_Name')
                regionCode = getData(islandRowNumber,islandLookup, 'Region_Code')
            else:
                myDict = {csvFileName:"islandRowNumber issue"}
                print("Skipped processing on this file: " + str(csvFileName))
                badFileList.append(myDict)
            
            regionKeywordLookupTable = 'Region_Keywords.csv'

            try:
                regionNumber = getRowNumber(regionCode,regionKeywordLookupTable,'Region_Code')
            except:
                regionNumber = False

            if regionNumber is not False:
                regionCountryKeyword = getData(regionNumber, regionKeywordLookupTable, 'CoRIS Country')
                regionOceanKeyword = getData(regionNumber, regionKeywordLookupTable, 'CoRIS Ocean')
            else:
                myDict = {csvFileName:"regionNumber issue"}
                print("Skipped processing on this file: " + str(csvFileName))
                badFileList.append(myDict)

            missionCode = mission[0] + mission[1]
            shipLookup = "shipLookup.csv"
            try:
                missionLookupNumber = getRowNumber(missionCode, shipLookup,'Ship_Two_letter_code')
            except:
                rowNumber = False

            if missionLookupNumber is not False:
                shipName = getData(missionLookupNumber, shipLookup,'Ship Keyword')
            else:
                myDict = {csvFileName:"missionLookup issue"}
                print("Skipped processing on this file: " + str(csvFileName))
                badFileList.append(myDict)

            #tarFileName = str(file)
            #tarFileName = tarFileName.replace(".mnf","")
            
            myUUID= getUUID(csvFileName)

            if rowNumber is not False and missionLookupNumber is not False and regionNumber is not False and islandRowNumber is not False and dictionaryRowNumber is not False:
                # Call the function that edits the xml template with all the gathered information
                xmlText = editTemplateForReal(myTemplate, surveyDate,islandKeywords,islandOceanKeywords,missionStart,missionEnd,siteName,eastLon,westLon,northLat,southLat,islandFullName,regionName,fileSize,csvFileName,year,parentRecordID,regionCountryKeyword,regionOceanKeyword,shipName,currentDate,csvFileName,gcmdKeyword,myUUID)
                writeXml(xmlText,csvFileName)


def writeXml(xmlData,xmlFileName):
    # Write the actual xml file based on the data gathered above.
    xmlFileName = str(xmlFileName).replace(".tar","")
    xmlFileName = str(xmlFileName).replace(".csv","")
    xmlFileName = xmlFileName + ".xml"
    with open(xmlFileName, 'w') as f:
        f.write(xmlData)
    
def getData(rowNumber,csvFileName,columnName):
    df = pd.read_csv(csvFileName)
    myVariable = df.loc[[rowNumber], [columnName]]
    variableData = myVariable.iloc[0][columnName]
    #print(variableData)
    return variableData

def getRowNumber(dataInHand,csvFileName,columnOfInterest):
    """
    Gets the row number where the attribute dataInHand is equal to the value in the column specified
    in the columnOfInterest variable.

    dataInHand: String (unless cast otherwise). Passed into the function. This is the data we are using
                basically as a key to look for other values. 
    csvFilename: String. This is the name of the file that the function will open and scan.

    columnOfInterest: String. This is the column name that the function will look through to match
                      the dataInHand variable.

    df: Pandas dataframe. Created by reading the file specified in csvFileName.

    rowNumber: int. The row number where the dataInHand variable is equal to the value in the columnOfInterest.
    """
    df = pd.read_csv(csvFileName)
    rowNumber = df[df[columnOfInterest] == dataInHand].index[0]
    if rowNumber.size != 0:
        return rowNumber
    else:
        rowNumber = False
        return rowNumber

def getFileName(mnfData):
    """
    Gets the filename of the CSV file from the mnf data file.
    """
    extensionList = ["tar","csv","dat"]
    for extension in extensionList:
        if extension == "csv":
            regex = ".*\.csv"
        elif extension == "tar":
            regex = ".*\.tar"
        elif extension == "dat":
            regex = ".*\.dat"
        for item in mnfData:
            #print("\n\nLooking through this data: " + str(item))
            filename = re.findall(regex,item)
            if len(filename) >0 :
                filename = str(filename).replace("[", "").replace("'", "").replace("]","")
                #print(str(filename))
                return filename


def getSiteName(fileName):
    splitsies = fileName.split('_', 8)
    siteName = str(splitsies[7])
    return siteName

def parseMnf(mnfFile):
    #df = pd.read_csv(mnfFile)
    # Open file 
    rowList = []
    with open(mnfFile) as file_obj:
        # Create reader object by passing the file 
        # object to reader method
        reader_obj = csv.reader(file_obj)
    # Iterate over each row in the csv 
    # file using reader object
        for row in reader_obj:
            rowList.append(row)
    return rowList
    
def setup():
    """_summary_
    """
    print("1. Run the application.")
    print("2. Exit the program.")
    selection = input("Enter your selection now:\n")
    if selection == "1":
        oneRecordPerFile()
    elif selection == "2":
        print("Thanks for using this program. Goodbye.")
        sys.exit()
    elif selection != "1" and selection != "2" and selection != "3":
        print("Please follow directions")
        setup()
        
# Main goes here I guess.
setup()
print("\n")
print(str(len(badFileList)))
for badFile in badFileList:
    print(str(badFile))