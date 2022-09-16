# EXTRACTING CENSUS 2000 COLUMN HEADERS FROM THE SF1 TECHNICAL DOCUMENTATION PDF

A few weeks back, Steve had asked about the code for reading in the column headers from the 2000 census. He suggested that regex might be able to use to figure out how to read the titles, but I couldn't figure out what the best way to use it. This creates the opportunity for a teachable moment! The following is what I did; I'd be happy to learn what the better/more efficient way of getting the headers out of the PDF would be. 

### BACKGROUND

First, here's the link to the PDF that contains the info: 

https://www2.census.gov/programs-surveys/decennial/2000/technical-documentation/complete-tech-docs/summary-files/sf1.pdf

This was the only place I could find that indicated which column headers went with which data file. The column headre text that needed to be read in starts on page 228 and ends on page 415 or so. 

I used PyPDF's PDFReader to read in each page one at a time, and then I split the text of the page on carriage returns. Here is what pages looked like, generally speaking: 

```
['TABLE (MATRIX) SECTIONThefilesforSummaryFile1areprovidedasasetforeachstate,includingonegeographicheaderfileand39datafiles.Thedatafilesarepresentedinthetable(matrix)section.TheyareASCIIformatwithcommadelimitedfields.ThedatafieldsarenumericwiththemaximumsizeshowninMAXSIZE.Eachdatafilebeginswithfivefilelinkingfieldsfromthegeographicheaderfile.Thesefieldsareshownatthebeginningofeachdatafileinthistable(matrix)section.',
  'TablenumberTablecontentsDatadictionaryreferencenameSeg-mentMaxsize',
  'File 01 File Linking Fields (comma delimited)',
  'Fieldname DatadictionaryreferencenameMAXsizeDatatype FileIdentification FILEID6 A/N State/U.S.-Abbreviation(USPS) STUSAB2A CharacteristicIteration CHARITER3 A/N CharacteristicIterationFileSequenceNumber CIFSN2 A/N LogicalRecordNumber LOGRECNO7N',
  'P1. TOTAL POPULATION [1]Universe:TotalpopulationTotal P001001 01 9P2. URBAN AND RURAL [6]Universe:TotalpopulationTotal: P002001 01 9Urban: P002002 01 9Insideurbanizedareas P002003 01 9Insideurbanclusters P002004 01 9Rural P002005 01 9Notdefinedforthisfile P002006 01 9P3. RACE [71]Universe:TotalpopulationTotal: P003001 01 9Populationofonerace: P003002 01 9Whitealone P003003 01 9BlackorAfricanAmericanalone P003004 01 9AmericanIndianandAlaskaNativealone P003005 01 9Asianalone P003006 01 9Native Hawaiian and Other Pacific Islander alone P003007 01 9Someotherracealone P003008 01 9Populationoftwoormoreraces: P003009 01 9Populationoftworaces: P003010 01 9White;BlackorAfricanAmerican P003011 01 9White;AmericanIndianandAlaskaNative P003012 01 9White;Asian P003013 01 9White;NativeHawaiianandOtherPacificIslander P003014 01 9White;Someotherrace P003015 01 9BlackorAfricanAmerican;AmericanIndianandAlaskaNative P003016 01 9BlackorAfricanAmerican;Asian P003017 01 9',
  '7–25 DataDictionary',
  'U.S.CensusBureau,Census2000']
```

The start of a new data file is indicated by the "File XX File Linking Fields (comma delimited)" line. 

I was looking to find a few different column name formats:

PXXXXXX<br>
PXXXAXXX<br>
HXXXXXX<br>
HXXXAXXX<br>
PCTXXXXXX<br>
PCTXXXAXXX<br>
(where X=a number, and A=a letter)

So, different lengths, and some inconsistency between whether it was all numbers or a mix of numbers and letters after the file type heading. I also wanted to find a way to read them in in order -- with 200+ pages of column headings, I didn't want to assume they would go in a neat and logical order.

### WHAT I DID

The first thing I did was iterate through the list of pages I had built, and regroup the contents so that each list object contained only column header text related to a single data file (i.e. breaking on "File Linking", rather than pdf pages). 

```
current_file_name = 0
raw_column_header_list = []
staging_list = []
for x in range(len(pdf_book)):
    for y in range(len(pdf_book[x])):
        if "File Linking" in pdf_book[x][y]:
            if current_file_name == 0: 
                pass
            else: 
                raw_column_header_list.append(staging_list)
            #print(staging_list)
            staging_list = []
            current_file_name = pdf_book[x][y]
        else: 
            staging_list.append(pdf_book[x][y])
raw_column_header_list.append(staging_list)
```

Then, I went looking for any instance of a P or an H. If it found one, it checked three characters ahead to see whether that character was a number. That made sure that it caught all of the column headers, but ignored the section titles listed in the PDF (e.g. "P1. TOTAL POPULATION") If three characters from the start was a number, I added that P/H along with the ten following characters to a list. (Ten characters is the length of the longest column heading.)

```
header_reduction_staging_list = []
column_header_reduction_list = []
for item in raw_column_header_list:
    for x in range(len(item)):
        for y in range(len(item[x])):
            if item[x][y] == "P" or item[x][y] == 'H':
                try:
                    if item[x][y+3].isnumeric():
                        header_reduction_staging_list.append(item[x][y:y+10])
                    else:
                        pass
                except IndexError:
                    pass
    column_header_reduction_list.append(header_reduction_staging_list)
    header_reduction_staging_list = []
```
   
There was always a space after the column headers in the PDF, so I iterated over the list that I had just made and split on spaces. 

```
for x in range(len(column_header_reduction_list)):
    for y in range(len(column_header_reduction_list[x])):
        if " " in column_header_reduction_list[x][y]:
            column_header_reduction_list[x][y] = column_header_reduction_list[x][y].split(" ")
```

Then I iterated over the list with splits and got rid of anything with a period (various section headers could meet all the criteria previously set without actually being a column header (such as PCT3.). Then I looked for items that had been the result of a split that had at least seven characters and added them to a list. Then I searched for any items that had not been split (i.e. no spaces, has ten characters), and confirmed the last character was a number and added that. 

```
final_column_header_list = []
for item in column_header_reduction_list:
    staging_set = set()
    for items in item:
        if "." in items[0]:
            pass
        elif type(items) == list:
            if len(items[0])>=7:
                staging_set.add(items[0])
        elif items[-1].isnumeric():
            staging_set.add(items)
    staging_set = list(sorted(staging_set))
    final_column_header_list.append(staging_set)
```

And that's it! 
