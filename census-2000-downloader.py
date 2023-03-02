from os import path, getcwd, mkdir
import glob
import zipfile
import requests
from io import BytesIO
import pandas as pd
import sys
import json
from collections import OrderedDict
from PyPDF2 import PdfReader
import os


wprdc_zip_codes = ['8710000US15001', '8710000US15003', '8710000US15004', '8710000US15005', '8710000US15006', '8710000US15007', '8710000US15009', 
                   '8710000US15010', '8710000US15012', '8710000US15014', '8710000US15015', '8710000US15017', '8710000US15018', '8710000US15019', 
                   '8710000US15020', '8710000US15022', '8710000US15024', '8710000US15025', '8710000US15026', '8710000US15027', '8710000US15028', 
                   '8710000US15030', '8710000US15031', '8710000US15033', '8710000US15034', '8710000US15035', '8710000US15037', '8710000US15038', 
                   '8710000US15042', '8710000US15044', '8710000US15045', '8710000US15046', '8710000US15047', '8710000US15049', '8710000US15050', 
                   '8710000US15051', '8710000US15052', '8710000US15053', '8710000US15054', '8710000US15055', '8710000US15056', '8710000US15057', 
                   '8710000US15060', '8710000US15061', '8710000US15062', '8710000US15063', '8710000US15064', '8710000US15065', '8710000US15066', 
                   '8710000US15067', '8710000US15068', '8710000US15071', '8710000US15072', '8710000US15074', '8710000US15075', '8710000US15076', 
                   '8710000US15077', '8710000US15078', '8710000US15081', '8710000US15082', '8710000US15083', '8710000US15084', '8710000US15085', 
                   '8710000US15086', '8710000US15087', '8710000US15088', '8710000US15089', '8710000US15090', '8710000US15101', '8710000US15102', 
                   '8710000US15104', '8710000US15106', '8710000US15108', '8710000US15110', '8710000US15112', '8710000US15116', '8710000US15120', 
                   '8710000US15122', '8710000US15126', '8710000US15129', '8710000US15131', '8710000US15132', '8710000US15133', '8710000US15135', 
                   '8710000US15136', '8710000US15137', '8710000US15139', '8710000US15140', '8710000US15142', '8710000US15143', '8710000US15144', 
                   '8710000US15145', '8710000US15146', '8710000US15147', '8710000US15148', '8710000US15201', '8710000US15202', '8710000US15203', 
                   '8710000US15204', '8710000US15205', '8710000US15206', '8710000US15207', '8710000US15208', '8710000US15209', '8710000US15210', 
                   '8710000US15211', '8710000US15212', '8710000US15213', '8710000US15214', '8710000US15215', '8710000US15216', '8710000US15217', 
                   '8710000US15218', '8710000US15219', '8710000US15220', '8710000US15221', '8710000US15222', '8710000US15223', '8710000US15224', 
                   '8710000US15225', '8710000US15226', '8710000US15227', '8710000US15228', '8710000US15229', '8710000US15232', '8710000US15233', 
                   '8710000US15234', '8710000US15235', '8710000US15236', '8710000US15237', '8710000US15238', '8710000US15239', '8710000US15241', 
                   '8710000US15243', '8710000US15260', '8710000US15290', '8710000US15301', '8710000US15311', '8710000US15313', '8710000US15314', 
                   '8710000US15315', '8710000US15316', '8710000US15317', '8710000US15320', '8710000US15321', '8710000US15322', '8710000US15323', 
                   '8710000US15324', '8710000US15325', '8710000US15327', '8710000US15329', '8710000US15330', '8710000US15331', '8710000US15332', 
                   '8710000US15333', '8710000US15334', '8710000US15337', '8710000US15338', '8710000US15340', '8710000US15341', '8710000US15342', 
                   '8710000US15344', '8710000US15345', '8710000US15346', '8710000US15347', '8710000US15348', '8710000US15350', '8710000US15351', 
                   '8710000US15353', '8710000US15357', '8710000US15358', '8710000US15359', '8710000US15360', '8710000US15361', '8710000US15363', 
                   '8710000US15364', '8710000US15366', '8710000US15367', '8710000US15368', '8710000US15370', '8710000US15377', '8710000US15378', 
                   '8710000US15379', '8710000US15380', '8710000US15401', '8710000US15410', '8710000US15412', '8710000US15413', '8710000US15417', 
                   '8710000US15419', '8710000US15420', '8710000US15421', '8710000US15422', '8710000US15423', '8710000US15425', '8710000US15427', 
                   '8710000US15428', '8710000US15429', '8710000US15430', '8710000US15431', '8710000US15432', '8710000US15433', '8710000US15434', 
                   '8710000US15435', '8710000US15436', '8710000US15438', '8710000US15442', '8710000US15443', '8710000US15444', '8710000US15445', 
                   '8710000US15446', '8710000US15447', '8710000US15448', '8710000US15449', '8710000US15450', '8710000US15454', '8710000US15455', 
                   '8710000US15456', '8710000US15458', '8710000US15460', '8710000US15461', '8710000US15462', '8710000US15463', '8710000US15466', 
                   '8710000US15467', '8710000US15468', '8710000US15470', '8710000US15472', '8710000US15473', '8710000US15474', '8710000US15475', 
                   '8710000US15476', '8710000US15477', '8710000US15479', '8710000US15480', '8710000US15482', '8710000US15483', '8710000US15484', 
                   '8710000US15486', '8710000US15489', '8710000US15490', '8710000US15492', '8710000US15601', '8710000US15610', '8710000US15611', 
                   '8710000US15612', '8710000US15613', '8710000US15615', '8710000US15616', '8710000US15617', '8710000US15618', '8710000US15620', 
                   '8710000US15621', '8710000US15623', '8710000US15624', '8710000US15625', '8710000US15626', '8710000US15627', '8710000US15628', 
                   '8710000US15629', '8710000US15631', '8710000US15632', '8710000US15633', '8710000US15634', '8710000US15635', '8710000US15636', 
                   '8710000US15637', '8710000US15638', '8710000US15639', '8710000US15640', '8710000US15641', '8710000US15642', '8710000US15644', 
                   '8710000US15646', '8710000US15647', '8710000US15650', '8710000US15656', '8710000US15660', '8710000US15661', '8710000US15662', 
                   '8710000US15663', '8710000US15665', '8710000US15666', '8710000US15668', '8710000US15670', '8710000US15671', '8710000US15672', 
                   '8710000US15673', '8710000US15675', '8710000US15676', '8710000US15678', '8710000US15679', '8710000US15680', '8710000US15681', 
                   '8710000US15683', '8710000US15684', '8710000US15686', '8710000US15688', '8710000US15689', '8710000US15690', '8710000US15691', 
                   '8710000US15692', '8710000US15693', '8710000US15695', '8710000US15696', '8710000US15697', '8710000US15698', '8710000US15701', 
                   '8710000US15710', '8710000US15712', '8710000US15713', '8710000US15716', '8710000US15717', '8710000US15723', '8710000US15725', 
                   '8710000US15727', '8710000US15728', '8710000US15729', '8710000US15731', '8710000US15732', '8710000US15734', '8710000US15736', 
                   '8710000US15739', '8710000US15741', '8710000US15745', '8710000US15746', '8710000US15747', '8710000US15750', '8710000US15752', 
                   '8710000US15754', '8710000US15756', '8710000US15759', '8710000US15761', '8710000US15765', '8710000US15771', '8710000US15774', 
                   '8710000US15777', '8710000US15779', '8710000US15783', '8710000US15920', '8710000US15923', '8710000US15929', '8710000US15949', 
                   '8710000US16001', '8710000US16002', '8710000US16022', '8710000US16023', '8710000US16024', '8710000US16025', '8710000US16027', 
                   '8710000US16029', '8710000US16030', '8710000US16033', '8710000US16034', '8710000US16035', '8710000US16037', '8710000US16040', 
                   '8710000US16045', '8710000US16046', '8710000US16048', '8710000US16050', '8710000US16051', '8710000US16052', '8710000US16053', 
                   '8710000US16055', '8710000US16056', '8710000US16059', '8710000US16061', '8710000US16063', '8710000US16066', '8710000US16101', 
                   '8710000US16102', '8710000US16105', '8710000US16117', '8710000US16123', '8710000US16132', '8710000US16136', '8710000US16140', 
                   '8710000US16141', '8710000US16157', '8710000US16160', '8710000US16201', '8710000US16210', '8710000US16211', '8710000US16212', 
                   '8710000US16226', '8710000US16228', '8710000US16229', '8710000US16236', '8710000US16238', '8710000US16244', '8710000US16246', 
                   '8710000US16249', '8710000US16250', '8710000US16253', '8710000US16262', '8710000US16263']

#this section reads in the text from the SF1 documentation PDF and creates a list of lists,
#where each of the nested lists is a page from the pdf
pdf_book = []
reader = PdfReader("sf1.pdf")
number_of_pages = len(reader.pages)
for x in range(227, 455):
    page = reader.pages[x]
    text = page.extract_text()
    text = text.split("\n")
    pdf_book.append(text)

#this block regroups the text so that all of the text after each instance of the phrase "File Linking"
#"File Linking" is text that indicates the start of a new sequence
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

#this block isolates the IDs of the fields in each of the data files. 
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
for x in range(len(column_header_reduction_list)):
    for y in range(len(column_header_reduction_list[x])):
        if " " in column_header_reduction_list[x][y]:
            column_header_reduction_list[x][y] = column_header_reduction_list[x][y].split(" ")
final_column_header_list = []
for item in column_header_reduction_list:
    staging_set = set()
    for items in item:
        if "." in items[0]:
            pass
        elif type(items) == list:
            if len(items[0])>=7:
                staging_set.add(items[0])
        elif items[-    1].isnumeric():
            staging_set.add(items)
    staging_set = list(sorted(staging_set))
    final_column_header_list.append(staging_set)

#this block creates a dict that links the ID of the various fields with their Name/description
headers_json =  requests.get("https://api.census.gov/data/2000/dec/sf1/variables.json")
headers_dict = json.loads(headers_json.content)
sorted_headers_dict = OrderedDict(sorted(headers_dict['variables'].items()))
full_2 = {}
for item in final_column_header_list:
    for items in item:
        full_2[items] = (f"{sorted_headers_dict[items]['concept']}: {sorted_headers_dict[items]['label']}")

#this block creates a list where each element is the header row for a different data file         
final_label_list = []
for x in range(len(final_column_header_list)):
    label_staging_list = ['FILEID', 'STUSAB', 'CHARITER', 'CIFSN', 'LOGRECNO']
    for y in range(len(final_column_header_list[x])):
        #print(final_column_header_list[x][y])
        label_staging_list.append(f'{final_column_header_list[x][y]}: {full_2[final_column_header_list[x][y]]}')
    final_label_list.append(label_staging_list)

#this section unzips and reads in the raw census data located in the various data files    
file_url = 'https://www2.census.gov/census_2000/datasets/Summary_File_1/Pennsylvania/'
full_census_data = []
length_dict = {}
length_list = []
for x in range(1, 40): 
    census_data_staging_list = []
    file_name = f'pa000{x:02d}_uf1.zip'
    zipped_file = requests.get(f'{file_url}{file_name}')
    read_file = zipfile.ZipFile(BytesIO(zipped_file.content))
    sub_name = read_file.namelist()
    file_data = pd.read_csv(read_file.open(sub_name[0]), header=None)
    census_data_staging_list.append(file_data)
    logrecno_reformatting_list = [f'{item:07d}' for item in census_data_staging_list[0].iloc[:,4]]
    census_data_staging_list[0].iloc[:,4] = logrecno_reformatting_list
    length_dict[file_name[:7]] = len(census_data_staging_list[0].columns)
    full_census_data.append(census_data_staging_list[0])
    length_list.append(len(census_data_staging_list[0].columns))

# this section reads in the geographical linking file
file_name = f'pageo_uf1.zip'
zipped_file = requests.get(f'{file_url}{file_name}')
read_file = zipfile.ZipFile(BytesIO(zipped_file.content))
read_file
temp_arg = read_file.namelist()
file_data = pd.read_csv(read_file.open(temp_arg[0]), sep='delimiter', engine='python', header=None)

# this section extracts the different geographic elements used by WPRDC from the string of each row from 
# the geography file for use in assembling the geoid. 
# NOTE: the starting positions for the various elements are different than the 2010 census. 
geo_codes_list = []
for item in file_data[0]:
    codes = [item[8:11], item[18:25], item[29:31], item[31:34], item[36:41], item[55:61], item[61:62], item[160:166], item[342:347], item[200:290].strip()]
    geo_codes_list.append(codes)

# this section creates a dict that links logrecnos with their geoids.
logrecno_dict = {}
geoid = ''
for item in geo_codes_list: 
    if item[0] == "050":
        geoid = item[0]+'0000US'+item[2]+item[3]
        geoid = geoid.strip()
        logrecno_dict[item[1]] = geoid
    if item[0] == "060":
        geoid = item[0]+'0000US'+item[2]+item[3]+item[4]
        geoid = geoid.strip()
        logrecno_dict[item[1]] = geoid
    if item[0] == "140":
        geoid = item[0]+'0000US'+item[2]+item[3]+item[5]
        geoid = geoid.strip()
        logrecno_dict[item[1]] = geoid
    if item[0] == "150":
        geoid = item[0]+'0000US'+item[2]+item[3]+item[5]+item[6]
        geoid = geoid.strip()
        logrecno_dict[item[1]] = geoid
    if item[0] == "871":
        geoid = item[0]+'0000US'+item[7]
        geoid = geoid.strip()
        if geoid in wprdc_zip_codes:
            logrecno_dict[item[1]] = geoid
        else:
            pass
    if item[0] == "970": 
        geoid = item[0]+'0000US'+item[8]
        geoid = geoid.strip()
        logrecno_dict[item[1]] = geoid

print(len(full_census_data))

# os.mkdir("full_census_data")
# file_count = 1
# for item in full_census_data:
#     item_df = pd.DataFrame(item)
#     output_file_name = f'full_census_data/census_df_{file_count}.csv'
#     item_df.to_csv(output_file_name)
#     file_count+=1

# #this section creates the output that will be added to the wprdc database
# final_output = []
# logrecno_dict_keys = list(logrecno_dict.keys())
# for item in full_census_data:
#     for x in range(len(item)):
#         for y in range(5, len(item[x])):
#             staging_list = []
#             print(item.loc[x][4])
#             if item.loc[x][4] in logrecno_dict_keys:
#                 staging_list.append(logrecno_dict[item.loc[x][4]])
#                 staging_list.append((item[x][y]))
#                 staging_list.append(str(item[x][y]))
#                 staging_list.append('cen')
#                 staging_list.append(2000),
#                 staging_list.append(final_label_list[x][y])
#                 final_output.append(staging_list)
#             else:
#                 pass


    


# table_names = []
# for item in crsr.tables(tableType="TABLE"):
#     if item.table_name in exclude_list:
#         pass
#     else:
#         table_names.append(item.table_name)

# table_names.sort()

# column_names = []
# for item in table_names:
#     column_staging_list = []
#     if "mod" in item:
#         for col in crsr.columns(table=item):
#             column_staging_list.append(col.column_name)
#         missing_col_combo = missing_columns+column_staging_list
#         column_names.append(missing_col_combo)    
#     else:
#         for col in crsr.columns(table=item):
#             column_staging_list.append(col.column_name)
#         column_names.append(column_staging_list)
# table_columns = {}
# for x in range(len(table_names)):
#     if "mod" in table_names[x]:
#         table_columns[table_names[x][:-3]] = column_names[x]
#     else:
#         table_columns[table_names[x]] = column_names[x]

# zip_file_name = 'pa2010.sf1.zip'
# zipped_file = requests.get(f'https://www2.census.gov/census_2010/04-Summary_File_1/Pennsylvania/{zip_file_name}')
# read_file = zipfile.ZipFile(BytesIO(zipped_file.content))

# file_idx = []
# file_contents = []
# subsample_list = []
# for x in range(start, end):
#     if len(str(x)) == 1:
#         subsample_list.append(f'pa0000{x}2010.sf1')
#     elif len(str(x)) == 2:
#         subsample_list.append(f'pa000{x}2010.sf1')
# geo_list = []
# staging_list = []

# for item in read_file.namelist(): 
#     if item == "pageo2010.sf1": 
#         geo = read_file.read(item) 
#         geo_decoded = geo.decode("utf-8").split("\n")
#         geo_decoded.pop()
#         for item in geo_decoded:
#             geo_list.append(item)
#     elif "packinglist" in item: 
#         pass 
#     elif item in subsample_list: #this should just become 'else' when you're ready to run the whole thing
#         staging_list = [] 
#         name, ext = item.split(".") 
#         file_idx.append(name)
#         the_file = read_file.read(item) 
#         final_file = the_file.decode("utf-8") 
#         data_list = final_file.split("\n") 
#         data_list.pop() 
#         for items in data_list: 
#             staging_list.append(items.split(","))
#         file_contents.append(staging_list) 
#     else:
#         pass
    
# # this section below creates a dictionary connecting logrecnos with geoids
# geo_codes_list = []
# for item in geo_list:
#     codes = [item[8:11], item[18:25], item[27:29], item[29:32], item[36:41], item[54:60], item[60:61], item[171:176], item[194:199], item[226:316].strip()]
#     geo_codes_list.append(codes)

# logrecno_dict = {}
# geoid = ''
# for item in geo_codes_list: 
#     if item[0] == "050":
#         geoid = item[0]+'0000US'+item[2]+item[3]
#         geoid = geoid.strip()
#         logrecno_dict[item[1]] = geoid
#     if item[0] == "060":
#         geoid = item[0]+'0000US'+item[2]+item[3]+item[4]
#         geoid = geoid.strip()
#         logrecno_dict[item[1]] = geoid
#     if item[0] == "140":
#         geoid = item[0]+'0000US'+item[2]+item[3]+item[5]
#         geoid = geoid.strip()
#         logrecno_dict[item[1]] = geoid
#     if item[0] == "150":
#         geoid = item[0]+'0000US'+item[2]+item[3]+item[5]+item[6]
#         geoid = geoid.strip()
#         logrecno_dict[item[1]] = geoid
#     if item[0] == "871":
#         geoid = item[0]+'0000US'+item[7]
#         geoid = geoid.strip()
#         if geoid in wprdc_zip_codes:
#             logrecno_dict[item[1]] = geoid
#         else:
#             pass
#     if item[0] == "970": 
#         geoid = item[0]+'0000US'+item[8]
#         geoid = geoid.strip()
#         logrecno_dict[item[1]] = geoid

# final_data_table = []
# staging_list = []

# for item in file_contents: 
# #    if item != file_contents[0]: #the [5] is limiting this process to just the final set of data in the file_contents variable for time-saving/debugging. Delete when ready to run fully.
# #        pass                     #delete this too when you're ready to run this
# #    else:                        #delete this too when you're ready to run this
#     for x in range(len(item)):
#         if item[x][3] == "45": 
#             for y in range(5, len(item[x][:-21])):
#                 staging_list = []
#                 table_col_lookup = f'SF1_000{item[x][3]}_PT1'
#                 if item[x][4] in logrecno_dict:
#                     staging_list.append(logrecno_dict[item[x][4]])
#                     staging_list.append((item[x][y]))
#                     staging_list.append(item[x][y])
#                     staging_list.append('cen')
#                     staging_list.append(2010), 
#                     staging_list.append(table_columns[table_col_lookup][y])
#                     final_data_table.append(staging_list)
#                 else:
#                     pass
#             for y in range(len(item[x][:-21]), len(item[x])):
#                 staging_list = []
#                 table_col_lookup = f'SF1_000{item[x][3]}_PT2'
#                 if item[x][4] in logrecno_dict:
#                     staging_list.append(logrecno_dict[item[x][4]])
#                     staging_list.append((item[x][y]))
#                     staging_list.append(item[x][y])
#                     staging_list.append('cen')
#                     staging_list.append(2010), 
#                     staging_list.append(table_columns[table_col_lookup][y-234])
#                     final_data_table.append(staging_list)
#                 else:
#                     pass
#         else: 
#             for y in range(5, len(item[x])):
#                 staging_list = []
#                 table_col_lookup = f'SF1_000{item[x][3]}'
#                 if item[x][4] in logrecno_dict:
#                     staging_list.append(logrecno_dict[item[x][4]])
#                     staging_list.append((item[x][y]))
#                     staging_list.append(item[x][y])
#                     staging_list.append('cen')
#                     staging_list.append(2010), 
#                     staging_list.append(table_columns[table_col_lookup][y])
#                     final_data_table.append(staging_list)
#                 else:
#                     pass

# decennial_dF = pd.DataFrame(final_data_table)
# decennial_dF.columns = ["geoid", "value", "raw_value", "survey", "year", "table_id"]
# decennial_dF["value"] = pd.to_numeric(decennial_dF["value"])
# file_name = f'census_2010_data_sf1_000{start}_to_sf1_000{end}.csv'
# decennial_dF.to_csv(file_name)

# #this section pulls the descriptions of the data fields from access and puts them in a dataframe with labels
# data_descriptors = []
# staging_list = []
# saved_seg, saved_table, header_1, header_2, header_3, header_4, header_5 = None, None, None, None, None, None, None
# crsr.execute("SELECT * FROM DATA_FIELD_DESCRIPTORS")
# for row in crsr.fetchall():
#     staging_list.append(row)
# for row in staging_list:
#     current_seg = row[1]
#     current_table = row[2]
#     if current_seg == saved_seg:
#         if current_table == saved_table:
#             if row[5] == None:
#                 header_2 = row[3]
#             elif row[3][0].isalnum():
#                 data_descriptors.append([current_seg, current_table, ", ".join([header_1, header_2, row[3]]), row[4]])
#                 header_3 = row[3]
#             elif row[3][5].isalnum():
#                 row[3] = row[3].strip()
#                 header_4 = row[3]
#                 data_descriptors.append([current_seg, current_table, ", ".join([header_1, header_2, header_3, row[3]]), row[4]])
#             elif row[3][11].isalnum():
#                 row[3] = row[3].strip()
#                 data_descriptors.append([current_seg, current_table, ", ".join([header_1, header_2, header_3, header_4, row[3]]), row[4]])
#                 header_5 = row[3]
#             elif row[3][15].isalnum():
#                 row[3] = row[3].strip()
#                 data_descriptors.append([current_seg, current_table, ", ".join([header_1, header_2, header_3, header_4, header_5, row[3]]), row[4]])
#         else:
#             saved_table = current_table
#     else: 
#         saved_seg = current_seg
#         saved_table = current_table
#         header_1 = row[3]

# descriptors_dF = pd.DataFrame(data_descriptors)
# descriptors_dF.to_csv("data_field_descriptors.csv")

# #test

# # seq_header_list = []
# # url_capture_idx = 0
# # if "seq" in read_file.namelist()[url_capture_idx]:
# #     url, filename = read_file.namelist()[url_capture_idx].split("seq")
# # else: 
# #     url, filename = read_file.namelist()[url_capture_idx+1].split("seq")
# # for x in range(1, len(read_file.namelist())):
# #     try:
# #         file_to_read = f'{url}seq{x}.xlsx'
# #         seq_header_txt = pd.read_excel(read_file.read(file_to_read), header=None)
# #         seq_header_txt = seq_header_txt.iloc[:, 6:].transpose()
# #         seq_header_txt.loc[:, len(seq_header_txt.columns)]=f'{x:04d}'
# #         seq_header_list.append(seq_header_txt)        
# #     except ValueError:
# #         pass
# # seq_header_dF = pd.concat(seq_header_list)
# # print(seq_header_dF)


# #for item in glob.glob('Downloads/pa2010.sf1/*'):
# #    item2 = item[:-4]
# #    os.rename(item, f'{item2}.txt')
