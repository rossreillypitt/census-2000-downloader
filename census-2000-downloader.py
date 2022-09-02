from os import path
import glob
import zipfile
import requests
from io import BytesIO
import pandas as pd
import sys
import json
from collections import OrderedDict
from PyPDF2 import PdfReader

pdf_book = []
reader = PdfReader("sf1.pdf")
number_of_pages = len(reader.pages)
for x in range(227, 455):
    page = reader.pages[x]
    text = page.extract_text()
    text = text.split("\n")
    pdf_book.append(text)

current_file_name = 0
file_list_dict = []
staging_list = []
for x in range(len(pdf_book)):
    for y in range(len(pdf_book[x])):
        if "File Linking" in pdf_book[x][y]:
            if current_file_name == 0: 
                pass
            else: 
                file_list_dict.append(staging_list)
            #print(staging_list)
            staging_list = []
            current_file_name = pdf_book[x][y]
        else: 
            staging_list.append(pdf_book[x][y])
file_list_dict.append(staging_list)

header_reduction_staging_list = []
column_header_reduction_list = []
for item in file_list_dict:
    for x in range(len(item)):
        for y in range(len(item[x])):
            if item[x][y] == "P" or item[x][y] == 'H':
                try:
                    if item[x][y+3].isnumeric():
                        #print(item[x][y:y+10])
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
        elif items[-1].isnumeric():
            staging_set.add(items)
    staging_set = list(sorted(staging_set))
    final_column_header_list.append(staging_set)

headers_json =  requests.get("https://api.census.gov/data/2000/dec/sf1/variables.json")
headers_dict = json.loads(headers_json.content)
sorted_headers_dict = OrderedDict(sorted(headers_dict['variables'].items()))

full_2 = {}
for item in final_column_header_list:
    for items in item:
        full_2[items] = (f"{sorted_headers_dict[items]['concept']}: {sorted_headers_dict[items]['label']}")
        
final_label_list = []
for x in range(len(final_column_header_list)):
    label_staging_list = ['FILEID', 'STUSAB', 'CHARITER', 'CIFSN', 'LOGRECNO']
    for y in range(len(final_column_header_list[x])):
        print(final_column_header_list[x][y])
        label_staging_list.append(f'{final_column_header_list[x][y]}: {full_2[final_column_header_list[x][y]]}')
    final_label_list.append(label_staging_list)
    
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
    length_dict[file_name[:7]] = len(census_data_staging_list[0].columns)
    full_census_data.append(census_data_staging_list)
    length_list.append(len(census_data_staging_list[0].columns))

file_name = f'pageo_uf1.zip'
zipped_file = requests.get(f'{file_url}{file_name}')
read_file = zipfile.ZipFile(BytesIO(zipped_file.content))
read_file
temp_arg = read_file.namelist()
file_data = pd.read_csv(read_file.open(temp_arg[0]), sep='delimiter', engine='python', header=None)



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
