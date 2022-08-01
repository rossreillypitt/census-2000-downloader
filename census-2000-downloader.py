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



#this section does not yet handle exceptions, come back to that and make it work later
if len(sys.argv) > 1:
    start = int(sys.argv[1])
    end = int(sys.argv[2])

pdf_book = []
reader = PdfReader("sf1.pdf")
number_of_pages = len(reader.pages)
for x in range(227, 455):
    page = reader.pages[x]
    text = page.extract_text()
    text = text.split("\n")
    pdf_book.append(text)


file_url = 'https://www2.census.gov/census_2000/datasets/Summary_File_1/Pennsylvania/'
full_census_data = []
length_dict = {}
for x in range(1, 40): 
    census_data_staging_list = []
    file_name = f'pa000{x:02d}_uf1.zip'
    zipped_file = requests.get(f'{file_url}{file_name}')
    read_file = zipfile.ZipFile(BytesIO(zipped_file.content))
    sub_name = read_file.namelist()
    file_data = pd.read_csv(read_file.read(sub_name[0]), header=None)
    census_data_staging_list.append(file_data)
    full_census_data.append(census_data_staging_list)
    length_dict[file_name[:7]] = len(census_data_staging_list[0].columns)

headers_json =  requests.get("https://api.census.gov/data/2000/dec/sf1/variables.json")
headers_dict = json.loads(headers_json)
sorted_headers_dict = OrderedDict(sorted(json_dict['variables'].items()))

full_decennial_labels = {}
p_labels = {}
pct_labels = {}
h_labels = {}
error_list = []
for item in sorted_headers_dict:
    if item[0].isupper():
        try: 
            if item.startswith('P') and item[1].isnumeric():
                full_decennial_labels[item] = (f"{sorted_headers_dict[item]['concept']}: {sorted_headers_dict[item]['label']}")
                p_labels[item] = (f"{sorted_headers_dict[item]['concept']}: {sorted_headers_dict[item]['label']}")
            elif item.startswith('PCT'):
                full_decennial_labels[item] = (f"{sorted_headers_dict[item]['concept']}: {sorted_headers_dict[item]['label']}")
                pct_labels[item] = (f"{sorted_headers_dict[item]['concept']}: {sorted_headers_dict[item]['label']}")
            elif item.startswith('H'): 
                full_decennial_labels[item] = (f"{sorted_headers_dict[item]['concept']}: {sorted_headers_dict[item]['label']}")
                h_labels[item] = (f"{sorted_headers_dict[item]['concept']}: {sorted_headers_dict[item]['label']}")
        except KeyError:
            error_list.append(item)

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

z = 0
final_label_list = []
for x in range(2):
    y = 5
    label_staging_list = ['FILEID', 'STUSAB', 'CHARITER', 'CIFSN', 'LOGRECNO']
    while y < (length_list[x]):
        label_staging_list.append(p_list[z])
        y += 1
        z += 1
    final_label_list.append(label_staging_list)






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
