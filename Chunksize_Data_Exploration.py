import pandas as pd
import numpy as np
import datetime
import csv
from collections import defaultdict

print('started')
print(datetime.datetime.now())
df_chunk = pd.read_csv('arcos_all_washpost.tsv', sep='\t', chunksize=20000000)
chunk_list = []
print('read_csv complete!')
print(datetime.datetime.now())

buyerName_dict = defaultdict(int)
drugName_dict = defaultdict(int)
county_dict = defaultdict(int)

i = 1
for chunk in df_chunk:
    # chunk_list.append(chunk)
    """ for name in chunk['BUYER_NAME']:
        buyerName_dict[name] += 1
    print('buyers', i)
    print(datetime.datetime.now())

    for drug in chunk['DRUG_NAME']:
        drugName_dict[drug] += 1
    print('drug', i)
    print(datetime.datetime.now()) """

    for county in chunk['BUYER_COUNTY']:
        county_dict[county] += 1
    print('Counties', i)
    print(datetime.datetime.now())

    print('end loop', i)
    print(datetime.datetime.now())
    i += 1
    # print(buyerName_dict)

print('start output')
print(datetime.datetime.now())
""" with open('buyerName.csv', 'w') as f:
    for key in buyerName_dict.keys():
        f.write("%s,%s\n"%(key, buyerName_dict[key]))

with open('drugName.csv', 'w') as f:
    for key in drugName_dict.keys():
        f.write("%s,%s\n"%(key, drugName_dict[key])) """

county_list = list(county_dict.keys())
with open('counties.csv', 'w') as f:
    for key in county_dict.keys():
        f.write("%s,%s\n"%(key, county_dict[key]))

#df_concat = pd.concat(chunk_list)
print('Finished with csv')

#for chunk in df_chunk:
    #for row in chunk:



print('finished')
print(datetime.datetime.now())
