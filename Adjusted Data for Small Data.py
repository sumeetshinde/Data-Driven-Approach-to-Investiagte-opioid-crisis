import pandas as pd
import numpy as np
import datetime
from collections import defaultdict

dt = {'REPORTER_DEA_NO': 'str',
    'REPORTER_BUS_ACT': 'object',       #Don't cut
    'REPORTER_NAME': 'object',
    'REPORTER_CITY': 'object',
    'REPORTER_ZIP': 'int32',           #Not Object
    'REPORTER_COUNTY': 'object',
    'BUYER_DEA_NO': 'object',
    'BUYER_BUS_ACT': 'object',          #Don't cut
    'BUYER_NAME': 'object',
    'BUYER_CITY': 'object',
    'BUYER_ZIP': 'int32',
    'BUYER_COUNTY': 'object',
    'TRANSACTION_CODE': 'object',       #Need more information
    'DRUG_CODE': 'object',              #Cut - same as drug name however in object format
    'NDC_NO': 'uint64',                 #Check
    'DRUG_NAME': 'object',              #
    'QUANTITY': 'int16',
    'TRANSACTION_DATE': 'int32',
    'TRANSACTION_ID': 'int64',
    'CALC_BASE_WT_IN_GM': 'float64',
    'DOSAGE_UNIT': 'float64',
    'Product_Name': 'object',
    'Measure': 'object',                #Check
    'Combined_Labeler_Name': 'object',
    'Revised_Company_Name': 'object',
    'dos_str': 'float64'
    }
dt2 = {'REPORTER_DEA_NO': 'object',
    'REPORTER_BUS_ACT': 'object',       #Don't cut
    'REPORTER_NAME': 'object',
    'REPORTER_ZIP': 'object',           #Not Object
    'BUYER_DEA_NO': 'object',
    'BUYER_BUS_ACT': 'object',          #Don't cut
    'BUYER_NAME': 'object',
    'BUYER_ZIP': 'object',
    'DRUG_NAME': 'object',              #
    'QUANTITY': 'str',
    'TRANSACTION_DATE': 'int32',
    'CALC_BASE_WT_IN_GM': 'float64',
    'DOSAGE_UNIT': 'float64',
    'Combined_Labeler_Name': 'object',
    'Revised_Company_Name': 'object',
    'dos_str': 'float64'
    }
cols_keep = ['REPORTER_DEA_NO',
    'REPORTER_BUS_ACT',
    'REPORTER_STATE',
    'REPORTER_ZIP',
    'BUYER_DEA_NO',
    'BUYER_BUS_ACT',
    'BUYER_STATE',
    'BUYER_ZIP',
    'DRUG_NAME',
    'QUANTITY',
    'TRANSACTION_DATE',
    'CALC_BASE_WT_IN_GM',
    'DOSAGE_UNIT',
    'MME_Conversion_Factor',
    'Combined_Labeler_Name',
    'Revised_Company_Name',
    'dos_str']

cols_keep2 = ['REPORTER_DEA_NO',
    'REPORTER_BUS_ACT',       #Don't cut
    'BUYER_DEA_NO',
    'BUYER_BUS_ACT',          #Don't cut
    'DRUG_NAME',
    'QUANTITY',
    'TRANSACTION_DATE']

dos_str = {'5': 1,
    '7.5': 2,
    '10': 3}


adj_dt = {
    'REPORTER_STATE': 'int8',
    'BUYER_STATE': 'int8',
    'DRUG_NAME': 'int8',
    }

states = {'AK': 1, 'AL': 2, 'AR': 3, 'AZ': 4, 'CA': 5,
          'CO': 6, 'CT': 7, 'DE': 8, 'FL': 9, 'GA': 10,
          'HI': 11, 'IA': 12, 'ID': 13, 'IL': 14, 'IN': 15,
          'KS': 16, 'KY': 17, 'LA': 18, 'MA': 19, 'MD': 20,
          'ME': 21, 'MI': 22, 'MN': 23, 'MO': 24, 'MS': 25,
          'MT': 26, 'NC': 27, 'ND': 28, 'NE': 29, 'NH': 30,
          'NJ': 31, 'NM': 32, 'NV': 33, 'NY': 34, 'OH': 35,
          'OK': 36, 'OR': 37, 'PA': 38, 'RI': 39, 'SC': 40,
          'SD': 41, 'TN': 42, 'TX': 43, 'UT': 44, 'VA': 45,
          'VT': 46, 'WA': 47, 'WI': 48, 'WV': 49, 'WY': 50,
          'AS': 51, 'DC': 52, 'FM': 53, 'GU': 54, 'MH': 55,
          'MP': 56, 'PR': 57, 'PW': 58, 'VI': 59, 'UM': 60,
          'AA': 61, 'AE': 62, 'AP': 63}

drugs = {'HYDROCODONE': 1, 'OXYCODONE': 2}

rep_bus_act = {'DISTRIBUTOR': 1,
'MANUFACTURER': 2,
'REVERSE DISTRIB': 3,
'CHEMICAL MANUFACTURER': 4}

buy_bus_act = {'PRACTITIONER': 1,
'RETAIL PHARMACY': 2,
'PRACTITIONER-DW/275': 3,
'CHAIN PHARMACY': 4,
'PRACTITIONER-DW/100': 5,
'PRACTITIONER-DW/30': 6}

print(datetime.datetime.now())

dd_wash = pd.read_csv('washRound4.csv', usecols=cols_keep2, chunksize=18000000)

#df_wash1000 = pd.read_csv('washRound2.csv', usecols=cols_keep, dtype=dt2, nrows=40000)
#df_og1000 = pd.read_csv('arcos_all_washpost.tsv', sep='\t', nrows=40000)
#df_wash1000 = df_wash1000.replace({'REPORTER_STATE': states, 'BUYER_STATE': states, 'DRUG_NAME': drugs})
#df_wash1000 = df_wash1000.astype(adj_dt)

print(datetime.datetime.now())

#df_head = pd.DataFrame(dd_wash.head())
#df_head.to_csv('washHeadR2.csv')
#df_og1000.to_csv('washOg4000.csv')
#df_wash1000.to_csv('wash4000R2.csv')

i = 1
for chunk in dd_wash:
    print('chunk read')
    print(datetime.datetime.now())

    #chunk = chunk.replace({'REPORTER_STATE': states, 'BUYER_STATE': states, 'DRUG_NAME': drugs, 'dos_str': dos_str})
    #chunk = chunk.replace({'dos_str': dos_str, 'REPORTER_BUS_ACT': rep_bus_act, 'BUYER_BUS_ACT': buy_bus_act})
    #chunk = chunk.astype(adj_dt)

    chunk.to_csv('washDistribution.csv', mode='a', index=False)
    print('end loop', i)
    print(datetime.datetime.now())
    i += 1

print('finished')
print(datetime.datetime.now())

# Code was altered and several variations were made to export different data sets. 
# Here is a link to some of the exported data sets: https://drive.google.com/open?id=1zdobHWNkC3CqH6qhx2b0mzmI05wXxYnT
