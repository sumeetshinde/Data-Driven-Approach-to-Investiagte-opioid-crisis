import pandas as pd
import datetime


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
             'Revised_Company_Name',
             'dos_str']
print(datetime.datetime.now())
df_wash = pd.read_csv('washRound3.csv', usecols=cols_keep, chunksize=18000000)     # RenameFile

print('read')
print(datetime.datetime.now())

i = 1
for chunk in df_wash:
    print('chunk read')
    print(datetime.datetime.now())

    for col in cols_keep:
        col_str = col + '.csv'
        chunk[col].to_csv(col_str, mode='a')
        print(col_str)
        print(datetime.datetime.now())

    print('end loop', i)
    print(datetime.datetime.now())
    i += 1

print('finished')
print(datetime.datetime.now())
