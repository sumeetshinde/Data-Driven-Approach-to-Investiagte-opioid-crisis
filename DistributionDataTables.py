import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime

cols_rep = ['REPORTER_DEA_NO',
            'REPORTER_BUS_ACT',
            'REPORTER_STATE',
            'REPORTER_COUNTY',
            'QUANTITY',
            'TRANSACTION_DATE']

cols_buy = ['BUYER_DEA_NO',
            'BUYER_BUS_ACT',
            'BUYER_STATE',
            'BUYER_COUNTY',
            'QUANTITY',
            'TRANSACTION_DATE']

cols_dist_cty = ['REPORTER_STATE',
                 'REPORTER_COUNTY',
                 'QUANTITY']

cols_rec_cty = ['BUYER_STATE',
                'BUYER_COUNTY',
                'QUANTITY']

cols_from_to_buy = ['BUYER_DEA_NO',
            'BUYER_BUS_ACT',
            'REPORTER_STATE',
            'REPORTER_COUNTY',
            'QUANTITY',
            'TRANSACTION_DATE']

cols_from_to_dist = ['REPORTER_DEA_NO',
            'REPORTER_BUS_ACT',
            'BUYER_STATE',
            'BUYER_COUNTY',
            'QUANTITY',
            'TRANSACTION_DATE']



print('started')
print(datetime.datetime.now())
df_comp = pd.read_csv('WashPostAdjusted.csv', usecols=cols_dist_cty, chunksize=5000000)
print('read_csv complete!')
print(datetime.datetime.now())

i = 1
for chunk in df_comp:
    chunk = pd.pivot_table(chunk, values='QUANTITY', index=['REPORTER_STATE'],
                           columns=['REPORTER_COUNTY'], aggfunc=np.sum)
    chunk = chunk.stack()
    chunk.to_csv('dist_cty_qty.csv', mode='a')

    print('end loop', i)
    print(datetime.datetime.now())
    i += 1

print('started')
print(datetime.datetime.now())
df_comp = pd.read_csv('WashPostAdjusted.csv', usecols=cols_rec_cty, chunksize=5000000)
print('read_csv complete!')
print(datetime.datetime.now())

i = 1
for chunk in df_comp:
    chunk = pd.pivot_table(chunk, values='QUANTITY', index=['BUYER_STATE'],
                           columns=['BUYER_COUNTY'], aggfunc=np.sum)
    chunk = chunk.stack()
    chunk.to_csv('rec_cty_qty.csv', mode='a')

    print('end loop', i)
    print(datetime.datetime.now())
    i += 1


print('finished')
print(datetime.datetime.now())

#Exported Data was adjusted in Excel and then charts were made in excel. Files available upon request. 
