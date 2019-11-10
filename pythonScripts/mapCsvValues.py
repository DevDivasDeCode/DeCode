import os
import pandas as pd
import numpy as np
import time
from multiprocessing import  Pool

hoseCountGeolocation = pd.read_csv("output.csv")
hoseCountData = pd.read_csv("hose-count-traffic-volumes-2014-to-2017.csv", delimiter=';')

size = len(hoseCountData)
hoseCountData['address'] = (hoseCountData['Block Num']).map(str) + ' ' + hoseCountData['STD Street'] + ' Vancouver, BC, Canada'
hoseCountGeolocation.rename(columns={'input_string':'address'}, inplace=True)

# i = 0
# for index, row in hoseCountData.iterrows():
#     if i == 10:
#         break
#     addr = (str(row['Block Num']) + ' ' + row['STD Street'])
#     print (index, addr)
#     hoseCountData.iloc[0]['latitude'] = hoseCountGeolocation.loc[hoseCountGeolocation['input_string'] == addr]['latitude']
#     row['longitude'] = hoseCountGeolocation.loc[hoseCountGeolocation['input_string'] == addr]['longitude']
#     print (row['latitude'], row['longitude'])
#     i += 1

newData = pd.merge(hoseCountData, hoseCountGeolocation, on='address')
newData.to_csv("hoseDataResult.csv", encoding='utf8')


def parallelize_dataframe(df, func, n_cores=4):
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df
 






