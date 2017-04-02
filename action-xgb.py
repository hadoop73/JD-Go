# coding:utf-8


import numpy as np
import pandas as pd



df = pd.read_csv('act.csv')


# 删除第 1 列
df.drop(df.columns[0],inplace=True,axis=1)

print df.head()






