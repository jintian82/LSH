# -*- coding: utf-8 -*-
"""
Created on Thu Mar 29 09:28:29 2018

@author: Tian
"""

from datasketch import MinHash, MinHashLSH
import pandas as pd
import numpy as np

filename = "data.csv"
df = pd.read_csv(filename, names = ["acct", "event"], usecols = ["acct", "event"])
df.sort_values("acct")

acct = df.acct[0]
tmp = MinHash()
hashs = list()
accts = list()

for row in df.itertuples():
    if acct != row[1]:
        hashs.append(tmp)
        accts.append(acct)
        acct = row[1]
        tmp = MinHash()
        print(acct)
    tmp.update(row[2].encode('utf8'))
hashs.append(tmp)
accts.append(acct)
   
lsh = MinHashLSH(threshold=0.9)
for i in range(len(hashs)):
    lsh.insert(accts[i], hashs[i])





# Create LSH index
lsh = MinHashLSH(threshold=0.5, num_perm=128)
lsh.insert("m2", m2)
lsh.insert("m3", m3)
result = lsh.query(m1)
print("Approximate neighbours with Jaccard similarity > 0.5", result)

xx = np.random.RandomState(1)


data = pd.read_csv("data.csv", names = ["x", "y", "z"], usecols = ["x", "y"])


csv = r"""dummy,date,loc,x
bar,20090101,a,1
bar,20090102,a,3
bar,20090103,a,5
bar,20090101,b,1
bar,20090102,b,3
bar,20090103,b,5"""
f = open('foo.csv', 'w')
f.write(csv)
f.close()

df1 = pd.read_csv('foo.csv', 
        index_col=["date", "loc"], 
        usecols=["dummy", "date", "loc", "x"],
        parse_dates=["date"],
        header=0,
        names=["dummy", "date", "loc", "x"])


x = np.array(range(40000000), dtype=np.uint64)




