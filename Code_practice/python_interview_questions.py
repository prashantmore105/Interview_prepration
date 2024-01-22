# -*- coding: utf-8 -*-
"""
Created on Mon Dec 18 13:23:38 2023

@author: prashantm5
"""

from collections import Counter
abc = [200,400,400,100,100,100,500,500,500,500]
occur = Counter(abc)
print("occur is")
print(occur)
def occurance(n):
    for x in occur:
        print(x)
        if occur[x]==n:
            print(x)
            
occurance(4)           


mystr = 'missisipi'
mydict = {}
for x in mystr:
    print(x)
    if x in mydict:
        mydict[x] +=1
        print("fist loop")
        print(mydict[x])
    else :
        mydict[x] =1
        print("2nd loop")
        print(mydict[x])

print("updated counter")
print(mydict)


mar = [29,71,95,32,71]
print(list(sorted(mar)))
print(list(sorted(set(mar)))[-2])




stu = ['Kalyan','Krish','Ashu','Abdul']
mar = [29,71,95,32]
Passing_marks = 35
#Output: {'Kalyan':'Fail','Krish':'Pass','Ashu':'Pass','Abdul':'Fail'}
out_dict = {}
for x in range(len(stu)):
    print("x in here")
    print(x)
    if mar[x] < Passing_marks:
        out_dict[stu[x]] = 'Fail'
    else:
        out_dict[stu[x]] = 'Pass'
		
print(out_dict)







