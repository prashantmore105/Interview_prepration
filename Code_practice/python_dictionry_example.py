# -*- coding: utf-8 -*-
"""
Created on Mon Dec 18 13:16:31 2023

@author: prashantm5
"""

test_dict = {'month' : [1, 2, 3],'name' : ['Jan', 'Feb', 'March']}
#output = {1: 'Jan', 2: 'Feb', 3: 'March'}
print("The original dictionary is : " + str(test_dict))
x=list(test_dict.values())
print(x)
a=x[0]
b=x[1]
d=dict()
for i in range(0,len(a)):
    d[a[i]]=b[i]
# printing result
print("Flattened dictionary : " + str(d)) 

test_dict = {'month' : [1, 2, 3],
             'name' : ['Jan', 'Feb', 'March']}
print("The original dictionary is : " + str(test_dict))
print(test_dict['month'])
res = dict(zip(test_dict['month'], test_dict['name']))
print("Flattened dictionary : " + str(res))