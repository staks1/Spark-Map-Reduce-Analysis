#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 18 01:36:41 2023

@author: st_ko
"""


import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

############# NOTES ###############################################
# WRITING INTO A CSV CAN EASILY BE DONE INSIDE EACH Qi.py query BUT I 
# DID IT OUT OF THE CLUSTER USING THE PRODUCED EXECUTION_TIMES SINCE
# I DID NOT WANT ANY PROBLEM WITH PACKAGES INSIDE THE VM 
# IF WE WANTED TO DO IT INSIDE EACH QUERY WE COULD CREATE A DATAFRAME
# THEN IN EACH QUERY WE WOULD WRITE-FILL IN THE CORRESPONDING FIELD
# AND THEN ALL QUERIES WERE RUN WE COULD PLOT THE DATAFRAME LIKE 
# I DO HERE 
###################################################################

# execution times as measured on the cluster 
v1 = 7.3400
v2 = 12.9137

# Create a DataFrame with the execution times 
df = pd.DataFrame({'Execution Time': [v1, v2]}, index=['Hash Join', 'Sort Merge'])

# Save DataFrame to a CSV file
# change to the path you want to save 
df.to_csv('/home/st_ko/Desktop/BigDataManagement_παραδοτέο/output/PART 2/TASK 2/execution_times.csv')


# Define custom colors for the bars
colors = ['steelblue', 'salmon']

# Create a bar plot with custom colors
ax = df.plot(kind='bar', legend=False)
#ax.set_axisbelow(True)
ax.yaxis.grid(color='gray', linestyle='dashed')

# Assign custom colors to the bars
for i, patch in enumerate(ax.patches):
    patch.set_facecolor(colors[i])

plt.xlabel('Join Type')
plt.ylabel('Execution Times')
plt.title('Comparison of Execution Times')
plt.yticks(np.arange(0,14,1))
plt.xticks(rotation='horizontal')
plt.show()

# save figure 
# change to the path you want to save 
plt.savefig('/home/st_ko/Desktop/BigDataManagement_παραδοτέο/output/PART 2/TASK 2/execution_times.png')
