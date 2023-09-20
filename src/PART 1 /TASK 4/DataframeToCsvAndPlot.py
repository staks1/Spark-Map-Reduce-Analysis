#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 14 04:29:39 2023

@author: st_ko
"""
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

############# NOTES ###############################################
# WRITING INTO A CSV CAN EASILY BE DONE INSIDE EACH Qi.py query BUT I 
# DID IT OUT OF THE CLUSTER USING THE PRODUCED EXECUTION_TIMES 
# IF WE WANTED TO DO IT INSIDE EACH QUERY WE COULD CREATE A DATAFRAME
# THEN IN EACH QUERY WE WOULD WRITE THE CORRESPONDING FIELD
# AND THEN ALL QUERIES WERE RUN WE COULD PLOT THE DATAFRAME LIKE 
# I DO HERE 
###################################################################

# CREATE DATAFRAME FROM THE PRODUCED EXECUTION TIMES 
data = {
    'RDD on .csv': [3.94, 76.40, 0.40, 0.62, 0.63],
    'SparkSQL on .csv': [19.80, 29.97, 3.94, 9.38, 9.56],
    'SparkSQL on .parquet': [8.87, 6.36, 3.90, 8.51, 9.95]
}

index = ['Q1', 'Q2', 'Q3', 'Q4', 'Q5']

# create dataframe and save it into csv file 
df = pd.DataFrame(data, index=index, dtype=float)
# THE PATH SHOULD CHANGE TO THE LOCATION YOU WANT TO SAVE 
df.to_csv('/home/st_ko/Desktop/BigDataManagementProject/PROJECT_V3/PART1/dataframe_times.csv', index=False)

# plot dataframe 
colors = ['red', 'green', 'blue']

# Plot the DataFrame
ax = df.plot.bar(color=colors, legend=True)

# Set plot labels and title
ax.set_xlabel('Queries')
ax.set_ylabel('Execution time (s)')
ax.set_title('Execution time vs Queries')
ax.set_yticks(np.arange(0,80,3))
ax.set_axisbelow(True)
ax.yaxis.grid(color='gray', linestyle='dashed')

# Display the plot
plt.show()


# add legends for description 
legend_labels = df.columns.tolist()
legend_handles = [plt.Rectangle((0, 0), 1, 1, color=color) for color in colors]

# Add descriptions as legend labels
legend_descriptions = ['RDD on .csv', 'SparkSQL on .csv', 'SparkSQL on .parquet']
legend_labels_with_descriptions = [f'{desc}' for label, desc in zip(legend_labels, legend_descriptions)]

# Set legend
ax.legend(handles=legend_handles, labels=legend_labels_with_descriptions, title='Legend Title')

# Display the plot
# AND SAVE IT 
plt.show()
# THE PATH SHOULD CHANGE TO THE LOCATION YOU WANT TO SAVE 
plt.savefig('/home/st_ko/Desktop/BigDataManagementProject/PROJECT_V3/PART1/dataframe_times.jpg')


