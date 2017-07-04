# -*- coding: utf-8 -*-
"""
Created on Thu Apr 26 00:02:33 2017

@author: Ramesh
"""

import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

 
df= pd.read_csv('OilAccByTimeOfDay.csv')                  
 
objects = df['TimeOfDay']
y_pos = np.arange(len(objects))
performance = df['AccCount']

plt.bar(y_pos, performance, align='center', alpha=0.5 )
plt.xticks(y_pos, objects, rotation=90)
plt.ylabel('Accident Count')
plt.title('Number of Accidents by Time of the Day')
 
plt.show()