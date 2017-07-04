# -*- coding: utf-8 -*-
"""
Created on Thu Apr 26 00:02:33 2017

@author: Ramesh
"""

import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

 
df= pd.read_csv('WeatherandAccidentDataCombined_2016.csv')                  
 
df['XAxisData'] = df['StateCode']+df['YearMonth'].map(str)

objects=df['XAxisData']
indexobject=objects.index.values
y_pos = np.arange(len(objects))
AccidentCount = df['AccidentCount']
pcp=df['pcp']
tavg=df['tavg']
 

plt.xticks(y_pos, objects,fontsize = 3, rotation=90)
pltacc,=plt.plot(indexobject,AccidentCount,label="No. of Accidents")
pltpcp,=plt.plot(indexobject,pcp,label="Precipitation")
pltavgtmp,=plt.plot(indexobject,tavg,label="Average Temperature")

plt.legend(handles=[pltacc,pltpcp,pltavgtmp])

plt.xlabel("Pipeline Accident grouped by Location and Month");
plt.title('Influence of Weather on Oil Pipeline Accidents')
 
plt.show()