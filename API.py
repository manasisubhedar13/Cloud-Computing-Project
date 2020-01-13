# -*- coding: utf-8 -*-
"""
Created on Fri Apr 19 17:54:30 2019

@author: manasi
"""

from googleapiclient import discovery
import json
import pandas as pd

API_KEY='AIzaSyDkeVXWSGlffPjP6d7aszADCwe0fR_dTFc'

df = pd.read_csv("BlackMen_User.csv")

df1 = pd.DataFrame()

df1['Tweet'] = df['Tweet']

No_of_Rows = (df1.shape[0])

Ts = []

for i in range(0,No_of_Rows):
    try:
        service = discovery.build('commentanalyzer', 'v1alpha1', developerKey=API_KEY)
        analyze_request = {'comment': { 'text': df1['Tweet'].iloc[i] },'requestedAttributes': {'TOXICITY': {}}}
        response = service.comments().analyze(body=analyze_request).execute()
        a = (pd.read_json(json.dumps(response['attributeScores'], indent=2)))
        b= (pd.DataFrame.from_dict(a.TOXICITY.spanScores[0]))
        Ts.append((b['score'][1]))
        print(i)
    except:
        Ts.append(0)
        
df['Toxicity_Score'] = pd.DataFrame({'Toxicity_Scores':Ts}) 

df.to_csv('BlackMen_User.csv')




