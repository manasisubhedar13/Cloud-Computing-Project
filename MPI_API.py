from mpi4py import MPI
import numpy as np
import pandas as pd
from googleapiclient import discovery
import json
import itertools
import time 

start_time = time.time()

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

API_KEY='AIzaSyDkeVXWSGlffPjP6d7aszADCwe0fR_dTFc'

df = pd.read_csv("WW.csv")

df1 = pd.DataFrame()

df1 = df['Tweet']

Ts = []

if rank == 0:

    data = df1

    data_part = [int(x) for x in np.linspace(0, len(data), size+1)]

    dflist = [data.iloc[data_part[i]:data_part[i+1]] for i in range(size)]
    
else:

    dflist = None

dflist  = comm.scatter(dflist, root=0)

for i in dflist:
    try:
        service = discovery.build('commentanalyzer', 'v1alpha1', developerKey=API_KEY)
        analyze_request = {'comment': { 'text': i },'requestedAttributes': {'TOXICITY': {}}}
        response = service.comments().analyze(body=analyze_request).execute()
        a = (pd.read_json(json.dumps(res['attributeScores'], indent=2)))
        b= (pd.DataFrame.from_dict(a.TOXICITY.spanScores[0]))
        Ts.append((b['score'][1]))
    except:
        Ts.append(0)

d = zip(dflist, Ts)

dflist_done = comm.gather(d, root = 0)

if rank == 0:
    d4 = list(itertools.chain.from_iterable(dflist_done))

    df3 = pd.DataFrame(d4)

    df3.to_csv('WW.csv')

    print('Time taken using MPI')

    print((time.time() - start_time))
