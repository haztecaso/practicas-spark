#!/usr/bin/env python3

import pandas as pd
import sys

colors = { True: 'red', False: 'blue' }

df = pd.read_csv(sys.argv[1])
df.plot.scatter(x='PCA1', y='PCA2')\
    .get_figure().savefig(sys.argv[1].split('.')[0]+'.png')

# df.plot.scatter(x='PCA1', y='PCA2', c=df['weekend'].map(colors))\
#     .get_figure().savefig(sys.argv[1].split('.')[0]+'.png')
