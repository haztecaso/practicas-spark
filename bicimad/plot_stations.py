#!/usr/bin/env python3

import pandas as pd
import sys

colors = { True: 'red', False: 'blue' }

df = pd.read_csv(sys.argv[1]).sort_values(by=['weekend','hour'])

df_week = df.iloc[0:24].rename(columns = {'sum(free_bases)': 'laboral'})

df_weekend = df.iloc[24:48].rename(columns = {'sum(free_bases)':'finde'})

plot_week = df_week.plot(x='hour')
plot = df_weekend.plot(x='hour', ax=plot_week)

plot.get_figure().savefig(sys.argv[1].split('.')[0]+'.png')
