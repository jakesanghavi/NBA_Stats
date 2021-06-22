import pandas as pd
import numpy as np
from numpy import genfromtxt
import os
import urllib
import matplotlib.pyplot as plt
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
import matplotlib.colors as colors
# from colors import rgb, hex
import matplotlib.cm as cm
import struct
import sys

pd.options.mode.chained_assignment = None
data = pd.read_csv('2021_reg_pbp.csv')
data = data.loc[data['EVENTMSGTYPE'] == 1]
names = pd.read_csv("stats_nba_player_data_2020-21.csv")

shots = pd.DataFrame(0, index=range(150), columns=range(1))
# name = "Zion Williamson"
for x in range(0, len(names)):
    name = names['PLAYER_NAME'].iloc[x]
    data_h = data.loc[data['PLAYER1_NAME'] == name]
    # data_h = data_h.loc[data_h['EVENTMSGTYPE'] == 1]
    shots[name] = np.nan
    news = data_h['EVENTMSGACTIONTYPE'].value_counts()
    news = news.sort_index()
    shots[name] = news
    # shots.join(news)

print(shots)
del shots[0]
shots['indexer'] = shots.index.values
shots.to_csv("shot_count_by_type.csv", index=False)
