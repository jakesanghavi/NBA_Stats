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

COLORS = {'ATL':'#E03A3E','BKN':'#000000','BOS':'#007A33','CHA':'#00788C','CHI':'#CE1141','CLE':'#860038',
          'DAL':'#00538C','DEN':'#FEC524','DET':'#C8102E','GSW':'#FFC72C','HOU':'#CE1141','IND':'#FDBB30',
          'LAC':'#C8102E','LAL':'#552583','MEM':'#5D76A9','MIA':'#98002E','MIL':'#00471B','MIN':'#0C2340',
          'NOP':'#0C2340','NYK':'#F58426','OKC':'#007AC1','ORL':'#0077C0','PHI':'#006BB6','PHX':'#E56020',
          'POR':'#E03A3E','SAC':'#5A2D81','SAS':'#C4CED4','TOR':'#CE1141','UTA':'#002B5C','WAS':'#E31837',
         }

# Remove panda warnings
pd.options.mode.chained_assignment = None
# Reads CVS. Ignore DType warnings
data = pd.read_csv("2021_reg_pbp.csv", low_memory=False)
# name_set = pd.read_csv("player_id_matches_2021.csv")
name_set = pd.read_csv("stats_nba_player_data_2020-21.csv")
bb_ref_stats = pd.read_csv("basketball_reference_totals_2021.csv")

guava = list(range(96))

for x in range(0, len(guava)):
    boof = str(guava[x])
    beef = boof + "'"
    guava[x] = beef

data = data.fillna('')


# for x in range(0, len(data)):
#     data['DESCRIPTION'][x] = str(data['HOMEDESCRIPTION'][x]) + ' ' + str(data['NEUTRALDESCRIPTION'][x]) + ' ' + str(data['VISITORDESCRIPTION'][x])

data['DESCRIPTION'] = data['HOMEDESCRIPTION'] + ' ' + data['NEUTRALDESCRIPTION'] + ' ' + data['VISITORDESCRIPTION']
data.insert(15, 'DEPTH', 0)

for x in range(0, len(data)):
    if (data['EVENTMSGTYPE'][x] == 1):
        L = data['DESCRIPTION'].iloc[x].split()
        for y in range(0, len(L)):
            if L[y] in guava:
                L[y] = L[y].replace("'", "")
                data['DEPTH'][x] = L[y]
    if (data['EVENTMSGTYPE'][x] == 2):
        L = data['DESCRIPTION'][x].split()
        for y in range(0, len(L)):
            if L[y] in guava:
                L[y] = L[y].replace("'", "")
                data['DEPTH'][x] = L[y]

depth_index = data.loc[(data['EVENTMSGTYPE']==1) | (data['EVENTMSGTYPE']==2)].groupby(by='PLAYER1_ID')[['DEPTH']].mean()
depth_index['Player_ids'] = depth_index.index.values
depth_index['Player_names'] = np.nan
depth_index['Player_teams'] = np.nan
depth_index['Games'] = np.nan
depth_index['FG_percentage'] = np.nan
for x in range(0, len(depth_index)):
    for y in range(0, len(name_set)):
        if depth_index['Player_ids'].iloc[x] == name_set['PLAYER_ID'].iloc[y]:
        # if depth_index['Player_ids'].iloc[x] == name_set['nba_id'].iloc[y]:
        #     beef = name_set['bbref_name'].iloc[y]
            beef = name_set['PLAYER_NAME'].iloc[y]
            depth_index['Player_names'].iloc[x] = beef
            depth_index['Player_teams'].iloc[x] = name_set['TEAM_ABBREVIATION'].iloc[y]
            depth_index['Games'].iloc[x] = name_set['GP'].iloc[y]
            depth_index['FG_percentage'].iloc[x] = name_set['FG_PCT'].iloc[y]

# depth_index['FG_percentage'] = np.nan
# for x in range(0, len(depth_index)):
#     for y in range(0, len(bb_ref_stats)):
#         if depth_index['Player_names'].iloc[x] == bb_ref_stats['Player'].iloc[y]:
#             beef = bb_ref_stats['Field Goal Percentage'].iloc[y]
#             depth_index['FG_percentage'].iloc[x] = beef

depth_index['Shot Count'] = data.loc[(data['EVENTMSGTYPE']==1) | (data['EVENTMSGTYPE']==2)].groupby(by='PLAYER1_ID')[['DEPTH']].count()
depth_index = depth_index.loc[depth_index['Shot Count'] >= 720]
print(depth_index)
# sys.exit()
x = depth_index['DEPTH']
y = depth_index['FG_percentage']


fig, ax = plt.subplots(figsize=(10, 10))

# ax.scatter(x, y)
for i in range(len(depth_index)):
    ax.scatter(depth_index.DEPTH.iloc[i], depth_index.FG_percentage.iloc[i],
               s=(depth_index['Shot Count'].iloc[i])/5, alpha=.7,
               color=COLORS[depth_index['Player_teams'].iloc[i]])

ax.set_xlabel('Average Shot Depth (Feet)', fontsize=14)
ax.set_ylabel('Shooting Percentage', fontsize=14)
ax.set_title('Average Shot Depth vs Shooting Percentage - 2020-2021', fontsize=18)
for x in range(0, len(depth_index['Player_names'])):
    plt.annotate(depth_index['Player_names'].iloc[x], (depth_index['DEPTH'].iloc[x], depth_index['FG_percentage'].iloc[x]), fontsize=6)
# Save the figure as a png
# plt.savefig('epa-vs-pbwr.png', dpi=400)
plt.show()

