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
data = pd.read_csv("rapm.csv")
# name_set = pd.read_csv("player_id_matches_2021.csv")
name_set = pd.read_csv("stats_nba_player_data_2020-21.csv")
bb_ref_stats = pd.read_csv("basketball_reference_totals_2021.csv")

data['MP'] = np.nan
data['TEAM'] = np.nan
data['GP'] = np.nan

for x in range(0, len(data)):
    nombre = data['Player'].iloc[x]
    for y in range(0, len(name_set)):
        nombre_dos = name_set['PLAYER_NAME'].iloc[y]
        nombre2 = nombre_dos[nombre_dos.find(' '):]
        nombre_dos = str(name_set['PLAYER_NAME'].iloc[y])[0]
        nombre2_final = nombre_dos + '.' + nombre2
        if nombre2_final == nombre:
            data['MP'].iloc[x] = name_set['MIN'].iloc[y]
            data['TEAM'].iloc[x] = name_set['TEAM_ABBREVIATION'].iloc[y]
            data['GP'].iloc[x] = name_set['GP'].iloc[y]

data = data.loc[data['MP']/data['GP'] >= 30]
print(data)

# sys.exit()
x = data['ORAPM']
y = data['DRAPM']


fig, ax = plt.subplots(figsize=(10, 10))

plt.axhline(y=0, c='k', linewidth=1.0, linestyle='dashed')
plt.axvline(x=0, c='k', linewidth=1.0, linestyle='dashed')
nx = np.linspace(-1.5,2,100)
plt.plot(nx,nx*-1, '--r')
ax.text(1.5, -1.4, "↑ Above the line\n↑ is good\n↑ (+ RAPM)", c='r')
ax.text(-1.5, 0.8, "↓ Below the line\n↓ is bad\n↓ (- RAPM)", c='r')

# ax.scatter(x, y)
for i in range(len(data)):
    ax.scatter(data.ORAPM.iloc[i], data.DRAPM.iloc[i],
               s=(data['MP'].iloc[i])/10, alpha=.7,
               color=COLORS[data['TEAM'].iloc[i]])

ax.set_xlabel('ORAPM', fontsize=14)
ax.set_ylabel('DRAPM', fontsize=14)
ax.set_aspect('equal')
ax.set_title('Offensive and Defensive RAPM (Minimum 30 MP Per Team Game) - 2020-2021', fontsize=18)
for x in range(0, len(data['Player'])):
    plt.annotate(data['Player'].iloc[x], (data['ORAPM'].iloc[x], data['DRAPM'].iloc[x]), fontsize=6)
# Save the figure as a png
# plt.savefig('epa-vs-pbwr.png', dpi=400)
plt.show()

