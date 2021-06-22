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

SHOTS = {1:'Normal Jumper', 2:'Running Jumper', 3:'Hook Shot', 5:'Normal Layup',
        6:'Driving Layup', 7:'Normal Dunk', 9:'Driving Dunk', 41:'Running Layup', 43:'Alley-Oop Layup',
        44:'Reverse Layup', 47:'Turnaround Jumper', 50:'Running Dunk', 51:'Reverse Dunk',
        52:'Alley-Oop Dunk', 57:'Driving Hook Shot', 58:'Turnaround Hook Shot', 63:'Fadeaway Jumper',
        66:'Jump Shot (Bank)', 67:'Hook Shot (Bank)', 71:'Finger Roll Layup', 72:'Putback Layup',
        73:'Driving Reverse Layup', 74:'Running Reverse Layup', 75:'Driving Finger Roll Layup',
        76:'Running Finger Roll Layup', 78:'Floater', 79:'Pullup Jumper', 80:'Stepback Jumper',
        86:'Turnaround Fadeaway Jumper', 87:'Putback Dunk', 93:'Driving Hook Shot(Bank)',
        96:'Turnaround Hook Shot (Bank)', 97:'Tip-in Layup', 98:'Cutting Layup',
        99:'Cutting Finger Roll Layup', 100:'Running Alley-Oop Layup', 101:'Driving Floater',
        102:'Driving Floater (Bank)', 103:'Running Pullup Jumper', 104:'Stepback Jumper (Bank)',
        105:'Turnaround Fadeaway Jumper (Bank)', 106:'Running Alley-Oop Dunk', 107:'Tip-in Dunk',
        108:'Cutting Dunk', 109:'Driving Reverse Dunk', 110:'Running Reverse Dunk',
}

data = pd.read_csv("shot_count_by_type.csv")

data = data.drop(columns=['indexer'])
# data = data.dropna(how='any')
# for x in range(0, 150):
df = data.max(axis=1)
df2 = data.idxmax(axis=1)
df = df.dropna(how='any')
df2 = df2.dropna(how='any')

df2 = pd.DataFrame(df2, columns=['Player'])
# df2.rename(index=SHOTS)
df2['Shot Types'] = pd.Series(SHOTS)
df2['Count'] = df

df2.to_csv('shootahs.csv', index=False)

