import pandas as pd
from pandas.api.types import CategoricalDtype
from pandas.errors import SettingWithCopyWarning
import warnings

warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)


def def_team_finder(row):
    if row['PLAYER1_TEAM_ABBREVIATION'] == row['team1']:
        return row['team2']
    return row['team1']


def points(row):
    miss = False
    if row['HOMEDESCRIPTION']:
        miss = miss or 'miss' in str(row['HOMEDESCRIPTION']).lower()
    if row['VISITORDESCRIPTION']:
        miss = miss or 'miss' in str(row['VISITORDESCRIPTION']).lower()
    three = False
    if row['HOMEDESCRIPTION']:
        three = three or '3pt' in str(row['HOMEDESCRIPTION']).lower()
    if row['VISITORDESCRIPTION']:
        three = three or '3pt' in str(row['VISITORDESCRIPTION']).lower()
    if miss:
        return 0
    if three:
        return 3
    if row['EVENTMSGTYPE'] == 3:
        return 1
    return 2


def position_handler(row):
    if len(row['position']) <= 2:
        return row['position']
    if row['position'] == 'PG-SG':
        return 'G'
    if row['position'] == 'SF-PF':
        return 'F'
    return row['position'].split('-')[0]


def position_grouper(row):
    return position_handler(row)[::-1][0]


pbp = pd.read_csv("2021_reg_pbp.csv")

pbp = pbp.loc[(pbp['EVENTMSGTYPE'] == 1) | (pbp['EVENTMSGTYPE'] == 3)][[
    'EVENTMSGTYPE', 'GAME_ID', 'PLAYER1_ID', 'PLAYER1_TEAM_ABBREVIATION', 'HOMEDESCRIPTION', 'VISITORDESCRIPTION']]

pbp['points'] = pbp.apply(points, axis=1)
pbp = pbp[['GAME_ID', 'PLAYER1_ID', 'points', 'PLAYER1_TEAM_ABBREVIATION']]

grp = pbp.groupby(by=['GAME_ID'])['PLAYER1_TEAM_ABBREVIATION'].agg(['unique'])
grp.reset_index(inplace=True)
grp.reset_index()
grp.columns = ['GAME_ID', 'teams']
teams_df = grp

grp[['team1', 'team2']] = grp['teams'].apply(lambda tms: pd.Series(','.join(tms).split(',')))
grp = grp.drop(columns=['teams'])

pbp = pbp.merge(grp, on=['GAME_ID'])
pbp['DEF_TEAM'] = pbp[['PLAYER1_TEAM_ABBREVIATION', 'team1', 'team2']].apply(def_team_finder, axis=1)

pbp = pbp.drop(columns=['team1', 'team2'])
pbp = pbp.loc[pbp['points'] > 0]

id_df = pd.read_csv('player_id_matches_2021-2022.csv')[['nba_id', 'position']]
id_df['position'] = id_df.apply(position_handler, axis=1)

pbp = pbp.merge(id_df, left_on=['PLAYER1_ID'], right_on=['nba_id'])
grp = pbp.groupby(by=['GAME_ID', 'DEF_TEAM']).sum(numeric_only=True)[['points']]
grp.reset_index(inplace=True)
grp.columns = ['GAME_ID', 'DEF_TEAM', 'points']

inter_df = []
for x in range(len(teams_df)):
    positions = ['PG', 'SG', 'SF', 'PF', 'C']
    for pos in positions:
        for val in teams_df['teams'].iloc[x]:
            inter_df.append([teams_df['GAME_ID'].iloc[x], 0, val, pos])

inter_df = pd.DataFrame(inter_df, columns=['GAME_ID', 'points', 'DEF_TEAM', 'position'])

pbp_safe = pd.concat([pbp, inter_df])
grp2 = pbp_safe.groupby(by=['GAME_ID', 'DEF_TEAM', 'position']).sum(numeric_only=True)[['points']]
grp2.reset_index(inplace=True)

position_order = CategoricalDtype(['PG', 'SG', 'SF', 'PF', 'C'], ordered=True)
grp2['pos_type'] = grp2['position'].astype(position_order)

grp2 = grp2.sort_values(by=['GAME_ID', 'DEF_TEAM', 'pos_type'], ascending=True).drop(columns=['pos_type'])

for ix, r in grp2.iterrows():
    if grp2['points'].iloc[ix] == 0:
        if grp2['position'].iloc[ix] == 'PG':
            grp2['points'].iloc[ix] = grp2['points'].iloc[ix + 1]
        if grp2['position'].iloc[ix] == 'SG':
            grp2['points'].iloc[ix] = grp2['points'].iloc[ix - 1]
        if grp2['position'].iloc[ix] == 'SF':
            grp2['points'].iloc[ix] = grp2['points'].iloc[ix + 1]
        if grp2['position'].iloc[ix] == 'PF':
            grp2['points'].iloc[ix] = grp2['points'].iloc[ix - 1]
        if grp2['position'].iloc[ix] == 'C':
            grp2['points'].iloc[ix] = grp2['points'].iloc[ix - 1]

grp2['position'] = grp2.apply(position_grouper, axis=1)
grp2 = grp2.groupby(by=['GAME_ID', 'DEF_TEAM', 'position']).sum(numeric_only=True)[['points']]
grp2.reset_index(inplace=True)
grp2.columns = ['GAME_ID', 'DEF_TEAM', 'position', 'pos_points']

final = grp.merge(grp2, on=['GAME_ID', 'DEF_TEAM'])

final.to_csv('pts_allowed_2021-2022.csv', index=False)
