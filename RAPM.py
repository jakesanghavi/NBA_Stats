import numpy as np
from sklearn import linear_model
import pandas as pd
import requests
from sklearn.feature_extraction import DictVectorizer
import sys

team_id_list = ['1610612737', '1610612738', '1610612751', '1610612766', '1610612741', '1610612739', '1610612742',
                '1610612743', '1610612765', '1610612744', '1610612745', '1610612754', '1610612746', '1610612747',
                '1610612763', '1610612748', '1610612749', '1610612750', '1610612740', '1610612752', '1610612760',
                '1610612753', '1610612755', '1610612756', '1610612757', '1610612758', '1610612759', '1610612761',
                '1610612762', '1610612764']

# headers = {
#     'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'}

headers = {
    'Connection': 'keep-alive',
    'Accept': 'application/json, text/plain, */*',
    'x-nba-stats-token': 'true',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
    'x-nba-stats-origin': 'stats',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'cors',
    'Referer': 'https://stats.nba.com/',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
}

df = pd.DataFrame()
for team_id in team_id_list:
    params = (
        ('Conference', ''),
        ('DateFrom', ''),
        ('DateTo', ''),
        ('Division', ''),
        ('GameID', ''),
        ('GameSegment', ''),
        ('GroupQuantity', '5'),
        ('LastNGames', '0'),
        ('LeagueID', '00'),
        ('Location', ''),
        ('MeasureType', 'Advanced'),
        ('Month', '0'),
        ('OpponentTeamID', '0'),
        ('Outcome', ''),
        ('PORound', '0'),
        ('PaceAdjust', 'N'),
        ('PerMode', 'PerGame'),
        ('Period', '0'),
        ('PlusMinus', 'N'),
        ('Rank', 'N'),
        ('Season', '2020-21'),
        ('SeasonSegment', ''),
        ('SeasonType', 'Regular Season'),
        ('ShotClockRange', ''),
        ('TeamID', team_id),
        ('VsConference', ''),
        ('VsDivision', ''),
    )

    data = requests.get('https://stats.nba.com/stats/leaguedashlineups', headers=headers, params=params).json()
    df = df.append(pd.DataFrame(data['resultSets'][0]['rowSet']))

df.columns = data['resultSets'][0]['headers']

units = []
ORTG = []
DRTG = []
weights = []
for i, name in enumerate(df['GROUP_NAME']):
    temp_string = name.split(' - ')

    home_offense_unit = {name: 1 for name in temp_string}
    units.append(home_offense_unit)
    ORTG.append(df['OFF_RATING'].iloc[i])
    DRTG.append(df['DEF_RATING'].iloc[i])
    weights.append(df['MIN'].iloc[i])

u = DictVectorizer(sparse=False)
u_mat = u.fit_transform(units)
players = u.get_feature_names()

clf = linear_model.RidgeCV(alphas=(np.array([3000])), cv=5)
weights = np.asarray(weights)
clf.fit(u_mat, ORTG, sample_weight=weights)
off_ratings = []
for player in players:
    off_ratings.append((player, clf.coef_[players.index(player)]))

clf.fit(u_mat, DRTG, sample_weight=weights)
def_ratings = []
for player in players:
    def_ratings.append((player, clf.coef_[players.index(player)]))

player_name_list = []
ORAPM_list = []
DRAPM_list = []
for rating in off_ratings:
    player_name_list.append(rating[0])
    ORAPM_list.append(rating[1])
for rating in def_ratings:
    DRAPM_list.append(-rating[1])

RAPM_list = []
for i in range(len(ORAPM_list)):
    RAPM_list.append(ORAPM_list[i] + DRAPM_list[i])
RAPM_dict = {'Player': player_name_list, 'ORAPM': ORAPM_list, 'DRAPM': DRAPM_list, 'RAPM': RAPM_list}
RAPM_df = pd.DataFrame(data=RAPM_dict)
RAPM_df = RAPM_df.sort_values(by=['RAPM'], ascending=False)
print(RAPM_df)

RAPM_df.to_csv('rapm.csv', index=False)