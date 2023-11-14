import os
import ast
import pandas as pd
import requests
import time
from wakepy import keepawake

pd.set_option('display.max_columns', 5)
pd.set_option('display.width', 1000)
pd.options.mode.chained_assignment = None  # default='warn'

# ID range by year for NBA games. Note that the current year ID range must be updated each day
id_getter = {2020: ("00220", (2, 1080)), 2021: ("00221", (2, 1231)), 2022: ("00222", (2, 942)),
             2023: ("00223", (62, 192))}

play_in_range = {2023: (1,17)}

# Play-in tournament games began in 2023. We need two loops to handle this. If there are no play-in games
# for a year, skip the loop.
play_in = {2020: False, 2021: False, 2022: False, 2023: True}

year = 2023
# Load your PBP path
PLAY_BY_PLAY = pd.read_csv('/Users/jakesanghavi/PycharmProjects/NBA/Data/PBP/2023_reg_pbp.csv')

# Headers for API Request
header_data = {
    'Host': 'stats.nba.com',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
    'Referer': 'stats.nba.com',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
}


# Build advanced boxscore url
def advanced_boxscore_url(game_id, start, end):
    return 'https://stats.nba.com/stats/boxscoretraditionalv2/?gameId={0}&startPeriod=0&endPeriod=14&startRange={1}&endRange={2}&rangeType=2'.format(
        game_id, start, end)


# Helper functions
def calculate_time_at_period(period):
    if period > 5:
        return (720 * 4 + (period - 5) * (5 * 60)) * 10
    else:
        return (720 * (period - 1)) * 10


def split_subs(df, tag):
    subs = df[[tag, 'PERIOD', 'EVENTNUM']]
    subs['SUB'] = tag
    subs.columns = ['PLAYER_ID', 'PERIOD', 'EVENTNUM', 'SUB']
    return subs

# Convert DF to list
def frame_to_row(df):
    team1 = df['TEAM_ID'].unique()[0]
    team2 = df['TEAM_ID'].unique()[1]
    players1 = df[df['TEAM_ID'] == team1]['PLAYER_ID'].tolist()
    players1.sort()
    players2 = df[df['TEAM_ID'] == team2]['PLAYER_ID'].tolist()
    players2.sort()

    lst = [team1, players1, team2, players2]

    return lst


# extracts data from api response
def extract_data(url):
    print(url)
    r = requests.get(url, headers=header_data)
    resp = r.json()
    results = resp['resultSets'][0]
    headers = results['headers']
    rows = results['rowSet']
    frame = pd.DataFrame(rows)
    frame.columns = headers
    return frame

# Find players who started each period on the court
def players_at_period(game_id):
    # Find the game just at your specified game ID.
    play_by_play = PLAY_BY_PLAY.loc[PLAY_BY_PLAY['GAME_ID'] == int(game_id)]

    # Some games have EVENTNUM out of order. Using the index values avoids this.
    play_by_play['EVENTNUM'] = play_by_play.index.values

    # Get only substitution events
    substitutionsOnly = play_by_play[play_by_play['EVENTMSGTYPE'] == 8][
        ['PERIOD', 'EVENTNUM', 'PLAYER1_ID', 'PLAYER2_ID']]
    substitutionsOnly.columns = ['PERIOD', 'EVENTNUM', 'OUT', 'IN']

    # Break into subbing in and out
    subs_in = split_subs(substitutionsOnly, 'IN')
    subs_out = split_subs(substitutionsOnly, 'OUT')

    full_subs = pd.concat([subs_out, subs_in], axis=0).reset_index()[['PLAYER_ID', 'PERIOD', 'EVENTNUM', 'SUB']]
    first_event_of_period = full_subs.loc[full_subs.groupby(by=['PERIOD', 'PLAYER_ID'])['EVENTNUM'].idxmin()]
    players_subbed_in_at_each_period = first_event_of_period[first_event_of_period['SUB'] == 'IN'][
        ['PLAYER_ID', 'PERIOD', 'SUB']]

    periods = players_subbed_in_at_each_period['PERIOD'].drop_duplicates().values.tolist()

    rows = []
    for period in periods:
        low = calculate_time_at_period(period) + 5
        high = calculate_time_at_period(period + 1) - 5
        boxscore = advanced_boxscore_url(game_id, low, high)
        boxscore_players = extract_data(boxscore)[['PLAYER_NAME', 'PLAYER_ID', 'TEAM_ID']]
        boxscore_players['PERIOD'] = period

        players_subbed_in_at_period = players_subbed_in_at_each_period[
            players_subbed_in_at_each_period['PERIOD'] == period]

        joined_players = pd.merge(boxscore_players, players_subbed_in_at_period, on=['PLAYER_ID', 'PERIOD'], how='left')
        joined_players = joined_players[pd.isnull(joined_players['SUB'])][
            ['PLAYER_NAME', 'PLAYER_ID', 'TEAM_ID', 'PERIOD']]
        row = frame_to_row(joined_players)
        row.append(period)
        rows.append(row)

    players_on_court_at_start_of_period = pd.DataFrame(rows)
    cols = ['TEAM_ID_1', 'TEAM_1_PLAYERS', 'TEAM_ID_2', 'TEAM_2_PLAYERS', 'PERIOD']
    players_on_court_at_start_of_period.columns = cols
    players_on_court_at_start_of_period['GAME_ID'] = int(game_id)
    return players_on_court_at_start_of_period


game_id = id_getter[year][0] + "000" + str(id_getter[year][1][0]-1)

pap = players_at_period(game_id)
with keepawake(keep_screen_awake=False):
    for x in range(id_getter[year][0], id_getter[year][1]):
        time.sleep(3)
        game_id = id_getter[year][0] + "".join(["0" for y in range(5 - len(str(x)))]) + str(x)
        try:
            # Extract the pbp data
            holder_pap = players_at_period(game_id)

            # Add this data on to the existing dataframe
            pap = pd.concat([pap, holder_pap]).reset_index()[
                ["GAME_ID", "TEAM_ID_1", "TEAM_1_PLAYERS", "TEAM_ID_2", "TEAM_2_PLAYERS", "PERIOD"]]

        # Catch the case in which the URL doesn't exist (sometimes the game id skips a number)
        except requests.exceptions.JSONDecodeError:
            print("Game does not exist")
        except ValueError:
            print("Value Error/Missing Game")

    if play_in[year]:
        for x in range(play_in_range[year][0], play_in_range[year][1]):
            time.sleep(3)
            game_id = id_getter[year][0] + "".join(["0" for y in range(5 - len(str(x)))]) + str(x)
            try:
                # Extract the pbp data
                holder_pap = players_at_period(game_id)

                # Add this data on to the existing dataframe
                pap = pd.concat([pap, holder_pap]).reset_index()[
                    ["GAME_ID", "TEAM_ID_1", "TEAM_1_PLAYERS", "TEAM_ID_2", "TEAM_2_PLAYERS", "PERIOD"]]

            # Catch the case in which the URL doesn't exist (sometimes the game id skips a number)
            except requests.exceptions.JSONDecodeError:
                print("Game does not exist")
            except ValueError:
                print("Value Error/Missing Game")

# Save the result to CSV
pap.to_csv(f"{year}_pap.csv", index=False)
