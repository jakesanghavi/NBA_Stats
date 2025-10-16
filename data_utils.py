import pandas as pd
import requests
from pathlib import Path
import json
from wakepy import keep
import os
import time
import ast
import math
import pbp_utils

# Headers for API request
header_data = {
    'Connection': 'keep-alive',
    'Accept': 'application/json, text/plain, */*',
    'x-nba-stats-token': 'true',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/79.0.3945.130 Safari/537.36',
    'x-nba-stats-origin': 'stats',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'cors',
    'Referer': 'https://stats.nba.com/',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
}


def save_file(data, directory, filename):
    cwd = Path.cwd()
    data_pack_dir = cwd / directory

    data_pack_dir.mkdir(parents=True, exist_ok=True)
    full_path = data_pack_dir / filename

    if isinstance(data, pd.DataFrame):
        if not str(full_path).lower().endswith('.csv'):
            full_path = Path(full_path).stem + '.csv'
        data.to_csv(full_path, index=False)
    elif isinstance(data, (dict, list)):
        if not str(full_path).lower().endswith('.json'):
            full_path = Path(full_path).stem + '.json'
        with open(full_path, 'w') as f:
            json.dump(data, f, indent=4)
    else:
        raise TypeError("Unsupported data type. Must be .json or .csv (Pandas DataFrame).")


def play_by_play_url(game_id_str):
    """
    Get the full URL associated with the game id
    Parameters
    ----------
    game_id_str : String

    Returns
    -------
    The URL you need
    """
    return f"https://stats.nba.com/stats/playbyplayv2/?gameId={game_id_str}&startPeriod=0&endPeriod=14"


def extract_data(url):
    """
    Extract the data stored at a specific URL
    Parameters
    ----------
    url : String
        The connection URL

    Returns
    -------
    The data from the URL in the form of a pandas dataframe.
    """
    # I keep this print statement in just so I can see my progress
    print(url)
    # Call to the url
    try:
        r = requests.get(url, headers=header_data, timeout=10)
        # Convert the data to JSON format
        resp = r.json()
        # Create our dataframe from this JSON
        results = resp['resultSets'][0]
        headers = results['headers']
        rows = results['rowSet']
        frame = pd.DataFrame(rows)
        frame.columns = headers

    except requests.exceptions.Timeout:
        print("Request timed out. Skipping...")
        return None
    return frame


def get_nba_game_ids(year):
    id_getter = {2020: ("00220", (1, 1081)),
                 2021: ("00221", (1, 1231)),
                 2022: ("00222", (1, 1231)),
                 2023: ("00223", (1, 1231)),
                 2024: ("00224", (1, 1231)),
                 2025: ("00225", (1, 1231))}

    dict_val = id_getter.get(year)
    beginning_string = "00" + str(year)[0] + str(year)[2:]
    id1 = 1
    id2 = 1231
    if dict_val is not None:
        beginning_string = dict_val[0]
        id1 = dict_val[1][0]
        id2 = dict_val[1][1]

    return beginning_string, id1, id2


def scrape_nba_pbp(year):
    columns = ['GAME_ID', 'EVENTNUM', 'EVENTMSGTYPE', 'EVENTMSGACTIONTYPE', 'PERIOD', 'WCTIMESTRING', 'PCTIMESTRING',
               'HOMEDESCRIPTION', 'NEUTRALDESCRIPTION', 'VISITORDESCRIPTION', 'SCORE', 'SCOREMARGIN', 'PERSON1TYPE',
               'PLAYER1_ID', 'PLAYER1_NAME', 'PLAYER1_TEAM_ID', 'PLAYER1_TEAM_CITY', 'PLAYER1_TEAM_NICKNAME',
               'PLAYER1_TEAM_ABBREVIATION', 'PERSON2TYPE', 'PLAYER2_ID', 'PLAYER2_NAME', 'PLAYER2_TEAM_ID',
               'PLAYER2_TEAM_CITY', 'PLAYER2_TEAM_NICKNAME', 'PLAYER2_TEAM_ABBREVIATION', 'PERSON3TYPE', 'PLAYER3_ID',
               'PLAYER3_NAME', 'PLAYER3_TEAM_ID', 'PLAYER3_TEAM_CITY', 'PLAYER3_TEAM_NICKNAME',
               'PLAYER3_TEAM_ABBREVIATION', 'VIDEO_AVAILABLE_FLAG']

    error_counter = 0

    with keep.presenting():
        # game_id starting string. This changes from year-to-year.
        beginning_string, id1, id2 = get_nba_game_ids(year)

        game_id = beginning_string + "00001"

        dirname = Path.cwd() / "DataPack"
        file_short = f"{year}_reg_pbp.csv"

        existing_file = dirname / file_short

        existing_data = None

        new_data_frames = []

        if os.path.isfile(existing_file):
            existing_data = pd.read_csv(existing_file)
            id1 = existing_data['GAME_ID'].astype(str).str[-5:].astype(int).max() + 1

        if existing_data is None:
            existing_data = extract_data(play_by_play_url(game_id))
            id1 += 1

        for x in range(id1, id2):
            if error_counter >= 5:
                if new_data_frames:
                    combined_new = pd.concat(new_data_frames, ignore_index=True)
                    if existing_data is not None:
                        all_data = pd.concat([existing_data[columns], combined_new[columns]], ignore_index=True).drop_duplicates()
                    else:
                        all_data = combined_new[columns]
                    all_data.drop_duplicates(inplace=True)
                    save_file(all_data, dirname, file_short)
                raise IndexError("Too many consecutive errors! Wrong game/s indexed?\n"
                                 "Writing current data and stopping...")
            # Required to have the website not block you. 2 seconds is the shortest sleep period that has worked for me.
            time.sleep(3)

            # Update the game id depending on x
            game_id = beginning_string + "".join(["0" for y in range(5 - len(str(x)))]) + str(x)
            try:
                # Extract the pbp data
                holder_play_by_play = extract_data(play_by_play_url(game_id))
                new_data_frames.append(holder_play_by_play[columns])
                error_counter = 0

            # Catch the case in which the URL doesn't exist (sometimes the game id skips a number)
            except requests.exceptions.JSONDecodeError:
                print(f"Game does not exist for game: {game_id}")
                error_counter += 1
            except ValueError:
                print(f"Value Error/Missing Game for game: {game_id}")
                error_counter += 1
            except KeyError:
                print(f"Column/s missing for game: {game_id}")
                error_counter += 1

        # Remove duplicated rows and write to a csv file
        if new_data_frames:
            combined_new = pd.concat(new_data_frames, ignore_index=True)
            if existing_data is not None:
                existing_data = pd.concat([existing_data[columns], combined_new[columns]], ignore_index=True).drop_duplicates()
            else:
                existing_data = combined_new[columns]

        existing_data.drop_duplicates(inplace=True)
        save_file(existing_data, dirname, file_short)
        return existing_data


def advanced_boxscore_url(game_id, start, end):
    return f'https://stats.nba.com/stats/boxscoretraditionalv2/?gameId={game_id}&startPeriod=0&endPeriod=14&startRange={start}&endRange={end}&rangeType=2'


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


def frame_to_row(df):
    team1 = df['TEAM_ID'].unique()[0]
    team2 = df['TEAM_ID'].unique()[1]
    players1 = df[df['TEAM_ID'] == team1]['PLAYER_ID'].tolist()
    players1.sort()
    players2 = df[df['TEAM_ID'] == team2]['PLAYER_ID'].tolist()
    players2.sort()

    lst = [team1, players1, team2, players2]

    return lst


def players_at_period(pbp, game_id):
    play_by_play = pbp.loc[pbp['GAME_ID'] == int(game_id)]
    play_by_play.loc[:, 'EVENTNUM'] = play_by_play.index.values

    substitutionsOnly = play_by_play[play_by_play['EVENTMSGTYPE'] == 8][
        ['PERIOD', 'EVENTNUM', 'PLAYER1_ID', 'PLAYER2_ID']]
    substitutionsOnly.columns = ['PERIOD', 'EVENTNUM', 'OUT', 'IN']

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
        if boxscore_players is None:
            print(f"Error getting game {game_id}")
            return None
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


def pap_loop(year, pbp):
    beginning_string, id1, id2 = get_nba_game_ids(year)
    game_id = beginning_string + "00001"

    pap_file_dir = Path.cwd() / "DataPack"
    pap_file_short = f"pap_{year}.csv"
    pap_file_full = pap_file_dir / pap_file_short

    existing_data = None

    new_data_frames = []
    columns = ["GAME_ID", "TEAM_ID_1", "TEAM_1_PLAYERS", "TEAM_ID_2", "TEAM_2_PLAYERS", "PERIOD"]
    error_counter = 0

    if os.path.isfile(pap_file_full):
        existing_data = pd.read_csv(pap_file_full)[columns]
        id1 = existing_data['GAME_ID'].astype(str).str[-5:].astype(int).max() + 1

    if existing_data is None:
        existing_data = players_at_period(pbp, game_id)[columns]
        id1 += 1

    with keep.presenting():
        for x in range(id1, id2):
            if error_counter >= 5:
                if new_data_frames:
                    combined_new = pd.concat(new_data_frames, ignore_index=True)
                    if existing_data is not None:
                        all_data = pd.concat([existing_data[columns], combined_new[columns]], ignore_index=True).drop_duplicates()
                    else:
                        all_data = combined_new[columns]
                    all_data.drop_duplicates(inplace=True)
                    save_file(all_data, pap_file_dir, pap_file_short)
                raise IndexError("Too many consecutive errors! Wrong game/s indexed?\n"
                                 f"Max gid hit: {id1-1}. Writing current data and stopping...")

            time.sleep(2)
            game_id = beginning_string + "".join(["0" for y in range(5 - len(str(x)))]) + str(x)
            try:
                # Extract the pbp data
                holder_pap = players_at_period(pbp, game_id)
                if holder_pap is None:
                    error_counter = 5
                    continue

                # Add this data on to the existing dataframe
                new_data_frames.append(holder_pap[columns])
                error_counter = 0
            # Catch the case in which the URL doesn't exist (sometimes the game id skips a number)
            except requests.exceptions.JSONDecodeError:
                error_counter += 1
                print("Game does not exist")
            except ValueError:
                error_counter += 1
                print("Value Error/Missing Game")

    existing_data['TEAM_1_PLAYERS'] = existing_data['TEAM_1_PLAYERS'].map(str)
    existing_data['TEAM_2_PLAYERS'] = existing_data['TEAM_2_PLAYERS'].map(str)
    existing_data['TEAM_1_PLAYERS'] = existing_data['TEAM_1_PLAYERS'].map(str)
    existing_data['TEAM_2_PLAYERS'] = existing_data['TEAM_2_PLAYERS'].map(str)

    if new_data_frames:
        combined_new = pd.concat(new_data_frames, ignore_index=True)
        if existing_data is not None:
            existing_data = pd.concat([existing_data[columns], combined_new[columns]], ignore_index=True).drop_duplicates()
        else:
            existing_data = combined_new[columns]

    existing_data['TEAM_1_PLAYERS'] = existing_data['TEAM_1_PLAYERS'].map(ast.literal_eval)
    existing_data['TEAM_2_PLAYERS'] = existing_data['TEAM_2_PLAYERS'].map(ast.literal_eval)

    pap = existing_data.reset_index()
    pap = pap.drop(columns=['level_0', 'index'], errors='ignore')

    save_file(pap, pap_file_dir, pap_file_short)
    return pap


# Players at the start of each period are stored as a string in the dataframe column
# We need to parse out that string into an array of player Ids
def split_row(list_str):
    return [x.replace('[', '').replace(']', '').strip() for x in list_str.split(',')]


# We will need to know the game clock at each event later on. Let's take the game clock string (7:34) and convert
# it into seconds elapsed
def parse_time_elapsed(time_str, period):
    # Maximum minutes in a period is 12 unless overtime
    max_minutes = 12 if period < 5 else 5
    # Split string on :
    [minutes, sec] = time_str.split(':')
    # extract minutes and seconds
    minutes = int(minutes)
    sec = int(sec)

    # 7:34 (4 minutes 26 seconds have passed) -> 12 - 7 -> 5, need to subtract an extra minute.
    min_elapsed = max_minutes - minutes - 1
    sec_elapsed = 60 - sec

    return (min_elapsed * 60) + sec_elapsed


# method for calculating time elapsed in a period from a play by play event row
def calculate_time_elapsed_period(row):
    return parse_time_elapsed(row[pbp_utils.game_clock], row[pbp_utils.period_column])


# We will also need to calculate the total time elapsed, not just the time elapsed in the period
def calculate_time_elapsed(row):
    # Caclulate time elapsed in the period
    time_in_period = calculate_time_elapsed_period(row)
    period = row[pbp_utils.period_column]
    # Calculate total time elapsed up to the start of the current period
    if period > 4:
        return (12 * 60 * 4) + ((period - 5) * 5 * 60) + time_in_period
    else:
        return ((period - 1) * 12 * 60) + time_in_period


# players on the court need to be updated after every substitution
def update_subs(sub_map, row):
    # print(row)
    period = row[pbp_utils.period_column]
    # If the event is a substitution we need to sub out the players on the court
    if pbp_utils.is_substitution(row):
        team_id = row[pbp_utils.player1_team_id]
        player_in = str(row[pbp_utils.player2_id])
        player_out = str(row[pbp_utils.player1_id])
        players = sub_map[period][team_id]
        players_index = players.index(player_out)
        players[players_index] = player_in
        players.sort()
        sub_map[period][team_id] = players
    # Once we have subbed the players in/out in our sub map we can add those players to the current event row so
    # that each event has all the players involved with the event included
    for i, k in enumerate(sub_map[period].keys()):
        # print(sub_map[period][k])
        row['TEAM{}_ID'.format(i + 1)] = k
        row['TEAM{}_PLAYER1'.format(i + 1)] = sub_map[period][k][0]
        row['TEAM{}_PLAYER2'.format(i + 1)] = sub_map[period][k][1]
        row['TEAM{}_PLAYER3'.format(i + 1)] = sub_map[period][k][2]
        row['TEAM{}_PLAYER4'.format(i + 1)] = sub_map[period][k][3]
        row['TEAM{}_PLAYER5'.format(i + 1)] = sub_map[period][k][4]


"""
What ends a possession?
1. Made Shot (Need to account for And-1s)
2. Defensive Rebound
3. Turnover
4. Last made free throw  (Ignore FT 1 of 1 on away from play fouls with no made shot)
"""


def is_end_of_possession(ind, row, rows):
    return pbp_utils.is_turnover(row) or (pbp_utils.is_last_free_throw_made(ind, row, rows)) or pbp_utils.is_defensive_rebound(ind, row, rows) or \
           pbp_utils.is_make_and_not_and_1(ind, row, rows) or pbp_utils.is_end_of_period(row)


# The main function of our tutorial, the method to group events by possession
def parse_possessions(sub_map, rows):
    # we will have a list of possessions and each possession will be a list of events
    possessions = []
    current_posession = []
    for ind, row in rows:
        # update our subs
        # print(row)
        update_subs(sub_map, row)
        # No need to include subs or end of period events in our possession list
        if not pbp_utils.is_substitution(row) and not pbp_utils.is_end_of_period(row):
            current_posession.append(row)
        # if the current event is the last event of a possession, add the current possession to our list of
        # possessions and start a new possession
        if is_end_of_possession(ind, row, rows):
            # No need to add empty end of period possessions
            if len(current_posession) > 0:
                possessions.append(current_posession)
            current_posession = []
    return possessions


# We need to count up each team's points from a possession
def count_points(possession):
    # points will be a map where the key is the team id and the value is the number of points scored in that
    # possesion
    points = {}
    for p in possession:
        if pbp_utils.is_made_shot(p) or (not pbp_utils.is_miss(p) and pbp_utils.is_free_throw(p)):
            if p[pbp_utils.player1_team_id] in points:
                points[p[pbp_utils.player1_team_id]] += extract_points(p)
            else:
                points[p[pbp_utils.player1_team_id]] = extract_points(p)
    return points


# We need to know how many points each shot is worth:
def extract_points(p):
    if pbp_utils.is_free_throw(p) and not pbp_utils.is_miss(p):
        return 1
    elif pbp_utils.is_made_shot(p) and pbp_utils.is_three(p):
        return 3
    elif pbp_utils.is_made_shot(p) and not pbp_utils.is_three(p):
        return 2
    else:
        return 0


# We need to determine which team has possession of the ball based on how the possession ended
# If the possession ended with a made shot or free throw then we can determine that the team of the player
# who made the shot was the team with possession of the ball
#
# If the possession ended with a rebound then we can determine that the team that did not get the rebound is
# the team that had possession of the ball (ORBDs do not end possessions)
#
# If the possession ended with a turnover then we can determine that the team that committed the turnover is
# the team that had possession of the ball
#
# If the possession ended due to the end of a period, we probably have some other random event as the last event
# We can assume that the team1 id of that event is the team with the ball
# improvements can be made by handling each event type individually
def determine_possession_team(p, team1, team2):
    if pbp_utils.is_made_shot(p) or pbp_utils.is_free_throw(p):
        return str(int(p[pbp_utils.player1_team_id]))
    elif pbp_utils.is_rebound(p):
        if pbp_utils.is_team_rebound(p):
            if p[pbp_utils.player1_id] == team1:
                return team2
            else:
                return team1
        else:
            if p[pbp_utils.player1_team_id] == team1:
                return team2
            else:
                return team1
    elif pbp_utils.is_turnover(p):
        if pbp_utils.is_team_turnover(p):
            return str(int(p[pbp_utils.player1_id]))
        else:
            return str(int(p[pbp_utils.player1_team_id]))
    else:
        if math.isnan(p[pbp_utils.player1_team_id]):
            return str(int(p[pbp_utils.player1_id]))
        else:
            return str(int(p[pbp_utils.player1_team_id]))


# Parse out the list of events in a possession into a single possession object for this tutorial we will only
# include the players on the court, the game id, period, start and end time of possession, points scored by each
# team, and which team was on offense during the possession.
def parse_possession(possession):
    times_of_events = [p[pbp_utils.time_elapsed] for p in possession]
    possession_start = min(times_of_events)
    possession_end = max(times_of_events)
    points = count_points(possession)
    period = possession[0][pbp_utils.period_column]

    team1_id = possession[0]['TEAM1_ID']
    team1_player1 = possession[0]['TEAM1_PLAYER1']
    team1_player2 = possession[0]['TEAM1_PLAYER2']
    team1_player3 = possession[0]['TEAM1_PLAYER3']
    team1_player4 = possession[0]['TEAM1_PLAYER4']
    team1_player5 = possession[0]['TEAM1_PLAYER5']
    team1_points = points[team1_id] if team1_id in points else 0

    team2_id = possession[0]['TEAM2_ID']
    team2_player1 = possession[0]['TEAM2_PLAYER1']
    team2_player2 = possession[0]['TEAM2_PLAYER2']
    team2_player3 = possession[0]['TEAM2_PLAYER3']
    team2_player4 = possession[0]['TEAM2_PLAYER4']
    team2_player5 = possession[0]['TEAM2_PLAYER5']
    team2_points = points[team2_id] if team2_id in points else 0

    possession_team = determine_possession_team(possession[-1], team1_id, team2_id)
    game_id = possession[0]['GAME_ID']
    event_num = possession[0]['EVENTNUM']

    return {
        'GAME_ID': game_id,
        'EVENTNUM': event_num,
        'team1_id': team1_id,
        'team1_player1': team1_player1,
        'team1_player2': team1_player2,
        'team1_player3': team1_player3,
        'team1_player4': team1_player4,
        'team1_player5': team1_player5,
        'team2_id': team2_id,
        'team2_player1': team2_player1,
        'team2_player2': team2_player2,
        'team2_player3': team2_player3,
        'team2_player4': team2_player4,
        'team2_player5': team2_player5,
        'game_id': game_id,
        'period': period,
        'possession_start': possession_start,
        'possession_end': possession_end,
        'team1_points': team1_points,
        'team2_points': team2_points,
        'possession_team': possession_team
    }


def pos_parser(big_pbp, big_pap, game_id):
    # Read in play by play and fill null description columsn with empty string
    play_by_play = big_pbp.loc[big_pbp['GAME_ID'] == int(game_id)]
    play_by_play = play_by_play.reset_index(drop=True)
    play_by_play[pbp_utils.home_description] = play_by_play[pbp_utils.home_description].fillna("")
    play_by_play[pbp_utils.neutral_description] = play_by_play[pbp_utils.neutral_description].fillna("")
    play_by_play[pbp_utils.away_description] = play_by_play[pbp_utils.away_description].fillna("")

    # Apply the methods for calculating time to add the columns to the dataframe
    play_by_play[pbp_utils.time_elapsed] = play_by_play.apply(calculate_time_elapsed, axis=1)
    play_by_play[pbp_utils.time_elapsed_period] = play_by_play.apply(calculate_time_elapsed_period, axis=1)

    # Read the players at the start of each period
    players_at_start_of_period = big_pap.loc[big_pap['GAME_ID'] == int(game_id)]

    # We need to keep track of substitutions as they happen. To do this we will maintain a map of players on the
    # court at a given moment It will be structured as period -> team_id -> players array
    sub_map = {}
    # Pre-populate the map with the players at the start of each period
    for row in players_at_start_of_period.iterrows():
        sub_map[row[1][pbp_utils.period_column]] = {row[1]['TEAM_ID_1']: split_row(row[1]['TEAM_1_PLAYERS']),
                                          row[1]['TEAM_ID_2']: split_row(row[1]['TEAM_2_PLAYERS'])}

    # convert dataframe into a list of rows. I know there is a better way to do this,
    # but this is the first thing I thought of.
    pbp_rows = list(play_by_play.iterrows())
    possessions = parse_possessions(pbp_rows)

    # Build a list of parsed possession objects
    parsed_possessions = []
    for possession in possessions:
        parsed_possessions.append(parse_possession(possession))

    # Build a dataframe from the list of parsed possession
    df = pd.DataFrame(parsed_possessions)

    discrepancies = df['possession_start'] != df['possession_end'].shift(1)
    rows_with_discrepancies = df[discrepancies]
    for index, row in rows_with_discrepancies.iterrows():
        if index > 0:
            df.at[index, 'possession_start'] = df.at[index - 1, 'possession_end']

    return df


def pc_to_sec(row):
    if row['PERIOD'] <= 4:
        p_time = 720 * (row['PERIOD'] - 1)
        m_time = 60 * (12 - int(row['PCTIMESTRING'].split(':')[0]) - 1)
    else:
        p_time = 720 * 4 + 300 * (row['PERIOD'] - 5)
        m_time = 60 * (5 - int(row['PCTIMESTRING'].split(':')[0]) - 1)
    s_time = 60 - int(row['PCTIMESTRING'].split(':')[1])
    return p_time + m_time + s_time


def possession_parser_loop(year, big_pbp, big_pap):
    # game_id starting string. This changes from year-to-year.
    # game_id = id_getter[year][0] + "000" + str(id_getter[year][1][0]-1)
    beginning_string, id1, id2 = get_nba_game_ids(year)
    x = 1
    game_id = beginning_string + "".join(["0" for y in range(5 - len(str(x)))]) + str(x)

    dirname = Path.cwd() / "DataPack"
    filename = f"full_reg_pbp_{year}.csv"
    full_filename = dirname / filename

    existing_data = None
    new_data_frames = []
    columns = ['GAME_ID', 'EVENTNUM', 'team1_id', 'team1_player1', 'team1_player2',
                 'team1_player3', 'team1_player4', 'team1_player5', 'team2_id',
                 'team2_player1', 'team2_player2', 'team2_player3', 'team2_player4',
                 'team2_player5', 'period', 'possession_start',
                 'possession_end', 'team1_points', 'team2_points', 'possession_team', 'possession_id']
    error_counter = 0

    if os.path.isfile(full_filename):
        existing_data = pd.read_csv(full_filename)[columns]
        id1 = existing_data['GAME_ID'].astype(str).str[-5:].astype(int).max() + 1

    if existing_data is None:
        existing_data = players_at_period(big_pbp, game_id)[columns]
        id1 += 1

    with keep.presenting():
        for x in range(id1, id2):
            if error_counter >= 5:
                if new_data_frames:
                    combined_new = pd.concat(new_data_frames, ignore_index=True)
                    if existing_data is not None:
                        all_data = pd.concat([existing_data[columns], combined_new[columns]], ignore_index=True).drop_duplicates()
                    else:
                        all_data = combined_new[columns]
                    all_data.drop_duplicates(inplace=True)
                    save_file(all_data, dirname, filename)
                raise IndexError("Too many consecutive errors! Wrong game/s indexed?\n"
                                 "Writing current data and stopping...")
            # Update the game id depending on x
            game_id = beginning_string + "".join(["0" for y in range(5 - len(str(x)))]) + str(x)
            try:
                # Extract the pbp data
                holder_poss = pos_parser(big_pbp, big_pap, game_id)
                holder_poss['possession_id'] = holder_poss.index.values + 1
                new_data_frames.append(holder_poss.reset_index()[columns])

            # Catch the case in which the URL doesn't exist (sometimes the game id skips a number)
            except IndexError:
                print(f"IE: Game {game_id} does not exist")
            except ValueError:
                print(f"VE: Game {game_id} does not exist")
            except KeyError:
                print(f"KE: Game {game_id} does not exist")

    if new_data_frames:
        combined_new = pd.concat(new_data_frames, ignore_index=True)
        if existing_data is not None:
            existing_data = pd.concat([existing_data[columns], combined_new[columns]], ignore_index=True).drop_duplicates()
        else:
            existing_data = combined_new[columns]

    # Merge our dataframes together
    poss = big_pbp.merge(existing_data, how="left", on=["GAME_ID", "EVENTNUM"])

    # Forward fill the NA values for the players who are on the court
    poss[columns] = \
        poss[columns].fillna(method="ffill")

    poss = poss.drop_duplicates()

    # Convert PCTIMESTRING into how many seconds into the game we are.
    poss['sec'] = poss.apply(pc_to_sec, axis=1)
    # Create an overarching 'desc' column -- single column to store all of the descriptions
    poss['desc'] = poss['HOMEDESCRIPTION'].fillna('') + poss['NEUTRALDESCRIPTION'].fillna('') + poss[
        'VISITORDESCRIPTION'].fillna('')

    # Events between the starts of possessions that have NA possession stats. This fills in the null values
    # Note that empty possessions will not have their null values filles
    for i in range(1, len(poss)):
        if pd.isna(poss.at[i, 'possession_start']) and poss.at[i - 1, 'possession_end'] >= poss.at[i, 'sec']:
            poss.at[i, 'possession_start'] = poss.at[i - 1, 'possession_start']
            poss.at[i, 'possession_end'] = poss.at[i - 1, 'possession_end']
            poss.at[i, 'team1_points'] = poss.at[i - 1, 'team1_points']
            poss.at[i, 'team2_points'] = poss.at[i - 1, 'team2_points']
            poss.at[i, 'possession_id'] = poss.at[i - 1, 'possession_id']

    save_file(poss, dirname, filename)

    return poss


def get_all_data(year):
    base_pbp = scrape_nba_pbp(year)
    base_pap = pap_loop(year, base_pbp)
    possession_parser_loop(base_pbp, base_pbp, base_pap)
