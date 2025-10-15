import pandas as pd
import requests
from pathlib import Path
import json
from wakepy import keep
import os
import time

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
    r = requests.get(url, headers=header_data)
    # Convert the data to JSON format
    resp = r.json()
    # Create our dataframe from this JSON
    results = resp['resultSets'][0]
    headers = results['headers']
    rows = results['rowSet']
    frame = pd.DataFrame(rows)
    frame.columns = headers
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
                        all_data = pd.concat([existing_data[columns], combined_new[columns]], ignore_index=True)
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
                existing_data = pd.concat([existing_data[columns], combined_new[columns]], ignore_index=True)
            else:
                existing_data = combined_new[columns]

        existing_data.drop_duplicates(inplace=True)
        save_file(existing_data, dirname, file_short)
        return existing_data
