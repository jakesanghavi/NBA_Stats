import pandas as pd
import requests
from pathlib import Path
import json
from wakepy import keep
import os
import ast
import math
import pbp_utils
import numpy as np
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
import pyautogui
import time
from datetime import datetime, date
import urllib3
from unidecode import unidecode
import bs4

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


def normalize_keys(obj):
    if isinstance(obj, dict):
        new_dict = {}
        for k, v in obj.items():
            if not isinstance(k, (str, int, float, bool, type(None))):
                k = str(k)
            new_dict[k] = normalize_keys(v)
        return new_dict
    elif isinstance(obj, list):
        return [normalize_keys(i) for i in obj]
    else:
        return obj


def save_file(data, directory, filename):
    cwd = Path.cwd()

    directory = Path(directory)

    # Handle case where directory already includes cwd or is absolute
    if directory.is_absolute() and str(directory).startswith(str(cwd)):
        data_pack_dir = directory
    else:
        data_pack_dir = cwd / directory

    data_pack_dir.mkdir(parents=True, exist_ok=True)
    full_path = data_pack_dir / filename

    if isinstance(data, pd.DataFrame):
        if not str(full_path).lower().endswith('.csv'):
            full_path = Path(full_path).stem + '.csv'
        data.to_csv(full_path, index=False)
    elif isinstance(data, (dict, list)):
        data = normalize_keys(data)
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


def get_nba_team_id_map():
    return {'ATL': '1610612737', 'BKN': '1610612751', 'BOS': '1610612738', 'CHA': '1610612766', 'CHI': '1610612741',
            'CLE': '1610612739', 'DAL': '1610612742', 'DEN': '1610612743', 'DET': '1610612765', 'GSW': '1610612744',
            'HOU': '1610612745', 'IND': '1610612754', 'LAC': '1610612746', 'LAL': '1610612747', 'MEM': '1610612763',
            'MIA': '1610612748', 'MIL': '1610612749', 'MIN': '1610612750', 'NOP': '1610612740', 'NYK': '1610612752',
            'OKC': '1610612760', 'ORL': '1610612753', 'PHI': '1610612755', 'PHX': '1610612756', 'POR': '1610612757',
            'SAC': '1610612758', 'SAS': '1610612759', 'TOR': '1610612761', 'UTA': '1610612762', 'WAS': '1610612764'
            }


def get_nba_team_abbr_map():
    return {'ATL': 'Atlanta Hawks', 'BKN': 'Brooklyn Nets', 'BOS': 'Boston Celtics', 'CHA': 'Charlotte Hornets',
            'CHI': 'Chicago Bulls', 'CLE': 'Cleveland Cavaliers', 'DAL': 'Dallas Mavericks', 'DEN': 'Denver Nuggets',
            'DET': 'Detroit Pistons', 'GSW': 'Golden State Warriors', 'HOU': 'Houston Rockets', 'IND': 'Indiana Pacers',
            'LAC': 'LA Clippers', 'LAL': 'Los Angeles Lakers', 'MEM': 'Memphis Grizzlies', 'MIA': 'Miami Heat',
            'MIL': 'Milwaukee Bucks', 'MIN': 'Minnesota Timberwolves', 'NOP': 'New Orleans Pelicans',
            'NYK': 'New York Knicks', 'OKC': 'Oklahoma City Thunder', 'ORL': 'Orlando Magic',
            'PHI': 'Philadelphia 76ers', 'PHX': 'Phoenix Suns', 'POR': 'Portland Trail Blazers',
            'SAC': 'Sacramento Kings', 'SAS': 'San Antonio Spurs', 'TOR': 'Toronto Raptors', 'UTA': 'Utah Jazz',
            'WAS': 'Washington Wizards'
            }


def player_stats_url(year):
    """
    Get the endpoint for a given season
    Parameters
    ----------
    year : String
        The season you are interested in

    Returns
    -------
    The NBA Stats endpoint for that season
    """
    return f"https://stats.nba.com/stats/leaguedashplayerstats?College=&Conference=&Country=&DateFrom=&DateTo" \
           f"=&Division=&DraftPick=&DraftYear=&GameScope=&GameSegment=&Height=&LastNGames=0&LeagueID=00&Location" \
           f"=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0" \
           f"&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season=" \
           f"{year}&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0" \
           f"&VsConference=&VsDivision=&Weight="


def extract_data(url, error_counter=0):
    """
    Extract the data stored at a specific URL
    Parameters
    ----------
    url : String
        The connection URL
    error_counter : int
        Counts number of errors

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
        error_counter += 1
        if error_counter < 5:
            extract_data(url, error_counter)
        else:
            print("Request timed out. Skipping...")
        return None
    return frame


def extract_data_urllib(http_client, url):
    """
    Extract data from the API endpoint
    Parameters
    ----------
    http_client : urllib3.PoolManager object
    url : String
        The URL you are interested in

    Returns
    -------
    The dataframe containing the stats of all players for the season you specified.
    """
    # Call to the GET endpoint
    r = http_client.request('GET', url, headers=header_data)
    # Get the JSON
    resp = json.loads(r.data)
    # Convert the json into a pandas dataframe
    results = resp['resultSets'][0]
    headers = results['headers']
    rows = results['rowSet']
    data_frame = pd.DataFrame(rows)
    data_frame.columns = headers
    return data_frame


def get_nba_stats_data(year):
    client = urllib3.PoolManager()
    season = str(year) + '-' + str(year)[2:]

    frame = extract_data_urllib(client, player_stats_url(season))
    frame = frame.dropna(subset=['PLAYER_ID'])
    dirname = "DataPack"
    mini_dir = "NBAStats"
    dirname = Path.cwd() / dirname / mini_dir
    filename = f"nba_stats_{year}.csv"

    save_file(frame, dirname, filename)


def player_totals_page(year):
    """
    Get the URL in BB Ref for the season you are interested in
    Parameters
    ----------
    year : String
        The year you are interested in

    Returns
    -------
    The URL for the season
    """
    return f"https://www.basketball-reference.com/leagues/NBA_{year}_totals.html"


def player_advanced_page(year):
    """
    Get the URL in BB Ref for the season you are interested in
    Parameters
    ----------
    year : String
        The year you are interested in

    Returns
    -------
    The URL for the season
    """
    return f"https://www.basketball-reference.com/leagues/NBA_{year}_advanced.html"


def extract_column_names(table):
    """
    Get the column names from the BB Ref HTML table
    Parameters
    ----------
    table : table header (<th> tag)

    Returns
    -------
    The column names
    """
    cols = [col["aria-label"] for col in table.find_all("thead")[0].find_all("th")][1:]
    cols.append("id")
    return cols


def extract_rows(table):
    """
    Get the rows from the HTML table (get the <tr>'s)
    Parameters
    ----------
    table : HTML table (<table>)

    Returns
    -------
    All rows from the table, properly parsed
    """

    # Get all rows in the table
    trs = table.find_all("tbody")[0].find_all("tr")
    # Parse each row and return
    parsed_rows = [parse_row(row) for row in trs if parse_row(row) is not None and len(parse_row(row)) > 0]
    return parsed_rows


def parse_row(row):
    """
    Parse a given row
    Parameters
    ----------
    row : row of an HTML table (<tr>)

    Returns
    -------
    The data from the row
    """
    # Get all table data elements
    other_data = row.find_all("td")

    # If there is no data, return an empty list
    if len(other_data) == 0:
        return []

    page_links = other_data[0].find_all("a")

    if page_links is None or len(page_links) == 0:
        return None

    # Get the player ids from the URLs associated with them
    ids = page_links[0]["href"].split("/")[-1].replace(".html", "")
    row_data = [td.string for td in other_data]
    row_data.append(ids)
    return row_data


def get_bbref_data(year):
    http = urllib3.PoolManager()
    columns = []
    rows = []

    # Request the page with a GET request
    r = http.request('GET', player_totals_page(year))
    # Use BS4 to parse the page
    soup = bs4.BeautifulSoup(r.data, 'html.parser')
    # Get all <table> elements
    f = soup.find_all("table")

    # Get the columns and rows
    if len(f) > 0:
        columns = extract_column_names(f[0])
        rows = extract_rows(f[0])

    # Convert your data to a pandas dataframe and return it
    frame = pd.DataFrame(rows)
    frame.columns = columns

    # Request the page with a GET request
    r = http.request('GET', player_advanced_page(year))
    # Use BS4 to parse the page
    soup = bs4.BeautifulSoup(r.data, 'html.parser')
    # Get all <table> elements
    f = soup.find_all("table")

    # Get the columns and rows
    if len(f) > 0:
        columns = extract_column_names(f[0])
        rows = extract_rows(f[0])

    # Convert your data to a pandas dataframe and return it
    frame2 = pd.DataFrame(rows)
    frame2.columns = columns

    frame = frame.merge(frame2, on=['Player', 'Pos', 'Age', 'Team', 'id', 'G', 'MP'])

    dirname = "DataPack"
    mini_dir = "BBRef"
    dirname = Path.cwd() / dirname / mini_dir
    filename = f"bbref_totals_{year}.csv"

    save_file(frame, dirname, filename)


def deduplicate_traded_players(group):
    if len(group) > 1:
        return group[group["Team"] == "TOT"]
    return group


def remove_accents(a):
    return unidecode(a)


def id_matching(year):
    dirname = "DataPack"
    nba_stats_dirname = "NBAStats"
    bbref_dirname = "BBRef"
    nba_stats_path = Path.cwd() / dirname / nba_stats_dirname / f"nba_stats_{year}.csv"
    bbref_path = Path.cwd() / dirname / bbref_dirname / f"bbref_totals_{year}.csv"
    bbref_data = pd.read_csv(bbref_path)
    bbref_data['Player'] = bbref_data['Player'].str.replace(".", "", regex=False)
    bbref_data["Player"] = bbref_data["Player"].apply(remove_accents)

    # read out stats.nba.com data
    nba_data = pd.read_csv(nba_stats_path)
    # convert the player id from an int to a string
    nba_data["PLAYER_ID"] = nba_data["PLAYER_ID"].astype(str)
    nba_data["PLAYER_NAME"] = nba_data["PLAYER_NAME"].str.replace(".", "", regex=False)
    nba_data["PLAYER_NAME"] = nba_data["PLAYER_NAME"].apply(remove_accents)

    bbref_base_data = bbref_data[["Player", "id", "Pos", "Team", "FGA", "TRB", "AST"]].groupby(
        by="id").apply(deduplicate_traded_players)

    # take the player name, id, and fields we will use for deduplication from stats.nba.com data
    nba_base_data = nba_data[["PLAYER_ID", "PLAYER_NAME", "FGA", "REB", "AST"]]

    # Perform a full outer join on the two dataframes. This allows us to get all of the exact matches
    name_matches = bbref_base_data.merge(nba_base_data,
                                         left_on=["Player", "FGA", "TRB", "AST"],
                                         right_on=["PLAYER_NAME", "FGA", "REB", "AST"], how="outer")

    # take all the exact matches and rename the columns, we only care about player name and id from each source
    name_matches_ids = name_matches.dropna()
    name_matches_ids = name_matches_ids[["Player", "id", "PLAYER_NAME", "PLAYER_ID", "Pos"]]
    name_matches_ids.columns = ["bbref_name", "bbref_id", "nba_name", "nba_id", 'position']

    name_matches_ids['bbref_name'] = name_matches_ids['bbref_name'].str.lower()
    name_matches_ids['nba_name'] = name_matches_ids['nba_name'].str.lower()

    filename = f"id_matches_{year}.csv"

    save_file(name_matches_ids, dirname, filename)


def get_nba_schedule(year):
    dirname = "DataPack"
    filename = f"nba_schedule_{year}.json"
    filename_full = Path.cwd() / dirname / filename
    if os.path.isfile(filename_full):
        return

    team_id_map = get_nba_team_id_map()
    team_ids = list(team_id_map.values())
    schedule = {}

    driver = webdriver.Chrome()

    for tid in team_ids:
        try:
            driver.get(f"https://www.nba.com/team/{tid}/schedule")  # Example team; replace dynamically if needed

            # Wait for table whose class starts with Crom_Body
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//tbody[starts-with(@class, 'Crom_body')]"))
            )

            # Locate all table rows within that table
            rows = driver.find_elements(By.XPATH, "//tbody[starts-with(@class, 'Crom_body')]//tr")

            for row in rows:
                game_id = row.get_attribute("data-game-id")
                if not game_id:
                    continue  # skip header or malformed rows

                # Get the first <td> (contains text like "Oct 7")
                tds = row.find_elements(By.TAG_NAME, "td")
                if not tds:
                    continue

                date_text = tds[0].text.strip()
                month_str = date_text.split()[0]

                if month_str.lower() in ("oct", "nov", "dec"):
                    actual_year = year
                else:
                    actual_year = year + 1

                try:
                    # Parse "Oct 7" into a date object using the given year
                    date_obj = datetime.strptime(f"{date_text} {actual_year}", "%b %d %Y").date()
                except ValueError:
                    # If parsing fails (e.g., blank cell), skip
                    continue

                if date_obj not in schedule:
                    schedule[date_obj] = []

                if game_id not in schedule[date_obj] and game_id.startswith('002'):
                    schedule[date_obj].append(game_id)

                # Delete useless ones
                if len(schedule[date_obj]) == 0:
                    del schedule[date_obj]
        except Exception as e:
            print(f"Error!: {e}")
        time.sleep(2)

    save_file(schedule, dirname, filename)
    driver.quit()


def get_nba_game_ids(year):
    id_getter = {2020: ("00220", (1, 1081)),
                 2021: ("00221", (1, 1231)),
                 2022: ("00222", (1, 1231)),
                 2023: ("00223", (1, 1231)),
                 2024: ("00224", (1, 1231)),
                 2025: ("00225", (1, 1231))}

    beginning_string = "00" + str(year)[0] + str(year)[2:]

    schedule_path = Path.cwd() / "DataPack" / f"nba_schedule_{year}.json"

    with open(schedule_path, "r") as f:
        schedule = json.load(f)

    # Convert back to time type
    schedule = {
        datetime.strptime(k, "%Y-%m-%d").date(): v
        for k, v in schedule.items()
    }

    schedule = dict(sorted(schedule.items(), key=lambda x: x[0]))

    today = date.today()

    # Filter: only include dates before today
    past_games = {k: v for k, v in schedule.items() if k < today}

    # Flatten all game id lists into one
    all_game_ids = [gid for ids in past_games.values() for gid in ids]

    return beginning_string, all_game_ids


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
        beginning_string, ids = get_nba_game_ids(year)

        game_id = ids[0]

        dirname = Path.cwd() / "DataPack"
        file_short = f"reg_pbp_{year}.csv"

        existing_file = dirname / file_short

        existing_data = None

        new_data_frames = []

        if os.path.isfile(existing_file):
            existing_data = pd.read_csv(existing_file)
            existing_game_ids = existing_data["GAME_ID"].drop_duplicates().tolist()
            existing_game_ids = ["00" + str(item) if type(item) is not str else str(item) for item in existing_game_ids]
            ids = [gid for gid in ids if gid not in existing_game_ids]

        if existing_data is None:
            existing_data = extract_data(play_by_play_url(game_id))
            ids = ids[1:]

        for x in range(len(ids)):
            if error_counter >= 5:
                if new_data_frames:
                    combined_new = pd.concat(new_data_frames, ignore_index=True)
                    if existing_data is not None:
                        all_data = pd.concat([existing_data[columns], combined_new[columns]],
                                             ignore_index=True).drop_duplicates()
                    else:
                        all_data = combined_new[columns]
                    all_data.drop_duplicates(inplace=True)
                    save_file(all_data, dirname, file_short)
                raise IndexError("Too many consecutive errors! Wrong game/s indexed?\n"
                                 "Writing current data and stopping...")
            # Required to have the website not block you. 2 seconds is the shortest sleep period that has worked for me.
            time.sleep(3)

            # Update the game id depending on x
            # This has been changed to read straight from the schedule so may not need these
            # 0 adding shenanigans
            # game_id = beginning_string + "".join(["0" for y in range(5 - len(str(x)))]) + str(x)
            game_id = ids[x]
            try:
                # Extract the pbp data
                holder_play_by_play = extract_data(play_by_play_url(game_id))
                if holder_play_by_play is None:
                    x -= 1
                    error_counter += 1
                    continue
                new_data_frames.append(holder_play_by_play[columns])
                error_counter = 0

            # Catch the case in which the URL doesn't exist (sometimes the game id skips a number)
            except requests.exceptions.JSONDecodeError:
                print(f"Game does not exist for game: {game_id}")
                error_counter += 1
            except ValueError as v:
                print(f"{game_id}: {v}")
                error_counter += 1
            except KeyError:
                print(f"Column/s missing for game: {game_id}")
                error_counter += 1

        # Remove duplicated rows and write to a csv file
        if new_data_frames:
            combined_new = pd.concat(new_data_frames, ignore_index=True)
            if existing_data is not None:
                existing_data = pd.concat([existing_data[columns], combined_new[columns]],
                                          ignore_index=True).drop_duplicates()
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

    substitutions_only = play_by_play[play_by_play['EVENTMSGTYPE'] == 8][
        ['PERIOD', 'EVENTNUM', 'PLAYER1_ID', 'PLAYER2_ID']]
    substitutions_only.columns = ['PERIOD', 'EVENTNUM', 'OUT', 'IN']

    subs_in = split_subs(substitutions_only, 'IN')
    subs_out = split_subs(substitutions_only, 'OUT')

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
        if boxscore is None:
            print(f"Error getting game {game_id}")
            return None
        boxscore_players = extract_data(boxscore)
        if boxscore_players is None:
            print(f"Error getting game {game_id}")
            return None
        else:
            boxscore_players = boxscore_players[['PLAYER_NAME', 'PLAYER_ID', 'TEAM_ID']]
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
    beginning_string, ids = get_nba_game_ids(year)
    game_id = ids[0]

    pap_file_dir = Path.cwd() / "DataPack"
    pap_file_short = f"pap_{year}.csv"
    pap_file_full = pap_file_dir / pap_file_short

    existing_data = None

    new_data_frames = []
    columns = ["GAME_ID", "TEAM_ID_1", "TEAM_1_PLAYERS", "TEAM_ID_2", "TEAM_2_PLAYERS", "PERIOD"]
    error_counter = 0

    if os.path.isfile(pap_file_full):
        existing_data = pd.read_csv(pap_file_full)[columns]
        existing_game_ids = existing_data["GAME_ID"].drop_duplicates().tolist()
        existing_game_ids = ["00" + str(item) if type(item) is not str else str(item) for item in existing_game_ids]
        ids = [gid for gid in ids if gid not in existing_game_ids]

    if existing_data is None:
        existing_data = players_at_period(pbp, game_id)[columns]
        ids = ids[1:]

    with keep.presenting():
        for x in range(len(ids)):
            if error_counter >= 5:
                if new_data_frames:
                    combined_new = pd.concat(new_data_frames, ignore_index=True)
                    if existing_data is not None:
                        existing_data['TEAM_1_PLAYERS'] = existing_data['TEAM_1_PLAYERS'].map(str)
                        existing_data['TEAM_2_PLAYERS'] = existing_data['TEAM_2_PLAYERS'].map(str)
                        all_data = pd.concat([existing_data[columns], combined_new[columns]],
                                             ignore_index=True).drop_duplicates()
                    else:
                        all_data = combined_new[columns]
                    all_data.drop_duplicates(inplace=True)
                    all_data['TEAM_1_PLAYERS'] = all_data['TEAM_1_PLAYERS'].map(ast.literal_eval)
                    all_data['TEAM_2_PLAYERS'] = all_data['TEAM_2_PLAYERS'].map(ast.literal_eval)
                    save_file(all_data, pap_file_dir, pap_file_short)
                raise IndexError("Too many consecutive errors! Wrong game/s indexed?\n"
                                 f"Max gid hit: {x - 1}. Writing current data and stopping...")

            time.sleep(3)
            game_id = ids[x]
            try:
                # Extract the pbp data
                holder_pap = players_at_period(pbp, game_id)
                if holder_pap is None:
                    error_counter = 5
                    continue

                holder_pap['TEAM_1_PLAYERS'] = holder_pap['TEAM_1_PLAYERS'].map(str)
                holder_pap['TEAM_2_PLAYERS'] = holder_pap['TEAM_2_PLAYERS'].map(str)

                # Add this data on to the existing dataframe
                new_data_frames.append(holder_pap[columns])
                error_counter = 0
            # Catch the case in which the URL doesn't exist (sometimes the game id skips a number)
            except requests.exceptions.JSONDecodeError:
                error_counter += 1
                print("Game does not exist")
            except ValueError:
                error_counter += 1
                print(f"Value Error/Missing Game for game id: {game_id}")

    if new_data_frames:
        combined_new = pd.concat(new_data_frames, ignore_index=True)
        if existing_data is not None:
            existing_data['TEAM_1_PLAYERS'] = existing_data['TEAM_1_PLAYERS'].map(str)
            existing_data['TEAM_2_PLAYERS'] = existing_data['TEAM_2_PLAYERS'].map(str)
            existing_data = pd.concat([existing_data[columns], combined_new[columns]],
                                      ignore_index=True).drop_duplicates()
        else:
            existing_data = combined_new[columns]

    existing_data = existing_data.drop_duplicates()
    existing_data['TEAM_1_PLAYERS'] = existing_data['TEAM_1_PLAYERS'].map(ast.literal_eval)
    existing_data['TEAM_2_PLAYERS'] = existing_data['TEAM_2_PLAYERS'].map(ast.literal_eval)

    pap = existing_data.reset_index()
    pap = pap.drop(columns=['level_0', 'index'], errors='ignore')

    save_file(pap, pap_file_dir, pap_file_short)
    return pap


# Players at the start of each period are stored as a string in the dataframe column
# We need to parse out that string into an array of player Ids
def split_row(list_str):
    list_str = str(list_str)
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
    return pbp_utils.is_turnover(row) or (
        pbp_utils.is_last_free_throw_made(ind, row, rows)) or pbp_utils.is_defensive_rebound(ind, row, rows) or \
           pbp_utils.is_make_and_not_and_1(ind, row, rows) or pbp_utils.is_end_of_period(row)


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
    event_data = possession[0]
    period = event_data[pbp_utils.period_column]

    team1_id = event_data['TEAM1_ID']
    team1_players = []
    for i in range(1, 6):
        player_key = f'TEAM1_PLAYER{i}'
        team1_players.append(event_data.get(player_key))
    team1_points = points[team1_id] if team1_id in points else 0

    team2_id = event_data['TEAM2_ID']
    team2_players = []
    for i in range(1, 6):
        player_key = f'TEAM2_PLAYER{i}'
        team2_players.append(event_data.get(player_key))
    team2_points = points[team2_id] if team2_id in points else 0

    possession_team = determine_possession_team(possession[-1], team1_id, team2_id)
    game_id = event_data['GAME_ID']
    event_num = event_data['EVENTNUM']

    return {
        'GAME_ID': game_id,
        'EVENTNUM': event_num,
        'team_1_id': team1_id,
        'team_1_players': team1_players,
        'team_2_id': team2_id,
        'team_2_players': team2_players,
        'game_id': game_id,
        'period': period,
        'possession_start': possession_start,
        'possession_end': possession_end,
        'team_1_points': team1_points,
        'team_2_points': team2_points,
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
    possessions = parse_possessions(sub_map, pbp_rows)

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
    beginning_string, ids = get_nba_game_ids(year)
    # x = 1
    # game_id = beginning_string + "".join(["0" for y in range(5 - len(str(x)))]) + str(x)
    game_id = ids[0]

    dirname = Path.cwd() / "DataPack"
    filename = f"full_reg_pbp_{year}.csv"
    full_filename = dirname / filename

    existing_data = None
    new_data_frames = []
    columns = ['GAME_ID', 'EVENTNUM', 'team_1_id', 'team_1_players', 'team_2_id',
               'team_2_players', 'period', 'possession_start',
               'possession_end', 'team_1_points', 'team_2_points', 'possession_team', 'possession_id']
    error_counter = 0

    if os.path.isfile(full_filename):
        existing_data = pd.read_csv(full_filename)[columns]
        existing_game_ids = existing_data["GAME_ID"].drop_duplicates().tolist()
        existing_game_ids = ["00" + str(item) if type(item) is not str else str(item) for item in existing_game_ids]
        ids = [gid for gid in ids if gid not in existing_game_ids]

    if existing_data is None:
        existing_data = pos_parser(big_pbp, big_pap, game_id)
        existing_data['possession_id'] = existing_data.index.values + 1
        ids = ids[1:]

    with keep.presenting():
        for x in range(len(ids)):
            if error_counter >= 5:
                if new_data_frames:
                    combined_new = pd.concat(new_data_frames, ignore_index=True)
                    combined_new['team_1_players'] = combined_new['team_1_players'].astype(str)
                    combined_new['team_2_players'] = combined_new['team_2_players'].astype(str)
                    if existing_data is not None:
                        existing_data['team_1_players'] = existing_data['team_1_players'].astype(str)
                        existing_data['team_2_players'] = existing_data['team_2_players'].astype(str)
                        all_data = pd.concat([existing_data[columns], combined_new[columns]],
                                             ignore_index=True).drop_duplicates()
                    else:
                        all_data = combined_new[columns]
                    all_data.drop_duplicates(inplace=True)
                    save_file(all_data, dirname, filename)
                raise IndexError("Too many consecutive errors! Wrong game/s indexed?\n"
                                 "Writing current data and stopping...")
            # Update the game id depending on x
            # game_id = beginning_string + "".join(["0" for y in range(5 - len(str(x)))]) + str(x)
            game_id = ids[x]
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
            except KeyError as k:
                print(f"KE: {k} for game id {game_id}")

    existing_data['team_1_players'] = existing_data['team_1_players'].astype(str)
    existing_data['team_2_players'] = existing_data['team_2_players'].astype(str)
    if new_data_frames:
        combined_new = pd.concat(new_data_frames, ignore_index=True)
        combined_new['team_1_players'] = combined_new['team_1_players'].astype(str)
        combined_new['team_2_players'] = combined_new['team_2_players'].astype(str)
        if existing_data is not None:
            existing_data = pd.concat([existing_data[columns], combined_new[columns]],
                                      ignore_index=True).drop_duplicates()
        else:
            existing_data = combined_new[columns]

    # Merge our dataframes together
    poss = big_pbp.merge(existing_data, how="left", on=["GAME_ID", "EVENTNUM"])

    # Forward fill the NA values for the players who are on the court
    poss[columns] = \
        poss[columns].ffill()

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
            poss.at[i, 'team_1_points'] = poss.at[i - 1, 'team_1_points']
            poss.at[i, 'team_2_points'] = poss.at[i - 1, 'team_2_points']
            poss.at[i, 'possession_id'] = poss.at[i - 1, 'possession_id']

    poss['possession_id'] = poss['possession_id'].astype(int)
    save_file(poss, dirname, filename)

    return poss


def get_espn_schedule(year):
    schedule_path = Path.cwd() / "DataPack" / "ESPN" / f"nba_schedule_{year}.json"

    with open(schedule_path, "r") as f:
        schedule = json.load(f)

    # Convert back to time type
    schedule = {
        datetime.strptime(k, "%Y-%m-%d").date(): v
        for k, v in schedule.items()
    }

    schedule = dict(sorted(schedule.items(), key=lambda x: x[0]))

    game_dates = list(schedule.keys())
    driver = webdriver.Chrome()
    hrefs = {}

    for game_date in game_dates:
        hrefs[game_date] = []
        link = f"https://www.espn.com/nba/scoreboard/_/date/{str(game_date).replace('-', '')}"
        print(link)

        driver.get(link)
        wait = WebDriverWait(driver, 20)  # Maximum wait time of 10 seconds
        time.sleep(2)

        pyautogui.hotkey('command', 'option', 'i')

        xpath = '//div[contains(@class, "ScoreCell--md")]'

        score_cells = None

        try:
            score_cells = wait.until(EC.presence_of_all_elements_located((By.XPATH, xpath)))
        except TimeoutException:
            no_games = driver.find_elements(By.CLASS_NAME, 'clr-gray-05')
            print(no_games)
            print(f"{game_date}: No games!")
            if len(no_games) != 0:
                del hrefs[game_date]
                pyautogui.hotkey('command', 'option', 'i')
                continue

        try:
            # Iterate through each instance and get the first link
            for score_cell in score_cells:
                link = score_cell.find_element(By.TAG_NAME, "a")
                hrefs[game_date].append(link.get_attribute("href"))
            pyautogui.hotkey('command', 'option', 'i')

        except StaleElementReferenceException:
            no_games = driver.find_elements(By.CLASS_NAME, 'clr-gray-05')
            print(no_games)
            if len(no_games) != 0:
                del hrefs[game_date]
                pyautogui.hotkey('command', 'option', 'i')
                continue

    driver.quit()

    dirname = "DataPack"
    dirname = Path.cwd() / dirname / "ESPN"
    filename = f"espn_schedule_{year}.json"
    save_file(hrefs, dirname, filename)
    return hrefs


def scrape_espn_data(year, pbp_data):
    dirname = "DataPack"
    existing_file = Path.cwd() / dirname / "ESPN" / f"espn_wp_{year}.csv"
    existing_data = None
    max_existing_date = date(1800, 1, 1)
    if os.path.isfile(existing_file):
        existing_data = pd.read_csv(existing_file)
        print(existing_data['GAME_DATE'])
        max_existing_date = pd.to_datetime(existing_data['GAME_DATE']).dt.date.max()

    espn_schedule_filename = f"espn_schedule_{year}.json"
    full_espn_file = Path.cwd() / dirname / "ESPN"/ espn_schedule_filename

    if os.path.isfile(full_espn_file):
        with open(full_espn_file, "r") as f:
            hrefs = json.load(f)

    else:
        hrefs = get_espn_schedule(year)

    hrefs = {
        datetime.strptime(k, "%Y-%m-%d").date(): v
        for k, v in hrefs.items()
    }

    today = date.today()

    hrefs = {str(k): v for k, v in hrefs.items() if today > k > max_existing_date}

    driver = webdriver.Chrome()
    df_list = []

    teams_per_game = (
        pbp_data
        .dropna(subset=['PLAYER1_TEAM_ID'])
        .groupby('GAME_ID', as_index=False)['PLAYER1_TEAM_ID']
        .apply(lambda x: list(pd.unique(x))[:2])  # first 2 unique teams
        .reset_index()
        .rename(columns={'PLAYER1_TEAM_ID': 'teams'})
    )

    teams_per_game[['team1', 'team2']] = (
        pd.DataFrame(teams_per_game['teams'].tolist(), index=teams_per_game.index)
    )
    unique_games = (
        pbp_data
        .groupby('GAME_ID', as_index=False)
        .head(1)[['GAME_ID']]
    )

    unique_games = unique_games.merge(
        teams_per_game[['GAME_ID', 'team1', 'team2']],
        on='GAME_ID',
        how='inner'
    )

    schedule_path = Path.cwd() / "DataPack" / f"nba_schedule_{year}.json"

    with open(schedule_path, "r") as f:
        schedule = json.load(f)

    schedule = {
        datetime.strptime(k, "%Y-%m-%d").date(): v
        for k, v in schedule.items()
    }

    schedule_merger = pd.DataFrame([
        {'Date': k, 'GAME_ID': v}
        for k, values in schedule.items()
        for v in values
    ])

    schedule_merger = schedule_merger[(schedule_merger['Date'] < today) & (schedule_merger['Date'] > max_existing_date)]

    schedule_merger['GAME_ID'] = schedule_merger['GAME_ID'].astype(int)

    unique_games = unique_games.merge(schedule_merger, on=['GAME_ID'])

    nba_id_map = get_nba_team_id_map()
    nba_id_map = {v: k for k, v in nba_id_map.items()}
    team_ids_inverted = pd.DataFrame(list(nba_id_map.items()), columns=['team1', 'team1_abbr'])
    team_ids_inverted['team1'] = team_ids_inverted['team1'].astype(int)
    unique_games = unique_games.merge(team_ids_inverted, on=['team1'])
    team_ids_inverted.columns = ['team2', 'team2_abbr']
    unique_games = unique_games.merge(team_ids_inverted, on=['team2'])

    nba_team_name_map = get_nba_team_abbr_map()
    team_names_inverted = pd.DataFrame(list(nba_team_name_map.items()),
                                       columns=['team1_abbr', 'team1_name'])

    unique_games = unique_games.merge(team_names_inverted, on=['team1_abbr'])
    team_names_inverted.columns = ['team2_abbr', 'team2_name']
    unique_games = unique_games.merge(team_names_inverted, on=['team2_abbr'])

    with keep.presenting():
        for game_date in list(hrefs.keys()):
            for href in hrefs[game_date]:
                print(href)
                driver.get(href)

                wait = WebDriverWait(driver, 10)  # Maximum wait time of 10 seconds

                win_probability_button = wait.until(
                    EC.element_to_be_clickable((By.XPATH, '//button[contains(text(), "Win Probability")]'))
                )
                win_probability_button.click()

                g_element = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'g[class="recharts-layer recharts-line"]'))
                )

                try:
                    path_element = g_element.find_element(By.TAG_NAME, 'path')
                    d_attribute = path_element.get_attribute('d')
                #         d_attribute = driver.execute_script("return arguments[0].getAttribute('d');", path_element)
                except StaleElementReferenceException:
                    path_element = g_element.find_element(By.TAG_NAME, 'path')
                    d_attribute = path_element.get_attribute('d')

                span_elements = driver.find_elements(By.XPATH, "//a[@data-testid='prism-linkbase']//span")

                span_texts = [e.text for e in span_elements]
                span_texts = [e for e in span_texts if e is not None and len(e.strip()) > 0]

                away_tm = span_texts[0]
                home_tm = span_texts[1]

                path_segments = d_attribute[1:].replace('C',
                                                        ',')  # Skip the first element as it is before the first 'C'

                data_array = np.fromstring(path_segments, sep=',')

                # Reshape the array to have two columns
                reshaped_array = data_array.reshape(-1, 2)

                iteration_df = pd.DataFrame(data=reshaped_array, columns=['time', 'wp_h'])
                iteration_df['GAME_DATE'] = game_date
                iteration_df['away_tm'] = away_tm
                iteration_df['home_tm'] = home_tm

                iteration_df['GAME_ID'] = unique_games[(unique_games['Date'].astype(str) == str(game_date)) &
                                                       (unique_games['team1_name'].isin([away_tm, home_tm])) &
                                                       (unique_games['team2_name'].isin([away_tm, home_tm]))]['GAME_ID'].iloc[0]

                # Append the current iteration's DataFrame to the main DataFrame
                df_list.append(iteration_df)

    driver.quit()

    if len(df_list) == 0:
        print("No new games!")
        return

    df_new = pd.concat(df_list, ignore_index=True, axis=0)
    tm_changes = {'GS': 'GSW', 'NO': 'NOP', 'NY': 'NYK', 'SA': 'SAS', 'UTAH': 'UTA'}
    df_new['away_tm'] = df_new['away_tm'].map(tm_changes).fillna(df_new['away_tm'])
    df_new['home_tm'] = df_new['home_tm'].map(tm_changes).fillna(df_new['home_tm'])

    if existing_data is not None:
        df_new = pd.concat([existing_data, df_new], axis=0)

    dirname = "DataPack"
    dirname = Path.cwd() / dirname / "ESPN"
    filename = f"espn_wp_{year}.csv"
    save_file(df_new, dirname, filename)


def combine_espn_data(year):
    pd.options.mode.chained_assignment = None
    dirname = "DataPack"
    filename = f"full_reg_pbp_{year}.csv"
    poss_path = Path.cwd() / dirname / filename
    poss = pd.read_csv(poss_path)
    poss['sec'] = poss['sec'].astype(float)
    espn_filename = f"espn_wp_{year}.csv"
    espn_path = Path.cwd() / dirname / "ESPN"/ espn_filename
    espn = pd.read_csv(espn_path)
    grouped_poss = poss.groupby('GAME_ID')

    poss['GAME_DATE'] = np.nan
    poss['away_tm'] = ''
    poss['home_tm'] = ''
    poss['road_wp'] = np.nan
    poss['home_wp'] = np.nan
    poss['road_wp_prev'] = np.nan
    poss['home_wp_prev'] = np.nan
    poss['road_wpa'] = np.nan
    poss['home_wpa'] = np.nan

    # Iterate through each group
    for group_name, group_df in grouped_poss:
        # Iterate through rows within the group and fill NA values in columns c and d
        holder = espn.loc[espn['GAME_ID'] == group_df['GAME_ID'].iloc[0]]
        group_df = group_df.drop(
            columns=['GAME_DATE', 'away_tm', 'home_tm', 'home_wp', 'road_wp', 'home_wp_prev', 'road_wp_prev',
                     'home_wpa', 'road_wpa'])

        max_time = group_df['sec'].max()
        max_espn_time = holder['time'].max()
        holder['home_wp'] = round((holder['wp_h'] - 5) / (205 - 5), 3)
        holder['road_wp'] = round(1 - holder['home_wp'], 3)
        holder['sec'] = round(max_time * (holder['time'] - 5) / (max_espn_time - 5), 3)

        holder = holder.drop(columns=['wp_h', 'time'])

        group_df['copy_index'] = group_df.index
        group_df = group_df.merge(holder, on=['GAME_ID', 'sec'], how='left')

        group_df['home_wp_prev'] = group_df['home_wp'].shift(1)
        group_df['road_wp_prev'] = group_df['road_wp'].shift(1)
        group_df['home_wpa'] = round(group_df['home_wp'] - group_df['home_wp_prev'], 3)
        group_df['road_wpa'] = round(group_df['road_wp'] - group_df['road_wp_prev'], 3)

        # Update the original 'poss' dataframe with the modified values in the current group
        group_df.index = group_df['copy_index']
        group_df = group_df.drop(columns=['copy_index'])
        poss.loc[group_df.index] = group_df

    poss[['GAME_DATE', 'away_tm', 'home_tm', 'road_wp', 'home_wp']] = poss[
        ['GAME_DATE', 'away_tm', 'home_tm', 'road_wp', 'home_wp']].ffill()

    filename_out = f"complete_pbp_{year}.csv"

    save_file(poss, dirname, filename_out)


def get_all_data(year, espn):
    get_nba_stats_data(year)
    get_bbref_data(year)
    id_matching(year)
    get_nba_schedule(year)
    base_pbp = scrape_nba_pbp(year)
    base_pap = pap_loop(year, base_pbp)
    possession_parser_loop(year, base_pbp, base_pap)

    if espn:
        scrape_espn_data(year, base_pbp)
        combine_espn_data(year)
