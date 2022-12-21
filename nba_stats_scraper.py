import json
import pandas as pd
import urllib3

# Header data to connect to the API
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
           f"&VsConference=&VsDivision=&Weight= "


def extract_data(http_client, url):
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


client = urllib3.PoolManager()
season = "2022-23"

frame = extract_data(client, player_stats_url(season))

frame.to_csv(f"stats_nba_player_data_{season}.csv", index=False)
