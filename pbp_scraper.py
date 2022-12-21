import pandas as pd
import requests
import time
from wakepy import keepawake

# Prevent your computer from sleeping. The script can take a couple of hours to complete if scraping many games due
# to the long wait time between each API call. On some systems, this stops the script from running.
with keepawake(keep_screen_awake=False):
    # game_id starting string. This changes from year-to-year.
    game_id = "0022200001"

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

    # Extract the data from the first game. Subsequent games will be concatenated onto this dataframe.
    play_by_play = extract_data(play_by_play_url(game_id))

    for x in range(2, 466):
        # Required to have the website not block you. 2 seconds is the shortest sleep period that has worked for me.
        time.sleep(3)

        # Update the game id depending on x
        game_id = "00222" + "".join(["0" for y in range(5 - len(str(x)))]) + str(x)
        try:
            # Extract the pbp data
            holder_play_by_play = extract_data(play_by_play_url(game_id))

            # Add this data on to the existing dataframe
            play_by_play = pd.concat([play_by_play, holder_play_by_play]).reset_index()[
                ['GAME_ID', 'EVENTNUM', 'EVENTMSGTYPE', 'EVENTMSGACTIONTYPE', 'PERIOD', 'WCTIMESTRING', 'PCTIMESTRING',
                 'HOMEDESCRIPTION', 'NEUTRALDESCRIPTION', 'VISITORDESCRIPTION', 'SCORE', 'SCOREMARGIN', 'PERSON1TYPE',
                 'PLAYER1_ID', 'PLAYER1_NAME', 'PLAYER1_TEAM_ID', 'PLAYER1_TEAM_CITY', 'PLAYER1_TEAM_NICKNAME',
                 'PLAYER1_TEAM_ABBREVIATION', 'PERSON2TYPE', 'PLAYER2_ID', 'PLAYER2_NAME', 'PLAYER2_TEAM_ID',
                 'PLAYER2_TEAM_CITY', 'PLAYER2_TEAM_NICKNAME', 'PLAYER2_TEAM_ABBREVIATION', 'PERSON3TYPE', 'PLAYER3_ID',
                 'PLAYER3_NAME', 'PLAYER3_TEAM_ID', 'PLAYER3_TEAM_CITY', 'PLAYER3_TEAM_NICKNAME',
                 'PLAYER3_TEAM_ABBREVIATION', 'VIDEO_AVAILABLE_FLAG']]

        # Catch the case in which the URL doesn't exist (sometimes the game id skips a number)
        except requests.exceptions.JSONDecodeError:
            print("Game does not exist")
        except ValueError:
            print("Value Error/Missing Game")
    # Use the below two lines to create one overall description column, if desired.
    # play_by_play['DESCRIPTION'] = play_by_play['HOMEDESCRIPTION'] + ' ' + play_by_play['NEUTRALDESCRIPTION'] + ' ' + \
    #                               play_by_play['VISITORDESCRIPTION']

    # If you already have an old pbp file, read it in here:
    # old_data = pd.read_csv("2022_reg_pbp.csv")

    # Concatenates your old file and new file, if needed
    # play_by_play = pd.concat([old_data, play_by_play]).reset_index()[['GAME_ID', 'EVENTNUM', 'EVENTMSGTYPE',
    #                                                                   'EVENTMSGACTIONTYPE', 'PERIOD',
    #                                                                   'WCTIMESTRING', 'PCTIMESTRING',
    #                                                                   'HOMEDESCRIPTION', 'NEUTRALDESCRIPTION',
    #                                                                   'VISITORDESCRIPTION', 'SCORE', 'SCOREMARGIN',
    #                                                                   'PERSON1TYPE', 'PLAYER1_ID', 'PLAYER1_NAME',
    #                                                                   'PLAYER1_TEAM_ID', 'PLAYER1_TEAM_CITY',
    #                                                                   'PLAYER1_TEAM_NICKNAME',
    #                                                                   'PLAYER1_TEAM_ABBREVIATION', 'PERSON2TYPE',
    #                                                                   'PLAYER2_ID', 'PLAYER2_NAME',
    #                                                                   'PLAYER2_TEAM_ID', 'PLAYER2_TEAM_CITY',
    #                                                                   'PLAYER2_TEAM_NICKNAME',
    #                                                                   'PLAYER2_TEAM_ABBREVIATION', 'PERSON3TYPE',
    #                                                                   'PLAYER3_ID', 'PLAYER3_NAME',
    #                                                                   'PLAYER3_TEAM_ID', 'PLAYER3_TEAM_CITY',
    #                                                                   'PLAYER3_TEAM_NICKNAME',
    #                                                                   'PLAYER3_TEAM_ABBREVIATION',
    #                                                                   'VIDEO_AVAILABLE_FLAG']]

    # Remove duplicated rows and write to a csv file
    play_by_play.drop_duplicates().to_csv("2022_reg_pbp.csv", index=False)
