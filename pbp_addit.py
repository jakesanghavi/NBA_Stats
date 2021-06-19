import pandas as pd
import requests
import time

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

# Game Id
game_id = "0022000001"

# Headers for API Request
header_data  = {
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


# build play by play url
def play_by_play_url(game_id):
   return "https://stats.nba.com/stats/playbyplayv2/?gameId={0}&startPeriod=0&endPeriod=14".format(game_id)


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


###
### Download and Save Play by Play Data
###

play_by_play = extract_data(play_by_play_url(game_id))
# play_by_play['DESCRIPTION'] = play_by_play['HOMEDESCRIPTION'] + ' ' + play_by_play['NEUTRALDESCRIPTION'] + ' ' + play_by_play['VISITORDESCRIPTION']

holder = "00220"
for x in range(391, 1069):
   time.sleep(2)
   excess = ""
   if(x < 10):
       excess = "0000" + str(x)
   elif(x < 100):
       excess = "000" + str(x)
   elif(x < 1000):
       excess = "00" + str(x)
   else:
       excess = "0" + str(x)
   holder = "00220" + excess
   game_id = holder
   try:
      holder_play_by_play = extract_data(play_by_play_url(game_id))

   # holder_play_by_play['DESCRIPTION'] = holder_play_by_play['HOMEDESCRIPTION'] + ' ' + holder_play_by_play['NEUTRALDESCRIPTION'] + ' ' + holder_play_by_play['VISITORDESCRIPTION']

      play_by_play = pd.concat([play_by_play, holder_play_by_play], axis=0).reset_index()[['GAME_ID', 'EVENTNUM', 'EVENTMSGTYPE', 'EVENTMSGACTIONTYPE', 'PERIOD', 'WCTIMESTRING', 'PCTIMESTRING', 'HOMEDESCRIPTION', 'NEUTRALDESCRIPTION', 'VISITORDESCRIPTION', 'SCORE', 'SCOREMARGIN', 'PERSON1TYPE', 'PLAYER1_ID', 'PLAYER1_NAME', 'PLAYER1_TEAM_ID', 'PLAYER1_TEAM_CITY', 'PLAYER1_TEAM_NICKNAME', 'PLAYER1_TEAM_ABBREVIATION', 'PERSON2TYPE', 'PLAYER2_ID', 'PLAYER2_NAME', 'PLAYER2_TEAM_ID', 'PLAYER2_TEAM_CITY', 'PLAYER2_TEAM_NICKNAME', 'PLAYER2_TEAM_ABBREVIATION', 'PERSON3TYPE', 'PLAYER3_ID', 'PLAYER3_NAME', 'PLAYER3_TEAM_ID', 'PLAYER3_TEAM_CITY', 'PLAYER3_TEAM_NICKNAME', 'PLAYER3_TEAM_ABBREVIATION', 'VIDEO_AVAILABLE_FLAG']]
   except:
      print("Game does not exist")
   # play_by_play['DESCRIPTION'] = play_by_play['HOMEDESCRIPTION'] + ' ' + play_by_play['NEUTRALDESCRIPTION'] + ' ' + \
   #                             play_by_play['VISITORDESCRIPTION']

# If you already have an old pbp file, read it in here
# old_data = pd.read_csv("pbp_0210.csv")

# If you have an old file, you'll want to not include the first game as it would be double counted
# play_by_play = play_by_play.loc[play_by_play['GAME_ID'] != "0022000001"]

# Concatenates your old file and new file, if needed
# play_by_play = pd.concat([old_data, play_by_play], axis=0).reset_index()[['GAME_ID', 'EVENTNUM', 'EVENTMSGTYPE', 'EVENTMSGACTIONTYPE', 'PERIOD', 'WCTIMESTRING', 'PCTIMESTRING', 'HOMEDESCRIPTION', 'NEUTRALDESCRIPTION', 'VISITORDESCRIPTION', 'SCORE', 'SCOREMARGIN', 'PERSON1TYPE', 'PLAYER1_ID', 'PLAYER1_NAME', 'PLAYER1_TEAM_ID', 'PLAYER1_TEAM_CITY', 'PLAYER1_TEAM_NICKNAME', 'PLAYER1_TEAM_ABBREVIATION', 'PERSON2TYPE', 'PLAYER2_ID', 'PLAYER2_NAME', 'PLAYER2_TEAM_ID', 'PLAYER2_TEAM_CITY', 'PLAYER2_TEAM_NICKNAME', 'PLAYER2_TEAM_ABBREVIATION', 'PERSON3TYPE', 'PLAYER3_ID', 'PLAYER3_NAME', 'PLAYER3_TEAM_ID', 'PLAYER3_TEAM_CITY', 'PLAYER3_TEAM_NICKNAME', 'PLAYER3_TEAM_ABBREVIATION', 'VIDEO_AVAILABLE_FLAG']]

play_by_play.to_csv("2020_reg_pbp.csv", index=False)