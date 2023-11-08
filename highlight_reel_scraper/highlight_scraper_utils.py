from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import requests
import numpy as np
import os
import subprocess

pd.set_option('display.max_colwidth', None)
pd.options.mode.chained_assignment = None

SHOTS = {1: 'Normal Jumper', 2: 'Running Jumper', 3: 'Hook Shot', 5: 'Normal Layup',
         6: 'Driving Layup', 7: 'Normal Dunk', 9: 'Driving Dunk', 41: 'Running Layup', 43: 'Alley-Oop Layup',
         44: 'Reverse Layup', 47: 'Turnaround Jumper', 50: 'Running Dunk', 51: 'Reverse Dunk',
         52: 'Alley-Oop Dunk', 57: 'Driving Hook Shot', 58: 'Turnaround Hook Shot', 63: 'Fadeaway Jumper',
         66: 'Jump Shot (Bank)', 67: 'Hook Shot (Bank)', 71: 'Finger Roll Layup', 72: 'Putback Layup',
         73: 'Driving Reverse Layup', 74: 'Running Reverse Layup', 75: 'Driving Finger Roll Layup',
         76: 'Running Finger Roll Layup', 78: 'Floater', 79: 'Pullup Jumper', 80: 'Stepback Jumper',
         86: 'Turnaround Fadeaway Jumper', 87: 'Putback Dunk', 93: 'Driving Hook Shot(Bank)',
         96: 'Turnaround Hook Shot (Bank)', 97: 'Tip-in Layup', 98: 'Cutting Layup',
         99: 'Cutting Finger Roll Layup', 100: 'Running Alley-Oop Layup', 101: 'Driving Floater',
         102: 'Driving Floater (Bank)', 103: 'Running Pullup Jumper', 104: 'Stepback Jumper (Bank)',
         105: 'Turnaround Fadeaway Jumper (Bank)', 106: 'Running Alley-Oop Dunk', 107: 'Tip-in Dunk',
         108: 'Cutting Dunk', 109: 'Driving Reverse Dunk', 110: 'Running Reverse Dunk'
         }

def read_data(year):
    df = pd.read_csv(f"/Users/jakesanghavi/PycharmProjects/NBA/Data/PBP/{year}_reg_pbp.csv", dtype={'GAME_ID': str})
    df = df.loc[df['EVENTMSGTYPE'] == 1]
    df['int_gid'] = df['GAME_ID'].astype(int)
    
    return df

def get_links(df, game_start=None, game_end=None, player_name=None, shot_types=None):
    # reduce games
    if game_start is not None and game_end is not None:
        df = df.loc[(df['int_gid'] >= 22300115) & (df['int_gid'] <= 22300127)]
    df['time'] = df['WCTIMESTRING'].apply(lambda x: int(x.split(':')[0]) * 60 + int(x.split(':')[1].split()[0]) + (12 * 60 if x.split()[1] == 'AM' else 0))

    player_names = pd.read_csv(f"/Users/jakesanghavi/PycharmProjects/NBA/Data/ids/player_id_matches_{year}-{year+1}.csv")

    if player_name is not None:
        player_id = player_names.loc[player_names['bbref_name'] == player_name.lower()]
        player_id = player_id['nba_id'].iloc[0]

        df = df.loc[df['PLAYER1_ID'] == player_id]
        for x in range(0, len(df)):
            if not df['GAME_ID'].iloc[x].startswith("0"):
                df['GAME_ID'].iloc[x] = "00" + df['GAME_ID'].iloc[x]
    
    df_small = df.loc[df['EVENTMSGACTIONTYPE'].isin(shot_types)]
    
    if len(df_small) == 0:
        return "No shots of the specified type/s found!"

    df_small = df_small.sort_values(by=['time'])

    df_small['video_link'] = np.nan
    df_small['description'] = (df_small['HOMEDESCRIPTION'].fillna('') +
                               df_small['VISITORDESCRIPTION'].fillna('') +
                               df_small['NEUTRALDESCRIPTION'].fillna(''))

    for x in range(0, len(df_small)):
        df_small['video_link'].iloc[
            x] = f"https://www.nba.com/stats/events?CFID=&CFPARAMS=&GameEventID={df_small['EVENTNUM'].iloc[x]}&GameID={df_small['GAME_ID'].iloc[x]}&Season={year}-{(year % 1000) + 1}&flag=1&title={df_small['description'].iloc[x]}"

    df_small['video_link'] = df_small['video_link'].str.replace(' ', '%20')
    df_small['video_link'] = df_small['video_link'].str.replace("'", '%27')

    df_small.to_csv("/Users/jakesanghavi/PycharmProjects/NBA/Data/highlight_reel.csv", index=False)
    return df_small

def video_writer():
    DEAD_LINK = 'https://videos.nba.com/nba/static/missing.mp4'

    # Depending on your current version of Chrome, the regular install may not work.
    # In this case, specify a previous version of the driver to install
    # driver = webdriver.Chrome(ChromeDriverManager().install())
    driver = webdriver.Chrome(ChromeDriverManager(driver_version='119.0.6045.104').install())

    df = pd.read_csv("/Users/jakesanghavi/PycharmProjects/NBA/Data/highlight_reel.csv")
    file_list = []

    for x in range(0, len(df)):
        link = df['video_link'].iloc[x]
        driver.get(link)
        wait = WebDriverWait(driver, 10)  # Maximum wait time of 10 seconds
        video = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'video.vjs-tech')))
        video_src = video.get_attribute('src')

        print(video_src)
        if video_src != DEAD_LINK and video_src != '':
            highlight = requests.get(video_src)
            # Save all the video content to a single file
            filename = video_src.split('/')[-1]
            with open(filename, 'wb') as file:
                file_list.append(filename)
                file.write(highlight.content)

    # Close the WebDriver
    driver.quit()

    with open("filelist.txt", "w") as filelist:
        for video_file in file_list:
            filelist.write(f"file '{video_file}'\n")

def video_stitcher():
    command = 'ffmpeg -f concat -safe 0 -i filelist.txt -c copy merged_video.mp4'

    try:
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
    else:
        print("Command executed successfully")


def video_cleanup(file_list):
    for video in file_list:
        if video.endswith('.mp4') and not video.endswith("merged_video.mp4"):
            os.remove(video)
