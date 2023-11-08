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

# Helpful dictionary for shot types
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

# Read the play-by-play data
def read_data(pbp_path):
    df = pd.read_csv(pbp_path, dtype={'GAME_ID': str})
    df = df.loc[df['EVENTMSGTYPE'] == 1]
    df['int_gid'] = df['GAME_ID'].astype(int)
    
    return df

# Get the video links for your plays of interest and return a dataframe with them as a column.
def get_links(df, player_ids_path, output_path, game_start=None, game_end=None, player_name=None, shot_types=None):
    # reduce games to certain window
    if game_start is not None and game_end is not None:
        df = df.loc[(df['int_gid'] >= 22300115) & (df['int_gid'] <= 22300127)]

    # Convert WCTIMESTRING to sortable time values to sort from earliest to latest plays.
    df['time'] = df['WCTIMESTRING'].apply(lambda x: int(x.split(':')[0]) * 60 + int(x.split(':')[1].split()[0]) + (12 * 60 if x.split()[1] == 'AM' else 0))

    # Read in your player names/IDs dataframe
    player_names = pd.read_csv(player_ids_path)

    # If a player is specified, only get the games that that player is in
    if player_name is not None:
        player_id = player_names.loc[player_names['bbref_name'] == player_name.lower()]
        player_id = player_id['nba_id'].iloc[0]

        df = df.loc[df['PLAYER1_ID'] == player_id]

    # Fixes some issues with GAMEID not retaining leading zeros
    for x in range(0, len(df)):
        if not df['GAME_ID'].iloc[x].startswith("0"):
            df['GAME_ID'].iloc[x] = "00" + df['GAME_ID'].iloc[x]

    # Pick only the shots that you wanted
    df_small = df.loc[df['EVENTMSGACTIONTYPE'].isin(shot_types)]

    # If there weren't any shots of the specified type, raise an error
    if len(df_small) == 0:
        raise Exception("No shots of specified type found!")

    # Sort events from first to last, chronologically
    df_small = df_small.sort_values(by=['time'])

    # Create an empty video_link column, and unify the descriptions into one
    df_small['video_link'] = np.nan
    df_small['description'] = (df_small['HOMEDESCRIPTION'].fillna('') +
                               df_small['VISITORDESCRIPTION'].fillna('') +
                               df_small['NEUTRALDESCRIPTION'].fillna(''))

    # Iterate through your dataframe and create the links to scrape
    for x in range(0, len(df_small)):
        df_small['video_link'].iloc[
            x] = f"https://www.nba.com/stats/events?CFID=&CFPARAMS=&GameEventID={df_small['EVENTNUM'].iloc[x]}&GameID={df_small['GAME_ID'].iloc[x]}&Season={year}-{(year % 1000) + 1}&flag=1&title={df_small['description'].iloc[x]}"

    # Replace certain characters with their URL encoding to construct a proper link
    df_small['video_link'] = df_small['video_link'].str.replace(' ', '%20')
    df_small['video_link'] = df_small['video_link'].str.replace("'", '%27')

    # Write your subset of plays to CSV and return
    df_small.to_csv(output_path, index=False)
    return df_small

# This function scrapes videos from all of the constructed links and saves them to files
def video_writer(df):
    # Not all plays have videos. The NBA uses a default video if the real video is missing.
    DEAD_LINK = 'https://videos.nba.com/nba/static/missing.mp4'

    # Depending on your current version of Chrome, the regular install may not work.
    # In this case, specify a previous version of the driver to install
    # driver = webdriver.Chrome(ChromeDriverManager().install())
    driver = webdriver.Chrome(ChromeDriverManager(driver_version='119.0.6045.104').install())

    file_list = []

    # Loop through the dataframe
    for x in range(0, len(df)):
        link = df['video_link'].iloc[x]
             
        # Route the driver to the video link, and wait until the video loads
        driver.get(link)
        wait = WebDriverWait(driver, 10)  # Maximum wait time of 10 seconds
        # Get the source MP4 file in the webpage
        video = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'video.vjs-tech')))
        video_src = video.get_attribute('src')

        # As long as the video exists, write it to a local MP4 file
        if video_src != DEAD_LINK and video_src != '':
            highlight = requests.get(video_src)
            # Save all the video content to a single file
            filename = video_src.split('/')[-1]
            with open(filename, 'wb') as file:
                file_list.append(filename)
                file.write(highlight.content)

    # Close the WebDriver
    driver.quit()

    # Write all of your MP4 file names to a .txt file so they can be stitched together with FFMPEG
    with open("filelist.txt", "w") as filelist:
        for video_file in file_list:
            filelist.write(f"file '{video_file}'\n")

# This function stitches all of the videos together into one called 'merged_video.mp4'
def video_stitcher():
    # Bash command to stitch the videos together
    command = 'ffmpeg -f concat -safe 0 -i filelist.txt -c copy merged_video.mp4'

    # Run the command
    try:
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
    else:
        print("Command executed successfully")

# This function deletes the component videos
def video_cleanup(file_list):
    # Loop through the file names in your text file and delete them
    for video in file_list:
        if video.endswith('.mp4') and not video.endswith("merged_video.mp4"):
            os.remove(video)
