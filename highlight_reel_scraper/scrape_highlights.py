from highlight_scraper_utils.py import *

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

# Read the dataframe from your specified year
df = read_data(2023)

game_start=22300115
game_end=22300127
player_name='Luka Doncic'

# HELPFUL SHOT TYPES: [80, 104] - stepback, [43, 52, 100, 106] - alley-oop
shot_types = [80, 104]

# Get the video links for your desired plays
df = get_links(df, game_start=game_start, game_end=game_end, player_name=player_name, shot_types=shot_types)

# Write the video for each play to a file. Save the names of these files so they can be deleted later
files = video_writer(df)

# Stitch the videos together using FFMPEG. The saved file will be called 'merged_video.mp4'
video_stitcher()

# Delete the individual play videos
video_cleanup(file_list)
