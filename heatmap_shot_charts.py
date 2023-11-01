import requests
import matplotlib.pyplot as plt
from matplotlib.patches import Circle, Rectangle, Arc
from matplotlib.offsetbox import OffsetImage
import pandas as pd
import seaborn as sns
import urllib.request
from matplotlib import rcParams
import numpy as np

rcParams['font.family'] = 'serif'

year = 2023

player_names = pd.read_csv(f"Data/ids/player_id_matches_{year}-{year + 1}.csv")

player_name = input("Input player name: ").lower()
player_id = player_names.loc[player_names['bbref_name'] == player_name]
player_id = str(player_id['nba_id'].iloc[0])

player_season = str(year) + '-' + str((year % 1000) + 1)

header_data = {
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


def draw_court(ax=None, color='black', lw=2, outer_lines=True):
    # If an axes object isn't provided to plot onto, just get current one
    if ax is None:
        ax = plt.gca()
    hoop = Circle((0, 0), radius=7.5, linewidth=lw, color=color, fill=False)
    # Create backboard
    backboard = Rectangle((-30, -7.5), 60, -1, linewidth=lw, color=color)
    # The paint
    # Create the outer box 0f the paint, width=16ft, height=19ft
    outer_box = Rectangle((-80, -47.5), 160, 190, linewidth=lw, color=color,
                          fill=False)
    # Create the inner box of the paint, widt=12ft, height=19ft
    inner_box = Rectangle((-60, -47.5), 120, 190, linewidth=lw, color=color,
                          fill=False)
    # Create free throw top arc
    top_free_throw = Arc((0, 142.5), 120, 120, theta1=0, theta2=180,
                         linewidth=lw, color=color, fill=False)
    # Create free throw bottom arc
    bottom_free_throw = Arc((0, 142.5), 120, 120, theta1=180, theta2=0,
                            linewidth=lw, color=color, linestyle='dashed')
    # Restricted Zone, it is an arc with 4ft radius from center of the hoop
    restricted = Arc((0, 0), 80, 80, theta1=0, theta2=180, linewidth=lw,
                     color=color)
    # Three point line
    # Create the side 3pt lines, they are 14ft long before they begin to arc
    corner_three_a = Rectangle((-220, -47.5), 0, 140, linewidth=lw,
                               color=color)
    corner_three_b = Rectangle((220, -47.5), 0, 140, linewidth=lw, color=color)
    # 3pt arc - center of arc will be the hoop, arc is 23'9" away from hoop
    # I just played around with the theta values until they lined up with the
    # threes
    three_arc = Arc((0, 0), 475, 475, theta1=22, theta2=158, linewidth=lw,
                    color=color)
    # Center Court
    center_outer_arc = Arc((0, 422.5), 120, 120, theta1=180, theta2=0,
                           linewidth=lw, color=color)
    center_inner_arc = Arc((0, 422.5), 40, 40, theta1=180, theta2=0,
                           linewidth=lw, color=color)
    # List of the court elements to be plotted onto the axes
    court_elements = [hoop, backboard, outer_box, inner_box, top_free_throw,
                      bottom_free_throw, restricted, corner_three_a,
                      corner_three_b, three_arc, center_outer_arc,
                      center_inner_arc]
    if outer_lines:
        # Draw the half court line, baseline and side out bound lines
        outer_lines = Rectangle((-250, -47.5), 500, 470, linewidth=lw,
                                color=color, fill=False)
        court_elements.append(outer_lines)
    # Add the court elements onto the axes
    for element in court_elements:
        ax.add_patch(element)
    return ax


shot_chart_url = 'http://stats.nba.com/stats/shotchartdetail?CFID=33&CFPAR' \
                 'AMS=' + player_season + '&ContextFilter=&ContextMeasure=FGA&DateFrom=&D' \
                                          'ateTo=&GameID=&GameSegment=&LastNGames=0&LeagueID=00&Loca' \
                                          'tion=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&' \
                                          'PaceAdjust=N&PerMode=PerGame&Period=0&PlayerID=' + player_id + '&Plu' \
                                                                                                          'sMinus=N&PlayerPosition=&Rank=N&RookieYear=&Season=' + player_season + '&Seas' \
                                                                                                                                                                                  'onSegment=&SeasonType=Regular+Season&TeamID=0&VsConferenc' \
                                                                                                                                                                                  'e=&VsDivision=&mode=Advanced&showDetails=0&showShots=1&sh' \
                                                                                                                                                                                  'owZones=0'

# Get the webpage containing the data
response = requests.get(shot_chart_url, headers=header_data)
# Grab the headers to be used as column headers for our DataFrame
headers = response.json()['resultSets'][0]['headers']
# Grab the shot chart data
shots = response.json()['resultSets'][0]['rowSet']

shot_df = pd.DataFrame(shots, columns=headers)

pic = urllib.request.urlretrieve(
    "http://ak-static.cms.nba.com/wp-content/uploads/headshots/nba/latest/260x190/" + player_id + ".png",
    player_id + ".png")

made = 'Made Shot'
missed = 'Missed Shot'

made_shots = shot_df.loc[shot_df['EVENT_TYPE'] == made]
missed_shots = shot_df.loc[shot_df['EVENT_TYPE'] == missed]

sns.set_style("white")
sns.set_color_codes()

shots_x = np.concatenate([made_shots.LOC_X, missed_shots.LOC_X])
shots_y = np.concatenate([made_shots.LOC_Y, missed_shots.LOC_Y])
shots_z = np.concatenate([np.repeat(1, len(made_shots)), np.repeat(0, len(missed_shots))])

print(shots_z)

fig = plt.figure()  # create a figure object
ax = fig.add_subplot(1, 1, 1)  # create an axes object in the figure
hb = plt.hexbin(shots_x, shots_y, C=shots_z, cmap='coolwarm', gridsize=10)
plt.colorbar(hb, label='Shooting Percentage')

draw_court(ax)

ax.set_xlim(-250, 250)
ax.set_ylim(422.5, -47.5)

ax.set_xlabel('')
ax.set_ylabel('')
ax.tick_params(labelbottom='off', labelleft='off')
ax.set_facecolor('wheat')

# Add a title
ax.set_title(player_name.title() + ' Hot Zones \n' + player_season + ' Regular Season',
             y=1.0, fontsize=18, loc='left')

ax.get_xaxis().set_visible(False)
ax.get_yaxis().set_visible(False)

plt.gca().set_aspect('equal')

plt.tight_layout()
plt.subplots_adjust(top=0.75)

player_pic = plt.imread(pic[0])
img = OffsetImage(player_pic, zoom=0.53)

img.set_offset((1800, 1375))
# add the image
ax.add_artist(img)

figManager = plt.get_current_fig_manager()
figManager.full_screen_toggle()
plt.rcParams["keymap.quit"] = 'ctrl+w'

plt.show()
