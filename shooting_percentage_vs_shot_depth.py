import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import string

# Team color hexcodes
COLORS = {'ATL': '#E03A3E', 'BKN': '#000000', 'BOS': '#007A33', 'CHA': '#00788C', 'CHI': '#CE1141', 'CLE': '#860038',
          'DAL': '#00538C', 'DEN': '#FEC524', 'DET': '#C8102E', 'GSW': '#FFC72C', 'HOU': '#CE1141', 'IND': '#FDBB30',
          'LAC': '#C8102E', 'LAL': '#552583', 'MEM': '#5D76A9', 'MIA': '#98002E', 'MIL': '#00471B', 'MIN': '#0C2340',
          'NOP': '#0C2340', 'NYK': '#F58426', 'OKC': '#007AC1', 'ORL': '#0077C0', 'PHI': '#006BB6', 'PHX': '#E56020',
          'POR': '#E03A3E', 'SAC': '#5A2D81', 'SAS': '#C4CED4', 'TOR': '#CE1141', 'UTA': '#002B5C', 'WAS': '#E31837',
          }

# Set a font style
plt.rcParams["font.family"] = "monospace"


def extract_depth(row):
    """
    Help with vectorizing code to extract shot depth from a shot attempt
    Parameters
    ----------
    row : pandas.Series
        A row of the dataframe

    Returns
    -------
    The shot depth for a given row
    """
    d = row['DESCRIPTION'].split()
    # Find the items in the description that are distance measurements
    matches = [x for x in d if x in str_nums]
    # If they match, strip the trailing '
    if matches:
        return int(matches[0].translate(trans_table))
    return None


def update_row(row):
    """
    Helps vectorize code to populate our depth dataframe
    Parameters
    ----------
    row : pandas.Series
        A row of the dataframe

    Returns
    -------
    The relevant values for a player
    """
    player_id = row['Player_ids']
    name_row = name_set[name_set['PLAYER_ID'] == player_id]
    player_name = name_row['PLAYER_NAME'].iloc[0]
    player_team = name_row['TEAM_ABBREVIATION'].iloc[0]
    games = name_row['GP'].iloc[0]
    efg_pct = (name_row['FGM'].iloc[0] + 0.5*name_row['FG3M'].iloc[0])/name_row['FGA'].iloc[0]
    return np.array([player_name, player_team, games, efg_pct])


def annotate_row(row):
    """
    Helps vectorize code that annotates the final plot
    Parameters
    ----------
    row : pandas.Series
        A row of the dataframe

    Returns
    -------

    """
    return row['DEPTH'], row['EFG_percentage'], row['Player_names']


def scatter_row(row):
    """
    Helps vectorize code for plotting the data
    Parameters
    ----------
    row : pandas.Series
        A row of the dataframe

    Returns
    -------
    The relevant metrics and colors
    """
    return row['DEPTH'], row['EFG_percentage'], (row['Shot Count'] / 5), COLORS[row['Player_teams']]


# Read in the necessary CSV files
data = pd.read_csv("2022_reg_pbp.csv", low_memory=False)
name_set = pd.read_csv("stats_nba_player_data_2022-23.csv")
bb_ref_stats = pd.read_csv("basketball_reference_totals_2023.csv")

# Get all possible shot depths and create a trans table
str_nums = set([str(x) + "'" for x in range(97)])
trans_table = str.maketrans("", "", string.punctuation)

# Helps with missing data errors
data = data.fillna('')

data['DESCRIPTION'] = data['HOMEDESCRIPTION'] + ' ' + data['NEUTRALDESCRIPTION'] + ' ' + data['VISITORDESCRIPTION']
# Create a depth column that is filled with zeroes
data.insert(15, 'DEPTH', 0)

# Slim down our data to only include shot attempts
data = data.loc[(data['EVENTMSGTYPE'] == 1) | (data['EVENTMSGTYPE'] == 2)]

# Get the depth for each shot
data['DEPTH'] = data.apply(extract_depth, axis=1)

# Grab this value to use in our plot
depth_mean = data['DEPTH'].mean()

# Create a new dataframe to store the values we will need for our plot
depth_index = data.groupby(by=['PLAYER1_ID']).agg({"DEPTH": "mean"})

depth_index['Player_ids'] = depth_index.index.values
depth_index['Player_names'] = np.nan
depth_index['Player_teams'] = np.nan
depth_index['Games'] = np.nan
depth_index['EFG_percentage'] = np.nan

# Fill in the dataframe
updated_data = np.vstack(depth_index.apply(update_row, axis=1))
depth_index[['Player_names', 'Player_teams', 'Games', 'EFG_percentage']] = updated_data

# Get the shot count for each player, and filter out those with few shots taken
depth_index['Shot Count'] = data.loc[(data['EVENTMSGTYPE'] == 1) | (data['EVENTMSGTYPE'] == 2)] \
    .groupby(by='PLAYER1_ID')[['DEPTH']].count()

mini = 300
depth_index = depth_index.loc[depth_index['Shot Count'] >= mini]
depth_index['EFG_percentage'] = depth_index['EFG_percentage'].astype('float')

# Grab this value to put on the final plot
fg_mean = depth_index['EFG_percentage'].mean()

x = depth_index['DEPTH']
y = depth_index['EFG_percentage']

fig, ax = plt.subplots(figsize=(10, 10))

# Scatter the data points
scatter_data = depth_index.apply(scatter_row, axis=1)
for x, y, fga, color in scatter_data:
    ax.scatter(x, y, s=fga, alpha=0.7, color=color)

# Label the plot
ax.set_xlabel('Average Shot Depth (Feet)', fontsize=14)
ax.set_ylabel('Effective Field Goal Percentage', fontsize=14)
ax.set_title(f'eFG% vs. Average Shot Depth -- 2022-2023 (Min. {mini} Shots)', fontsize=18)

# Annotate the points
annotations = depth_index.apply(annotate_row, axis=1)
for x, y, label in annotations:
    plt.annotate(label, (x, y), fontsize=6)

# Place the average lines on the plot
plt.axvline(x=depth_mean, ls='dashed', c='r')
plt.axhline(y=fg_mean, ls='dashed', c='r')

# Place relevant text on the plot
plt.figtext(0.15, 0.15, 'Short Shots, Bad Shooter', horizontalalignment='left', weight='bold')
plt.figtext(0.9, 0.058, 'by Jake Sanghavi', horizontalalignment='right')
plt.figtext(0.85, 0.85, 'Long Shots, Good Shooter', horizontalalignment='right', weight='bold')

# Some personal customization
ax.set_facecolor('peachpuff')
fig.patch.set_facecolor('wheat')

plt.show()
