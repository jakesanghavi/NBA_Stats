import pandas as pd
from unidecode import unidecode

'''Credit to Ryan Davis for some of this code'''


# Basketball reference contains multiple rows for players who have played on multiple teams.
# we only care about the season total for the player so we must deduplicate the rows (selected Team = TOT)
def deduplicate_traded_players(group):
    if len(group) > 1:
        return group[group["Tm"] == "TOT"]
    return group


def remove_accents(a):
    return unidecode(a)


season = "2022"

# Read our basketball_reference data
bbref_data = pd.read_csv("basketball_reference_totals_{}.csv".format(season))
bbref_data['Player'] = bbref_data['Player'].str.replace(".", "", regex=False)
bbref_data["Player"] = bbref_data["Player"].apply(remove_accents)


# read out stats.nba.com data
nba_data = pd.read_csv("stats_nba_player_data_2021-22.csv")
# convert the player id from an int to a string
nba_data["PLAYER_ID"] = nba_data["PLAYER_ID"].astype(str)
nba_data["PLAYER_NAME"] = nba_data["PLAYER_NAME"].str.replace(".", "", regex=False)
nba_data["PLAYER_NAME"] = nba_data["PLAYER_NAME"].apply(remove_accents)

# take the player name, id, team and fields we will use for deduplication from bbref data
bbref_base_data = bbref_data[["Player", "id", "Pos", "Tm", "FGA", "Total Rebounds", "Assists"]].groupby(
    by="id").apply(deduplicate_traded_players)

# take the player name, id, and fields we will use for deduplication from stats.nba.com data
nba_base_data = nba_data[["PLAYER_ID", "PLAYER_NAME", "FGA", "REB", "AST"]]

# Perform a full outer join on the two dataframes. This allows us to get all of the exact matches
name_matches = bbref_base_data.merge(nba_base_data,
                                     left_on=["Player", "FGA", "Total Rebounds", "Assists"],
                                     right_on=["PLAYER_NAME", "FGA", "REB", "AST"], how="outer")

# take all the exact matches and rename the columns, we only care about player name and id from each source
name_matches_ids = name_matches.dropna()
name_matches_ids = name_matches_ids[["Player", "id", "PLAYER_NAME", "PLAYER_ID", "Pos"]]
name_matches_ids.columns = ["bbref_name", "bbref_id", "nba_name", "nba_id", 'position']

# Take all the rows from the full outer join that have null values. These are the cases where no match was found.
non_matches = name_matches[name_matches.isnull().any(axis=1)]

# take all the bbref data from non_matches
bbref_non_matches = non_matches[["Player", "id", "FGA", "Total Rebounds", "Assists"]].dropna()

# take all the stats.nba data from the non_matches
nba_non_matches = non_matches[["PLAYER_NAME", "PLAYER_ID", "FGA", "REB", "AST"]].dropna()

name_matches_ids.to_csv('player_id_matches_2021-2022.csv', index=None)
