import data_utils
from wakepy import keep

year = 2025

with keep.presenting():
    data_utils.get_all_data(year, espn=True)