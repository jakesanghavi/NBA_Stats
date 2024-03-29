from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
import pyautogui
import pandas as pd
import requests
import numpy as np
import os
import csv
import time
from wakepy import keepawake

# Specify the year you are interested in and the days of the year
year = 2023
game_dates = []
months = ['0' +  str(x) if len(str(x)) == 1 else str(x) for x in range(10,12)]
days = ['0' +  str(x) if len(str(x)) == 1 else str(x) for x in range(1,32)]

# Add a date range
for m in months:
    for d in days:
        link = str(year) + m + d
        if int(link) <= 20231113 and int(link) >= 20231111:
            game_dates.append(link)
            
ist_dates = ['20231103', '20231110', '20231114', '20231117', '20231121', '20231124', '20231128', '20231204', '20231205'
      '20231207', '20231209']

driver = webdriver.Chrome(ChromeDriverManager(driver_version='119.0.6045.123').install())
hrefs = {}

for game_date in game_dates:
    hrefs[game_date] = []
    link = "https://www.espn.com/nba/scoreboard/_/date/" + game_date
    print(link)
    
    driver.get(link)
    wait = WebDriverWait(driver, 10)  # Maximum wait time of 10 seconds
    time.sleep(2)
    
    pyautogui.hotkey('command', 'option', 'i')
    
    xpath = '//div[contains(@class, "ScoreCell--md")]'
    
    try:
        score_cells = wait.until(EC.presence_of_all_elements_located((By.XPATH, xpath)))
    except TimeoutException:
            no_games = driver.find_elements_by_class_name('clr-gray-05')
            print(no_games)
            if len(no_games) != 0 :
                del hrefs[game_date]
                pyautogui.hotkey('command', 'option', 'i')
                continue

                
    try:
        # Iterate through each instance and get the first link
        for score_cell in score_cells:
            # Find the first link within the current instance
            link = score_cell.find_element_by_tag_name("a")
            # Do something with the link, for example, print its href attribute
            hrefs[game_date].append(link.get_attribute("href"))
        pyautogui.hotkey('command', 'option', 'i')
        
    except StaleElementReferenceException:
        no_games = driver.find_elements_by_class_name('clr-gray-05')
        print(no_games)
        if len(no_games) != 0 :
            del hrefs[game_date]
            pyautogui.hotkey('command', 'option', 'i')
            continue
    
print(hrefs)    
driver.quit()

data = pd.read_csv("/Users/jakesanghavi/PycharmProjects/NBA/Data/2023_espn_wp.csv")
data_reg_id = data.loc[data['GAME_ID'] >= 22300061]['GAME_ID'].max() + 1
data_ist_id = data.loc[data['GAME_ID'] < 22300061]['GAME_ID'].max() + 1

driver = webdriver.Chrome(ChromeDriverManager(driver_version='119.0.6045.123').install())

game_id_norm = '00' + str(data_reg_id)
game_id_ist = '00' + str(data_ist_id)

df = pd.DataFrame(columns=['GAME_ID', 'GAME_DATE', 'time', 'wp_h'])

with keepawake(keep_screen_awake=False):
    for game_date in list(hrefs.keys()):
        for href in hrefs[game_date]:
            print(href)
            driver.get(href)

            wait = WebDriverWait(driver, 10)  # Maximum wait time of 10 seconds

            win_probability_button = wait.until(
                EC.element_to_be_clickable((By.XPATH, '//button[contains(text(), "Win Probability")]'))
            )
            win_probability_button.click()

            g_element = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'g[class="recharts-layer recharts-line"]'))
            )

            try:
                path_element = g_element.find_element(By.TAG_NAME, 'path')
                d_attribute = path_element.get_attribute('d')
    #         d_attribute = driver.execute_script("return arguments[0].getAttribute('d');", path_element)
            except StaleElementReferenceException:
                path_element = g_element.find_element(By.TAG_NAME, 'path')
                d_attribute = path_element.get_attribute('d')
                
            span_elements = driver.find_elements_by_xpath("//a[@data-testid='prism-linkbase']//span")
            
            away_tm = span_elements[0].text
            home_tm = span_elements[1].text
            
            path_segments = d_attribute[1:].replace('C', ',')  # Skip the first element as it is before the first 'C'

            data_array = np.fromstring(path_segments, sep=',')

            # Reshape the array to have two columns
            reshaped_array = data_array.reshape(-1, 2)

            iteration_df = pd.DataFrame(data=reshaped_array, columns=['time', 'wp_h'])
            iteration_df['GAME_DATE'] = game_date
            iteration_df['away_tm'] = away_tm
            iteration_df['home_tm'] = home_tm

            if game_date in ist_dates:
                iteration_df['GAME_ID'] = game_id_ist
                game_id_ist = "00" + str(int(game_id_ist) + 1)
            else:
                iteration_df['GAME_ID'] = game_id_norm
                game_id_norm = "00" + str(int(game_id_norm) + 1)

            # Append the current iteration's DataFrame to the main DataFrame
            df = pd.concat([df, iteration_df], ignore_index=True, axis=0)

driver.quit()

df_new = df.copy()
tm_changes = {'GS': 'GSW', 'NO': 'NOP', 'NY': 'NYK', 'SA': 'SAS', 'UTAH': 'UTA'}
df_new['away_tm'] = df_new['away_tm'].map(tm_changes).fillna(df_new['away_tm'])
df_new['home_tm'] = df_new['home_tm'].map(tm_changes).fillna(df_new['home_tm'])

data = pd.concat([data, df_new])

data.to_csv("/Users/jakesanghavi/PycharmProjects/NBA/Data/2023_espn_wp.csv", index=False)
