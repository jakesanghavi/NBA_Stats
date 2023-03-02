library(rvest)
library(XML)
library(httr)
library(tidyverse)
library(RSelenium)

# Set Team Abbreviations
team_abbs <- c('ATL', 'BKN', 'BOS', 'CHA', 'CLE', 'CHI', 'DAL', 'DEN', 'DET', 'GSW', 'HOU',
              'IND', 'LAC', 'LAL', 'MEM', 'MIA', 'MIL', 'MIN', 'NO', 'NY', 'OKC', 'ORL', 'PHI',
              'PHX', 'POR', 'SA', 'SAC', 'TOR', 'UTAH', 'WSH')

season <- 2022

# Scrape Data for Seasons with ESPN RPM (2012 to 2020)
seasons <- c(season:season+1)
urls <- {}
n <- 0
for(i in 1:length(team_abbs)){
  for(j in 1:length(seasons)){
    n = n + 1
    urls[n] <- paste0('https://www.espn.com/nba/team/stats/_/name/', team_abbs[i], '/season/', seasons[j], '/seasontype/2')
  }
}

# Scrape Information from Player Links
espn_links <- {}
start <- 1
for(i in start:length(urls)){
  Sys.sleep(2)
  webpage <- read_html(as.character(urls[i]))
  links <- webpage %>%
    html_nodes(xpath = "//td/span/a") %>% 
    html_attr("href")
  links <- links[!duplicated(links)]
  names <- webpage %>%
    html_nodes(xpath = "//td/span/a") %>%
    html_text()
  names <- names[!duplicated(names)]
  links <- data.frame(link = links, name = names)
  espn_links[i] <- list(links)
  start <- start + 1
  str(start)
}

# Rbind All Links
library(plyr)
espn_links_all <- ldply(espn_links, data.frame)
detach('package:plyr')
names(espn_links_all) <- c('espn_link', 'espn_name')

# Function to get out only player IDs from the links
get_id <- function(x){
  return (strsplit(x,"/")[[1]][8])
}

espn_links_all <- as.data.frame(espn_links_all[!duplicated(espn_links_all$id),])
f_name <- paste0('nba_ESPNIDs_', season, '_', season+1, '.csv')
write.csv(espn_links_all, f_name, row.names = F)
