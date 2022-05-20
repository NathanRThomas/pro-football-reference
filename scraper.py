# scaper for pulling nfl data into a postgresql database

import requests 
from bs4 import BeautifulSoup
import psycopg2
import pandas as pd
from dateutil import parser
import time 
# import numpy as np

headers = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Max-Age': '3600',
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0'
}

def parseTeamStats (team_info):
    ret = {}
    for idx, val in enumerate(team_info):
        if idx == 0:
            ret['first_downs'] = int(val)
        if idx == 3:
            ret['sacks'] = int(val.split('-')[0])
        if idx == 5:
            ret['total_yards'] = int(val)
        if idx == 7:
            ret['turnovers'] = int(val)
        if idx == 8:
            ret['penalties'] = int(val.split('-')[0])
            ret['penalty_yards'] = int(val.split('-')[1])
        if idx == 11:
            ret['time_of_possession'] = int(val.split(':')[0]) * 60 + int(val.split(':')[1])

    return ret 

def gameData (url, season, week, cursor):
    req = requests.get("https://www.pro-football-reference.com" + url, headers) # get this once

    # top level meta about the game
    soup = BeautifulSoup(req.content, 'html.parser')
    boxes = soup.select('div.scorebox > div.scorebox_meta')[0].find_all('div')
    
    game_date = parser.parse(boxes[0].contents[0], fuzzy=True)
    game_date = "%d-%d-%d" % (game_date.year, game_date.month, game_date.day)

    stadium = boxes[2].contents[2].contents[0]

    game_time = parser.parse(boxes[1].contents[1], fuzzy=True)
    game_time = "%d:%d:00" % (game_time.hour, game_time.minute)

    dur = None # this is sometimes missing

    if boxes[3].contents[0].contents[0] == 'Time of Game':
        dur = boxes[3].contents[1].split(':')
    elif boxes[4].contents[0].contents[0] == 'Time of Game':
        dur = boxes[4].contents[1].split(':')

    game_duration = None # could be missing
    if dur:
        game_duration = int(dur[1]) * 60 + int(dur[2]) # game time in minutes

    game_data = pd.read_html(req.content)

    # home team top box
    home_team = game_data[0].values[1][1]
    home_score_q1 = game_data[0].values[1][2]
    home_score_q2 = game_data[0].values[1][3] + home_score_q1
    home_score_q3 = game_data[0].values[1][4] + home_score_q2
    home_score_q4 = game_data[0].values[1][5] + home_score_q3 
    # see if we went into overtime
    home_score_ot = None 
    if len(game_data[0].values[1]) == 8:
        home_score_ot = game_data[0].values[1][6]
        home_score = game_data[0].values[1][7]
    else:
        home_score = game_data[0].values[1][6]

    # away team top box
    away_team = game_data[0].values[0][1]
    away_score_q1 = game_data[0].values[0][2]
    away_score_q2 = game_data[0].values[0][3] + away_score_q1
    away_score_q3 = game_data[0].values[0][4] + away_score_q2
    away_score_q4 = game_data[0].values[0][5] + away_score_q3 

    away_score_ot = None 
    if len(game_data[0].values[1]) == 8:
        away_score_ot = game_data[0].values[0][6]
        away_score = game_data[0].values[0][7]
    else:
        away_score = game_data[0].values[0][6]

    away_score = game_data[0].values[0][6]

    # not getting the specific tables I want
    # game info
    start = str(req.content).index('id="game_info"')
    end = str(req.content)[start:].index('</table>') + 9

    game_info = pd.read_html(str(req.content)[start-50:start+end])[0]
    weather = '' # we don't always get this one
    first_possession = ''
    vegas_line = None

    for idx, key in enumerate(game_info[0]):
        if key == "Won Toss":
            # we want the team that started with the ball
            # see if they defered
            deferred = 'eferred' in game_info[1][idx]

            # see if the home team exists
            homeFlag = False 
            for word in home_team:
                if word in game_info[1][idx]:
                    homeFlag = True 
            
            if homeFlag and deferred:
                first_possession = away_team
            elif homeFlag or deferred:
                first_possession = home_team
            else:
                first_possession = away_team # away flag and not deferred

        if key == "Roof":
            roof = game_info[1][idx]
        if key == "Surface":
            surface = game_info[1][idx]
        if key == "Weather":
            weather = game_info[1][idx]
        if key == "Vegas Line":

            for part in game_info[1][idx].split():
                try:
                    vegas_line = abs(float(part))
                except ValueError:
                    pass 

            # we got our line, figure out out favorite team
            if home_team in game_info[1][idx]:
                vegas_line = vegas_line * -1 # home is favorited
            # else we leave whatever happened as it happened
            
                

        if key == "Over/Under":
            over_under = 0.0 
            
            for part in game_info[1][idx].split():
                try:
                    over_under = float(part)
                except ValueError:
                    pass 



    # team stats
    start = str(req.content).index('id="team_stats"')
    end = str(req.content)[start:].index('</table>') + 9

    team_stats = pd.read_html(str(req.content)[start-40:start+end])[0]

    # do the away team actual stats 
    away_actual = parseTeamStats(team_stats[team_stats.columns[1]])
    home_actual = parseTeamStats(team_stats[team_stats.columns[2]])

    reg_season = 0
    if week.isdigit():
        reg_season = 1 # they're in the regular season

    cursor.execute('''INSERT INTO games (home_team, away_team, season, week, reg_season, game_date, start_time, game_duration, stadium, 
                    home_score_final, home_score_q1, home_score_q2, home_score_q3, home_score_q4, home_score_ot,
                    away_score_final, away_score_q1, away_score_q2, away_score_q3, away_score_q4, away_score_ot,
                    first_possession, roof, surface, weather, vegas_line, over_under, 
                    home_first_downs, home_total_yards, home_turnovers, home_penalties, home_penalty_yards, home_sacks, home_time_of_possession,
                    away_first_downs, away_total_yards, away_turnovers, away_penalties, away_penalty_yards, away_sacks, away_time_of_possession,
                    last_updated) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    now())''', (home_team, away_team, season, week, reg_season, game_date, game_time, game_duration, stadium,
                    home_score, home_score_q1, home_score_q2, home_score_q3, home_score_q4, home_score_ot,
                    away_score, away_score_q1, away_score_q2, away_score_q3, away_score_q4, away_score_ot,
                    first_possession, roof, surface, weather, vegas_line, over_under,
                    home_actual['first_downs'], home_actual['total_yards'], home_actual['turnovers'], home_actual['penalties'], home_actual['penalty_yards'], home_actual['sacks'], home_actual['time_of_possession'],
                    away_actual['first_downs'], away_actual['total_yards'], away_actual['turnovers'], away_actual['penalties'], away_actual['penalty_yards'], away_actual['sacks'], away_actual['time_of_possession']))
    

# now start some code

# connect to our database
conn = psycopg2.connect(
    database="nfl_historic", user='postgres', password='password', host='localhost', port= '5432'
)

conn.autocommit = True

#Creating a cursor object using the cursor() method
cursor = conn.cursor()

season = 2020 # doing 1 season at a time, you can obviously loop this, but remember 2021 has 1 extra week of games

for week in range(1, 22):

    # do the week
    url = "https://www.pro-football-reference.com/years/%d/week_%d.htm" % (season, week)

    req = requests.get(url, headers)
    soup = BeautifulSoup(req.content, 'html.parser')
    
    print("\nPulling links %d week%d" % (season, week))

    for game in soup.select('div.game_summary'):
        # print(game)
        link = game.select('td.gamelink a')
        # print(link)
        # print(link[0]['href'])

        #note, these need to be adjusted for 2021 with a longer season, week 19 is wild card week
        wk = str(week)
        if week == 18:
            wk = 'Wild Card'
        if week == 19:
            wk = 'Divisional'
        if week == 20:
            wk = 'Conf Champ'
        if week == 21:
            wk = 'Super Bowl'

        # now we got a link, let's get that game's data and insert it
        gameData(link[0]['href'], season, wk, cursor)
        print(".", end="")
        time.sleep(1) # trying to be nice and not hammer pro-football-reference
    

# done with the database, close it
conn.close()

print("DONE %d!!!" % (season))
