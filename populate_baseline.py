import psycopg2

# connect to our database
conn = psycopg2.connect(
    database="nfl_historic", user='postgres', password='password', host='localhost', port= '5432'
)

conn.autocommit = True

#Creating a cursor object using the cursor() method
cursor = conn.cursor()

# pull in all the scores
cursor.execute('SELECT home_score_final, away_score_final FROM games')

total_games = 0

odds = [ [0]*10 for i in range(10)]

for row in cursor:
    total_games += 1
    # print("%d - %d" %(row[0], row[1]))
    # print("%d - %d" %(row[0] % 10, row[1] % 10))
    odds[row[0] % 10][row[1] % 10] += 1

for home_idx, home in enumerate(odds):
    for away_idx, cnt in enumerate(home):
        cursor.execute('INSERT INTO baseline (home_score, away_score, final_float) VALUES (%s, %s, %s)',
                        (home_idx, away_idx, cnt / total_games))

# done with the database, close it
conn.close()

print ("DONE", total_games)