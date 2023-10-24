import json

f = open('elo_ratings.json')
teams = json.load(f)

sorted_teams = [key for key in sorted(teams, key=teams.get, reverse=True)]

rankings = {key: value for value, key in enumerate(sorted_teams, start=1)}

with open('rankings.json', 'w') as f:
    f.write(json.dumps(rankings))