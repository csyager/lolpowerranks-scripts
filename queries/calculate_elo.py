import json
from collections import defaultdict

f = open('output.json')
games = json.load(f)

scalar = 74

team_ratings = defaultdict(lambda: 1000)
num_games = len(games)
print(f"Number of games: {num_games}")
upset_count = 0
for i in range(num_games):
    game = games[i]
    teams = json.loads(game.get("Data")[-1].get("VarCharValue"))
    if teams[0].get("id") == "0" or teams[1].get("id") == "0":
            continue
    team_1_elo = team_ratings[teams[0].get("id")]
    team_2_elo = team_ratings[teams[1].get("id")]

    prob_team_1_wins = (1.0 / (1.0 + 10 ** ((team_2_elo - team_1_elo) / 400.0)))
    prob_team_2_wins = (1.0 / (1.0 + 10 ** ((team_1_elo - team_2_elo) / 400.0)))

    try:
        if teams[0].get("result").get("outcome") == "win":
            if prob_team_1_wins < prob_team_2_wins:
                upset_count += 1
            team_ratings[teams[0].get("id")] += scalar * (1 - prob_team_1_wins)
            team_ratings[teams[1].get("id")] += scalar * (0 - prob_team_2_wins)
        else:
            if prob_team_2_wins < prob_team_1_wins:
                upset_count += 1
            team_ratings[teams[0].get("id")] += scalar * (0 - prob_team_1_wins)
            team_ratings[teams[1].get("id")] += scalar * (1 - prob_team_2_wins)
    except Exception as e:
        print(f"Exception while getting results for game {game}: {e}")
    
    if i % 100 == 0:
        print(f"Calculating elo ratings... {i / num_games * 100}%")

print(f"Number of upset games: {upset_count}")
print(f"Percent upset games: {upset_count / num_games * 100}%")

with open('elo_ratings.json', 'w') as f:
    f.write(json.dumps(team_ratings))