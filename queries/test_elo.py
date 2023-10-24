import json
import math
from collections import defaultdict

f = open('output.json')
games = json.load(f)



def calculate_elo(scalar: int):
    team_ratings = defaultdict(lambda: 1000)
    upset_count = 0
    num_games = len(games)
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

    return upset_count


def test_elo(team_ratings):
    upset_count = 0
    for game in games:
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
            else:
                if prob_team_2_wins < prob_team_1_wins:
                    upset_count += 1
        except Exception as e:
            print(f"Exception while getting results for game {game}: {e}")
    
    return upset_count

if __name__ == '__main__':
    results = defaultdict(int)
    min_upsets = math.inf
    max_upsets = 0
    for i in range(1, 500):
        num_upsets = calculate_elo(i)
        min_upsets = min(min_upsets, num_upsets)
        max_upsets = max(max_upsets, num_upsets)
        results[i] = num_upsets

    print(min_upsets)
    print(max_upsets)
    print(json.dumps(results))