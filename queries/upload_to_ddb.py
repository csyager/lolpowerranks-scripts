import json
from decimal import Decimal
from collections import defaultdict
import boto3

dynamodb = boto3.resource('dynamodb')


def upload_team_elos():
    table = dynamodb.Table('team_elos')

    with open('elo_ratings.json', 'r') as f:
        ratings = json.load(f, parse_float=Decimal)

    with table.batch_writer() as writer:
        for team_id, elo in ratings.items():
            writer.put_item(Item={
                'team_id': team_id,
                'elo': elo
            })


def upload_team_rankings():
    table = dynamodb.Table('team_id_to_ranking')
    with open('rankings.json', 'r') as f:
        rankings = json.load(f)

    with table.batch_writer() as writer:
        for team_id, ranking in rankings.items():
            writer.put_item(Item={
                'team_id': team_id,
                'rank': ranking
            })


def upload_team_rankings_with_team_data():
    table = dynamodb.Table('rankings')
    team_data_dict = {}
    with open("teams.json", 'r') as f:
        team_data_list = json.load(f)
    for data_entry in team_data_list:
        team_id = data_entry.get("Data")[0].get("VarCharValue")
        team_name = data_entry.get("Data")[1].get("VarCharValue")
        team_acronym = data_entry.get("Data")[2].get("VarCharValue")
        team_slug = data_entry.get("Data")[3].get("VarCharValue")
        team_data_dict[team_id] = {
            "team_name": team_name,
            "team_acronym": team_acronym,
            "team_slug": team_slug
        }
    with open("rankings.json", 'r') as f:
        rankings = json.load(f)
    with table.batch_writer() as writer:
        for team_id, ranking in rankings.items():
            writer.put_item(Item={
                'team_id': team_id,
                'ranking': ranking,
                'team_name': team_data_dict.get(team_id, {}).get("team_name"),
                'team_acronym': team_data_dict.get(team_id, {}).get("team_acronym"),
                'team_slug': team_data_dict.get(team_id, {}).get("team_slug")
            })


def upload_tournament_teams_to_ddb():
    tournament_teams = defaultdict(lambda: defaultdict(set))
    with open("games.json") as f:
        games = json.load(f)
    for data_entry in games:
        data = data_entry.get("Data")
        tournament_id = data[0].get("VarCharValue")
        stage_slug = data[8].get("VarCharValue")
        team_1 = data[12].get("VarCharValue")
        team_2 = data[13].get("VarCharValue")
        tournament_teams[tournament_id][stage_slug].add(team_1)
        tournament_teams[tournament_id][stage_slug].add(team_2)
    table = dynamodb.Table('tournament_teams')
    with table.batch_writer() as writer:
        for tournament in tournament_teams:
            for stage in tournament_teams[tournament]:
                for team in tournament_teams[tournament][stage]:
                    stage_team_key = stage + "-" + team
                    writer.put_item(Item={
                        'tournament_id': tournament,
                        'stage_team_key': stage_team_key,
                        'stage_slug': stage,
                        'team_id': team
                    })
    

def upload_tournaments_to_ddb():
    with open("../esports-data/tournaments.json") as f:
        tournaments = json.load(f)
    
    table = dynamodb.Table('tournaments')
    with table.batch_writer() as writer:
        for tournament in tournaments:
            writer.put_item(Item={
                'tournament_id': tournament.get("id"),
                'tournament_league_id': tournament.get("leagueId"),
                'tournament_name': tournament.get("name"),
                'tournament_slug': tournament.get("slug"),
                'tournament_start_date': tournament.get("startDate"),
                'tournament_end_date': tournament.get("endDate"),
                'tournament_sport': tournament.get("sport")
            })


def upload_tournament_stages_to_ddb():
    with open("../esports-data/tournaments.json") as f:
        tournaments = json.load(f)
    
    tournament_stages = defaultdict(list)
    for tournament in tournaments:
        for stage in tournament.get("stages"):
            tournament_stages[tournament.get("id")].append(stage)
    
    table = dynamodb.Table('tournament_stages')
    with table.batch_writer() as writer:
        for tournament in tournament_stages:
            for stage in tournament_stages[tournament]:
                writer.put_item(Item={
                    'tournament_id': tournament,
                    'stage_name': stage.get("name"),
                    'stage_type': stage.get("type"),
                    'stage_slug': stage.get("slug")
                })

if __name__ == '__main__':
    upload_team_elos()
    print("Done.")