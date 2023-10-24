import boto3
import time
import json

client = boto3.client('athena')

query_start = client.start_query_execution(
    QueryString = """
        WITH dataset AS(
            SELECT id, leagueid, name, slug, sport, startdate, enddate, i.name AS stage_name, i.slug AS stage_slug, i.sections AS stage_sections FROM tournaments
            CROSS JOIN UNNEST(stages) as t(i)
        )
        SELECT 
            id AS tournament_id, 
            leagueid as tournament_league_id,
            name as tournament_name,
            slug as tournament_slug,
            sport as tournament_sport,
            startdate as tournament_start_date,
            enddate as tournament_end_date,
            stage_name as stage_name, 
            stage_slug as stage_slug, 
            section.name AS section_name, 
            match.id AS match_id, 
            game.id AS game_id, 
            game.teams[1].id as team_1_id, 
            game.teams[2].id as team_2_id,
            game.teams[1].side as team_1_side,
            game.teams[2].side as team_2_side,
            game.teams[1].result.outcome as team_1_outcome,
            game.teams[2].result.outcome as team_2_outcome
        from dataset, UNNEST(stage_sections) as t(section), UNNEST(section.matches) as t(match), UNNEST (match.games) as t(game)
    """,
    QueryExecutionContext = {
        'Database': 'lol'
    }
)

backoff_factor = 1
execution_status = client.get_query_execution(QueryExecutionId = query_start['QueryExecutionId'])['QueryExecution']['Status']['State']
while execution_status not in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
    # sleep the thread in an exponential backoff, starting at 1 second and doubling each time
    print(f"Waiting for query to finish, backoff factor {backoff_factor}")
    execution_status = client.get_query_execution(QueryExecutionId = query_start['QueryExecutionId'])['QueryExecution']['Status']['State']
    time.sleep(2 ** backoff_factor)
    backoff_factor += 1
    
print(f"execution status: {execution_status}")
results = client.get_query_results(QueryExecutionId=query_start['QueryExecutionId'])
games = results.get("ResultSet").get("Rows")[1:]
next_token = results.get("NextToken")
i = 1
while next_token:
    print(f"loading batch {i}")
    i += 1
    results = client.get_query_results(QueryExecutionId=query_start['QueryExecutionId'], NextToken=next_token)
    games += results.get("ResultSet").get("Rows")[1:]
    next_token = results.get("NextToken")

print(f"all batches processed, writing to file")
with open('games.json', 'w') as f:
    f.write(json.dumps(games))