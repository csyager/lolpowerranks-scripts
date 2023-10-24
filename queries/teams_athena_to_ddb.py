import boto3
import json
import time

client = boto3.client('athena')

query_start = client.start_query_execution(
    QueryString = """
        SELECT * FROM teams;
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
teams = results.get("ResultSet").get("Rows")[1:]
next_token = results.get("NextToken")
i = 1
while next_token:
    print(f"loading batch {i}")
    i += 1
    results = client.get_query_results(QueryExecutionId=query_start['QueryExecutionId'], NextToken=next_token)
    teams += results.get("ResultSet").get("Rows")[1:]
    next_token = results.get("NextToken")

# print(f"all batches processed, writing to file")
# with open('teams.json', 'w') as f:
#     f.write(json.dumps(teams))


### write to ddb

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('teams')

print("writing to ddb")

with table.batch_writer() as writer:
    for team_data in teams:
        team_id = team_data.get("Data")[0].get("VarCharValue")
        team_name = team_data.get("Data")[1].get("VarCharValue")
        team_acronym = team_data.get("Data")[2].get("VarCharValue")
        team_slug = team_data.get("Data")[3].get("VarCharValue")
        writer.put_item(Item={
            'team_id': team_id,
            'team_name': team_name,
            'team_acronym': team_acronym,
            'team_slug': team_slug
        })

print("Done.")