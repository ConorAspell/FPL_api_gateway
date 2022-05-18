import json
import pandas as pd
import requests
import boto3 
import io

def lambda_handler(event, context):
  
    response_object = {}
    response_object['statusCode'] = 400
    response_object['headers'] = {}
    response_object['headers']['Content-Type'] = 'application/json'
    response_object['body'] = json.dumps("Please include a valid team id in your request")

    team_id = str(event["queryStringParameters"]['team_id'])

    try: 
        team_id = str(event["queryStringParameters"]['team_id'])
        players_df = get_df('fpl-bucket-2022', 'players.csv')
    
        gameweek= players_df.gameweek.iat[0]
        team = get('https://fantasy.premierleague.com/api/entry/'+str(team_id)+'/event/'+str(gameweek-1) +'/picks/')
        players = [x['element'] for x in team['picks']]
    
        bank = team['entry_history']['bank']
        my_team = players_df.loc[players_df.id.isin(players)]
        potential_players = players_df.loc[~players_df.id.isin(players)]
    
        player_out = calc_out_weight(my_team)
    
        position = player_out.element_type.iat[0]
        out_cost = player_out.now_cost.iat[0]
        budget = bank + out_cost
        dups_team = my_team.pivot_table(index=['team'], aggfunc='size')
        invalid_teams = dups_team.loc[dups_team==3].index.tolist()
    
        potential_players=potential_players.loc[~potential_players.team.isin(invalid_teams)]
        potential_players=potential_players.loc[potential_players.element_type==position]
        potential_players = potential_players.loc[potential_players.now_cost<=budget]
    
        player_in = calc_in_weights(potential_players)
        response_object['statusCode'] = 200
        response_object['body'] = json.dumps(player_out.web_name.iat[0] + "->" + player_in.web_name.iat[0])
        
        return response_object
    except Exception as e:
        response_object['body'] = json.dumps("Please enter a valid address")
        return response_object


def calc_out_weight(players):
    return players.sort_values(by='out_weight', ascending=False).iloc[:1]

def calc_in_weights(players):
    return players.sort_values(by='in_weight', ascending=False).iloc[:1]

def get_df(bucket, key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    return df

def get(url):
    response = requests.get(url)
    return json.loads(response.content)