import requests
import pandas  as pd
import json
from datetime import datetime
import boto3
from io import StringIO # python3; python2: BytesIO 
import os
import io

s3_client = boto3.client('s3')
def lambda_handler(event, context):
    players_df, fixtures_df, gameweek = get_data()
    players_df = calc_out_weight(players_df)
    players_df = calc_in_weights(players_df)
    players_df['gameweek'] = gameweek
    players_df = players_df[necessary_columns]
    print(players_df.head())
    print(fixtures_df.head())
    print(gameweek)
    bucket= "fpl-bucket-2022"
    put_df(players_df,bucket, "players.csv")

    # TODO implement


necessary_columns = ['element_type', 'id', 'now_cost', 'team','web_name', 'in_weight', 'out_weight','gameweek']

columns = ['chance_of_playing_next_round', 'chance_of_playing_this_round',
 'element_type', 'ep_next',
       'ep_this',  'first_name', 'form', 'id', 'in_dreamteam',
        'now_cost', 'points_per_game',
       'second_name', 'selected_by_percent', 
        'team', 'team_code', 'total_points', 'transfers_in',
        'transfers_out',
       'value_form', 'value_season', 'web_name',      
        'influence', 'creativity', 'threat',
       'ict_index']

def get_team(ids, players):
    players =  get('https://fantasy.premierleague.com/api/bootstrap-static/')
    players_df = pd.DataFrame(players['elements'])
    teams_df = pd.DataFrame(players['teams'])
    fixtures_df = pd.DataFrame(players['events'])
    today = datetime.now().timestamp()
    fixtures_df = fixtures_df.loc[fixtures_df.deadline_time_epoch>today]
    gameweek =  fixtures_df.iloc[0].id
    players_df = players_df[columns]
    players_df.chance_of_playing_next_round = players_df.chance_of_playing_next_round.fillna(100.0)
    players_df.chance_of_playing_this_round = players_df.chance_of_playing_this_round.fillna(100.0)
    fixtures = get('https://fantasy.premierleague.com/api/fixtures/?event='+str(gameweek))
    fixtures_df = pd.DataFrame(fixtures)

    
    teams=dict(zip(teams_df.id, teams_df.name))
    players_df['team_name'] = players_df['team'].map(teams)
    fixtures_df['team_a_name'] = fixtures_df['team_a'].map(teams)
    fixtures_df['team_h_name'] = fixtures_df['team_h'].map(teams)

    home_strength=dict(zip(teams_df.id, teams_df.strength_overall_home))
    away_strength=dict(zip(teams_df.id, teams_df.strength_overall_away))

    fixtures_df['team_a_strength'] = fixtures_df['team_a'].map(away_strength)
    fixtures_df['team_h_strength'] = fixtures_df['team_h'].map(home_strength)

    fixtures_df=fixtures_df.drop(columns=['id'])
    a_players = pd.merge(players_df, fixtures_df, how="inner", left_on=["team"], right_on=["team_a"])
    h_players = pd.merge(players_df, fixtures_df, how="inner", left_on=["team"], right_on=["team_h"])

    a_players['diff'] = a_players['team_a_strength'] - a_players['team_h_strength']
    h_players['diff'] = h_players['team_h_strength'] - h_players['team_a_strength']

    players_df = a_players.append(h_players)
    return players_df

def get_data():
    today = datetime.now()
    key = "odds" +today.strftime("%d-%m-%Y") +".csv"
    bucket_name = "odds-bucket-conora"
    resp = s3_client.get_object(Bucket=bucket_name, Key=key)
    bet_df = pd.read_csv(resp['Body'], sep=',')
    bet_df['home_chance'] = 100/bet_df['home_odds']
    bet_df['away_chance'] = 100/bet_df['away_odds']
    players =  get('https://fantasy.premierleague.com/api/bootstrap-static/')
    players_df = pd.DataFrame(players['elements'])
    teams_df = pd.DataFrame(players['teams'])
    fixtures_df = pd.DataFrame(players['events'])
    today = datetime.now().timestamp()
    fixtures_df = fixtures_df.loc[fixtures_df.deadline_time_epoch>today]
    gameweek =  fixtures_df.iloc[0].id
    players_df = players_df[columns]
    players_df.chance_of_playing_next_round = players_df.chance_of_playing_next_round.fillna(100.0)
    players_df.chance_of_playing_this_round = players_df.chance_of_playing_this_round.fillna(100.0)
    fixtures = get('https://fantasy.premierleague.com/api/fixtures/?event='+str(gameweek))
    fixtures_df = pd.DataFrame(fixtures)

    fixtures_df['home_chance'] = bet_df['away_chance']
    fixtures_df['away_chance'] = bet_df['home_chance']

    fixtures_df=fixtures_df.drop(columns=['id'])
    teams=dict(zip(teams_df.id, teams_df.name))
    players_df['team_name'] = players_df['team'].map(teams)
    fixtures_df['team_a_name'] = fixtures_df['team_a'].map(teams)
    fixtures_df['team_h_name'] = fixtures_df['team_h'].map(teams)

    home_strength=dict(zip(teams_df.id, teams_df.strength_overall_home))
    away_strength=dict(zip(teams_df.id, teams_df.strength_overall_home))

    fixtures_df['team_a_strength'] = fixtures_df['team_a'].map(away_strength)
    fixtures_df['team_h_strength'] = fixtures_df['team_h'].map(home_strength)

    a_players = pd.merge(players_df, fixtures_df, how="inner", left_on=["team"], right_on=["team_a"])
    h_players = pd.merge(players_df, fixtures_df, how="inner", left_on=["team"], right_on=["team_h"])

    a_players['diff'] = a_players['away_chance'] - a_players['home_chance']
    h_players['diff'] = h_players['home_chance'] - h_players['away_chance']

    players_df = a_players.append(h_players)
    return players_df, fixtures_df, gameweek

def calc_out_weight(players):
    players['out_weight'] = 100
    players['out_weight']-= players['diff']
    players['out_weight']-= players['form'].astype("float")*10
    players['out_weight']+= (100 - players['chance_of_playing_this_round'].astype("float"))*0.2
    players.loc[players['element_type'] ==1, 'out_weight'] -=10
    players.loc[players['out_weight'] <0, 'out_weight'] =0

    return players

def calc_in_weights(players):
    players['in_weight'] = 1
    players['in_weight'] += players['diff']
    players['in_weight'] += players['form'].astype("float")*10
    players['in_weight'] -= (100 - players['chance_of_playing_this_round'].astype("float"))*0.2
    players.loc[players['in_weight'] <0, 'in_weight'] =0

    return players

def put_df(df, bucket, key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer,index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())
    
    
def get(url):
    response = requests.get(url)
    return json.loads(response.content)