
print('#### Package Loaded ####')

import json
import pandas as pd
from pprint import pprint
from collections import Counter
pd.options.mode.chained_assignment = None  # default='warn'

# gameRecordsDataAnon_16-01-2021.json
def read_convert_json_record(json_name):
    ### read the file
    with open(json_name, 'r') as f:
        json_data = f.readlines()

    ## fix the json
    json_data = json_data[0]
    json_data = json_data.replace('][][', ',') # fix
    json_data = json_data.replace('][', ',') # fix
    json_data = json.loads(json_data)

    print('complete loading!')
    print('==========')
    print('')
    # pprint(json_data[0])

    ## convert into df
    data_set = pd.DataFrame.from_dict(json_data, orient='columns')
    print('rows and columns:', data_set.shape)
    return  data_set # json_data


def filter_data(df):
    ## filter only 'Merlin', 'Percival', 'Assassin', 'Morgana' in the game
    game_role = {'Merlin', 'Percival', 'Assassin', 'Morgana'}
    df = df[df['roles'].map(set) == game_role]

    ## filter out the supplement rule (e.g., lady of the lake...)
    df = df[df['cards'].map(set) == set()]

    ## filter only 'Avalon' mode in the game
    df = df[df['gameMode'] == 'avalon']

    ## filter in only the 5 or 6 player games
    df = df[df['numberOfPlayers'].isin([5, 6])]

    print('Complete filtering!')
    return df

def create_new_role(role_series):
    role_dict = {}
    is_soldier2 = 0

    for k, v in role_series.items():
        new_role = v.get('role', 'Missing')
        # replace 'Resistance' with 'Soldier1' or 'Soldier2'
        if new_role == 'Resistance':
            new_role = 'Soldier1'
            if is_soldier2 == 1:
                new_role = 'Soldier2'
            is_soldier2 += 1
        role_dict[k] = new_role
    return role_dict

## what is the order of the leader? seems random start but follow the same reverse order (e.g., c/b/a... , b/a/e...) Seems right
def create_leadership_seq(row):
    result = []
    for r in range(len(row['missionHistory'])):
        for p in row['voteHistory'].keys():
            if 'VHleader' in row['voteHistory'].get(p)[r][-1]:
                result.append(p)
                break
    # convert to role
    result2 = [row['playerRolesNew'].get(r) for r in result]
    return result2

def create_leadership_seq_team(seq_series):
    team_dict = {
    'Merlin': 'Resistance',
    'Percival': 'Resistance',
    'Soldier1': 'Resistance',
    'Soldier2': 'Resistance',
    'Assassin': 'Spy',
    'Morgana': 'Spy'}
    return [team_dict.get(i) for i in seq_series]

def create_each_quest_team(row):
    result = []
    each_quest_team_list = []
    for r in range(len(row['missionHistory'])):
        each_quest_team_list = []
        for p in row['voteHistory'].keys():
            if 'VHpicked' in row['voteHistory'].get(p)[r][-1]:
                each_quest_team_list.append(p)
        result.append(each_quest_team_list)
#     convert to role
    result2 = [[row['playerRolesNew'].get(r) for r in q] for q in result]
    ## The last list comprehension break down
    # result = [['c', 'd'], ['b', 'c', 'd'], ['c', 'd']]
    # result2 = []
    # for q in result:
    #     tt = []
    #     for r in q:
    #         tt.append(row['playerRoles_new'].get(r))
    #     result2.append(tt)
    return result2

def count_failed_vote(series_vote):
    return sum([len(i)-1 for i in series_vote['a']])

def create_new_feature(df):
    ## time feature
    df['timeGameStarted'] = pd.to_datetime(df['timeGameStarted'])
    df['timeGameFinished'] = pd.to_datetime(df['timeGameFinished'])
    df['timeAssassinationStarted'] = pd.to_datetime(df['timeAssassinationStarted'])

    df['timeGameSpan'] = round((df['timeGameFinished'] - df['timeGameStarted']).dt.seconds/60, 2)
    df['timeAssasinationSpan'] = round((df['timeGameFinished'] - df['timeAssassinationStarted']).dt.seconds/60, 2)
    df['timeGameStarted'] = pd.to_datetime(df['timeGameStarted'].dt.strftime('%m/%d/%Y %H:%M:%S'))

    ## how many round? use length of missionHistory
    df['totalRound'] = df['missionHistory'].map(len) # total_round
    # {'a': 'Morgana', 'b': 'Assassin', 'c': 'Percival', 'd': 'Soldier1', 'e': 'Merlin', 'f': 'Soldier2'}
    df['playerRolesNew'] = df['playerRoles'].map(create_new_role)

    df['leadershipSeq'] = df.apply(create_leadership_seq, axis='columns')
    df['leadershipSeqTeam'] = df['leadershipSeq'].map(create_leadership_seq_team)
    df['eachQuestTeamMember'] = df.apply(create_each_quest_team, axis='columns')
    df['failedVoteCount'] = df['voteHistory'].map(count_failed_vote)
    ## filter out the records with less than 3 round of quests (There less than 100 records)
    df = df[df['leadershipSeq'].map(len) >= 3]

    ## who vote in each round? [[Merlin, Soldier, Morgana], [Morgana, assassin, soldier]]
    ## TBD

    ## clean some redundant labels
    df['howTheGameWasWon'] = (df['howTheGameWasWon'].
    map({'Mission successes and Merlin did not die.': 'Mission successes and assassin shot wrong.'}).
    fillna(df['howTheGameWasWon']))

    print('Complete creating new features!')
    return df


def drop_feature(df):
    drop_feature_list = ['botUsernames', 'playerUsernamesOrderedReversed',
    'ladyHistoryUsernames', 'refChain', 'refHistoryUsernames', 'sireChain',
    'sireHistoryUsernames', 'cards', 'ladyChain', 'whoAssassinShot2',
    'timeGameFinished'] # timeGameStarted timeAssassinationStarted
    df = df.drop(drop_feature_list, axis=1)
    print('Complete dropping!')
    return df

def data_pipeline_df(json_name):
    df = read_convert_json_record(json_name)
    df = filter_data(df)
    df = create_new_feature(df)
    df = drop_feature(df)

    print('Complete data pipeline!')
    print('------------------------')
    return df
