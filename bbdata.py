#!/usr/local/bin/python3
"""Blackboard Data API
Usage:
    bbdata.py [options]
    bbdata.py (-h | --help)
    bbdata.py --version

Commands:
    -s <store>, --store <store>  Set the persistence layer. [default: mongo]

Options:
General Options:
    -h, --help      Show this screen.
    -v, --version   Show version.
    -D, --debug     Enable debug mode: verbose.
    --pprint        Pretty print the results.
"""
import json
import sys
import re
import pandas as pd
import matplotlib.pyplot as plt

from docopt import docopt
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from pymongo import MongoClient, ReturnDocument, ASCENDING
from pprint import pprint
from settings import config as config, DEFAULT_MODE
from pathlib import Path

def to_dict(result):
    return [dict(zip(row.keys(), row)) for row in result]


def load_query(query_name, chunk_size=None, refresh=False, as_dict=False):
    query = None
    variables = None
    data_path = f"./queries/{query_name}/data.csv"
    
    if not refresh and Path(data_path).is_file():
        return pd.read_csv(data_path)
    else:
        print ("Data file does not exist")
        with open(f"./queries/{query_name}/query.sql") as qf:
            query = qf.read()

        with open(f"./queries/{query_name}/variables.json") as vf:
            variables = json.load(vf)['variables']

        for k, v in variables.items():
            query = query.replace('{' + k + '}', build_type(v))

        if as_dict:
            # direct via sqlalchemy engine
            return to_dict(sessions['bbdata'].execute(query))
        else:
            # using pandas
            df = pd.read_sql_query(query, engines['bbdata'], chunksize=chunk_size)
            df.to_csv(data_path, index=None)
            return df

def build_type(value):
    if type(value) == dict:
        return json.dumps(value)
    if type(value) == list:
        return ','.join([build_type(i) for i in value])
    if type(value) == int:
        return str(value)
    if type(value) == str:
        return f"'{value}'"


def main():
    ###### Simple user load
    # load_user = load_query('get-user')
    # print(load_user)

    ###### Load Users Activity
    # load_user_activity = load_query('get-user-activity', chunk_size=10, as_dict=True)
    # _print = True
    # for page in load_user_activity:
    #     if _print:
    #         print(page)
    #         _print = False
    #     else:
    #         break

    # load_user_activity = load_query('get-user-activity')
    # print(load_user_activity.head(10))

    ###### Load Users Activity alt
    # load_user_activity = load_query('get-user-activity')
    # lua = load_user_activity.plot(x='person_id', y='course_id').scatter(x='person_id', y='course_id')
    
    ###### Load a User's Course Activity for Minute spent
    # luca = load_query('get-user-course-activity')
    # fig= luca[['name', 'duration_minutes']].head(10).plot(
    #     x='name',
    #     y='duration_minutes',
    #     yticks=luca['duration_minutes'],
    #     kind='bar',
    #     figsize=(10,5),
    #     fontsize=10
    # )
    
    # fig.set_xlabel("Course")
    # fig.set_ylabel("Duration in Mintues")
    # fig.bar(
    #     x='name',
    #     height='duration_minutes'
    # )
    
    # plt.savefig('./exports/GetUserCourseActivity.svg')

    ###### Load Users Course Activity Over Time (dates)
    
    # lucaot = load_query('get-user-course-activity-over-time', refresh=False)
    # data = lucaot[lucaot.name.str.contains('SLS1101-20191-21-M-003')].plot(
    #     x='name',
    #     y='duration_minutes',
    #     kind='bar'
    # )
    # print(data)

    # lucaot.head(15).groupby('name').plot(kind='bar')


    # Time spent in Learn
    # tsil = load_query('time-spent-in-learn')
    # print(tsil.head(10))
    iabt = load_query('instructor-activity-by-term')
    fig, ax = plt.subplots()
    tabs = ['tab:blue', 'tab:orange', 'tab:green', 'tab:red', 'tab:black', 'tab:purple']
    for i, color in enumerate(iabt['term']):
        x = iabt['enrolment_count']
        y = iabt['courses_accessed']
        scale = iabt['course_accesses'] * .5
        ax.scatter(x, y, c=tabs[i], s=scale, label=color,
                alpha=0.3, edgecolors='none')


    ax.legend()
    ax.grid(True)
    plt.savefig('./exports/InstructorActivityByTerm.svg')
    plt.show()

    for db, session in sessions.items():
        session.close()
        engines[db].dispose()

    print('Job Complete!!')


if __name__ == '__main__':

    args = docopt(__doc__, version='Workday To Blackboard Sync API 0.0.5')

    store = args['--store']
    debug = args['--debug']

    client = None

    if store == 'mongo':
        # connect to mongo
        client = MongoClient(config['mongo'][DEFAULT_MODE]['url'])
        mongo_db = client[config['mongo'][DEFAULT_MODE]['database']]

    conn_str = f"snowflake://{config['bbdata']['user']}:{config['bbdata']['pass']}@{config['bbdata']['account']}/{config['bbdata']['database']}/{config['bbdata']['schema']}"
    engines = {
        'bbdata': create_engine(conn_str, echo=debug)
    }

    sessions = {
        'bbdata': sessionmaker(bind=engines['bbdata'])(),
    }

    output_file = './exports/GetUser.csv'
    log_file = './exports/ErrorLog.csv'
    main()
