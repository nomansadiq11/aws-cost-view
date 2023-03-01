#!/usr/bin/env python3

import boto3
import click
import sqlite3
from sqlite3 import Error

from calendar import monthrange
from datetime import datetime
from prettytable import PrettyTable

# define table layout
pt = PrettyTable()

pt.field_names = [
    'TimePeriodStart',
    'USAGE_TYPE',
    'Service',
    'Amount',
]

database = r"data.db"

pt.align = "l"
pt.align["Amount"] = "r"


def get_cost_and_usage(bclient: object, start: str, end: str) -> list:
    cu = []

    while True:
        data = bclient.get_cost_and_usage(
            TimePeriod={
                'Start': start,
                'End':  end,
            },
            Granularity='MONTHLY',
            Metrics=[
                'UnblendedCost',
            ],
            GroupBy=[
                {
                    'Type': 'DIMENSION',
                    'Key': 'USAGE_TYPE',
                },
                {
                    'Type': 'DIMENSION',
                    'Key': 'SERVICE',
                }

            ],
        )

        cu += data['ResultsByTime']
        token = data.get('NextPageToken')

        if not token:
            break

    return cu


def fill_table_content(results: list, start: str, end: str) -> None:

    conn = create_connection(database)

    total = 0
    for result_by_time in results:
        for group in result_by_time['Groups']:
            amount = float(group['Metrics']['UnblendedCost']['Amount'])

            total += amount
            # Skip, if total amount less then 0.00001 USD
            if amount < 0.00001:
                continue


            data = ('account', result_by_time['TimePeriod']['Start'], group['Keys'][0], group['Keys'][1], format(amount, '0.5f'))
            adddata(conn, data)


            pt.add_row([
                result_by_time['TimePeriod']['Start'],
                group['Keys'][0],
                group['Keys'][1],
                format(amount, '0.5f'),
            ])
    print("Total: {:5f}".format(total))


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn



def adddata(conn, task):


    sql = ''' INSERT INTO awscostdata(account,TimePeriodStart,USAGE_TYPE,Service,Amount)
              VALUES(?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, task)
    conn.commit()
    return 1

@click.command()
@click.option('-P', '--profile', help='profile name')
@click.option('-S', '--start', help='start date (default: 1st date of current month)')
@click.option('-E', '--end', help='end date (default: last date of current month)')
def report(profile: str, start: str, end: str) -> None:
    # set start/end to current month if not specify
    if not start or not end:
        # get last day of month by `monthrange()`
        # ref: https://stackoverflow.com/a/43663
        ldom = monthrange(datetime.today().year, datetime.today().month)[1]

        start = "2022-03-01"
        end = "2022-03-31"

        # start = datetime.today().replace(day=1).strftime('%Y-%m-%d')
        # end = datetime.today().replace(day=ldom).strftime('%Y-%m-%d')

    # cost explorer
    SERVICE_NAME = 'ce'
    bclient = boto3.Session(profile_name=profile).client(SERVICE_NAME)

    results = get_cost_and_usage(bclient, start, end)
    print(results)
    fill_table_content(results, start, end)

    print(pt)


if __name__ == '__main__':

    report()