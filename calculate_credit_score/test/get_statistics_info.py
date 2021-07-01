import os
import sys

TOP_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.join(TOP_DIR, '../'))

import csv
import time

import numpy as np
from py2neo import Graph
from scipy.stats.stats import _contains_nan

from config.config import Neo4jConfig


def get_standardized_score_info(a, axis=0, ddof=0, nan_policy='propagate'):
    a = np.asanyarray(a)

    contains_nan, nan_policy = _contains_nan(a, nan_policy)

    if contains_nan and nan_policy == 'omit':
        mns = np.nanmean(a=a, axis=axis, keepdims=True)
        sstd = np.nanstd(a=a, axis=axis, ddof=ddof, keepdims=True)
    else:
        mns = a.mean(axis=axis, keepdims=True)
        sstd = a.std(axis=axis, ddof=ddof, keepdims=True)

    return mns[0], sstd[0]


def get_property(property, getter):
    if property in getter:
        return getter[property]
    else:
        return 0


def calculate_average_second(values, timestamps, time_current):
    if values == 0:
        return 0
    if (len(values) == 1):
        if time_current == timestamps[0]:
            return 0
        else:
            return (values[0] / (time_current - timestamps[0]))
    sum = 0
    for i in range(len(values) - 1):
        sum += values[i] * (timestamps[i + 1] - timestamps[i])
    sum += values[-1] * (time_current - timestamps[-1])
    total_time = time_current - timestamps[1]
    average = sum / total_time
    return average


def get_statistics():
    k = 100
    # get wallet data from KG
    bolt = f"bolt://{Neo4jConfig.HOST}:{Neo4jConfig.BOTH_PORT}"
    graph = Graph(bolt, auth=(Neo4jConfig.NEO4J_USERNAME, Neo4jConfig.NEO4J_PASSWORD))
    try:
        graph.run("Match () Return 1 Limit 1")
        print('Neo4j Database is connected')
    except Exception:
        print('Neo4j Database is not connected')
    getter = graph.run("match (w:Wallet) return w;").data()
    total_assets_statistic = {'mean': 0, 'std': 0}
    age_of_account_statistic = {'mean': 0, 'std': 0}
    frequency_of_transaction = {'mean': 0, 'std': 0}
    transaction_amount_statistic = {'mean': 0, 'std': 0}

    timeCurrent = int(time.time())

    createdAt = []
    # dailyFrequencyOfTransactions = []
    dailyTransactionAmounts = []
    total_asset = []

    for i in range(len(getter)):
        # get age info x21
        temp_age = get_property('createdAt', getter[i]['w'])
        if(temp_age > 0):
            createdAt.append(temp_age)

        # Get Daily Frequent Transaction x22
        # dailyFrequencyOfTransactions_temp = get_property('dailyFrequencyOfTransactions', getter[i]['w'])
        # if (dailyFrequencyOfTransactions_temp == 0):
        #     dailyFrequencyOfTransactions_temp.append(0)
        # else:
        #     dailyFrequencyOfTransactions.append(sum(dailyFrequencyOfTransactions_temp))

        # Get Daily Transaction Amount x23
        dailyTransactionAmounts_temp = get_property('dailyTransactionAmounts', getter[i]['w'])
        if (dailyTransactionAmounts_temp <= 0):
            dailyTransactionAmounts.append(0)
        else:
            dailyTransactionAmounts.append(sum(dailyTransactionAmounts_temp))

        # get total asset info

        balanceChangeLogTimestamps = get_property('balanceChangeLogTimestamps', getter[i]['w'])
        balanceChangeLogValues = get_property('balanceChangeLogValues', getter[i]['w'])
        depositChangeLogTimestamps = get_property('depositChangeLogTimestamps', getter[i]['w'])
        depositChangeLogValues = get_property('depositChangeLogValues', getter[i]['w'])
        borrowChangeLogTimestamps = get_property('borrowChangeLogTimestamps', getter[i]['w'])
        borrowChangeLogValues = get_property('borrowChangeLogValues', getter[i]['w'])

        loan_average = calculate_average_second(borrowChangeLogValues, borrowChangeLogTimestamps, timeCurrent)
        balance_average = calculate_average_second(balanceChangeLogValues, balanceChangeLogTimestamps, timeCurrent)
        deposit_average = calculate_average_second(depositChangeLogValues, depositChangeLogTimestamps, timeCurrent)
        total_asset_average = balance_average + deposit_average - loan_average
        if(total_asset_average > 0):
            total_asset.append(total_asset_average)

    timestamp = np.array(createdAt)
    age = timeCurrent - timestamp

    #age = age[age>2000000]
    [age_of_account_statistic['mean'], age_of_account_statistic['std']] = get_standardized_score_info(age)

    [transaction_amount_statistic['mean'], transaction_amount_statistic['std']] = get_standardized_score_info(
        dailyTransactionAmounts)

    [total_assets_statistic['mean'], total_assets_statistic['std']] = get_standardized_score_info(total_asset)
    print('Complete calculating statistics information')
    print("wallet have asset > 0 ", len(total_asset))
    print("wallet have age>0 ", len(age))
    # write to csv file
    fieldnames = ['variable', 'mean', 'std']
    rows = [
        {'variable': 'total_asset',
         'mean': total_assets_statistic['mean'],
         'std': total_assets_statistic['std']},
        {'variable': 'age_of_account',
         'mean': age_of_account_statistic['mean'],
         'std': age_of_account_statistic['std']},
        {'variable': 'transaction_amount',
         'mean': transaction_amount_statistic['mean'],
         'std': transaction_amount_statistic['std']},
        {'variable': 'frequency_of_transaction',
         'mean': frequency_of_transaction['mean'],
         'std': frequency_of_transaction['std']},
    ]
    with open('statistic_report.csv', 'w', encoding='UTF8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == '__main__':
    get_statistics()
    pass
