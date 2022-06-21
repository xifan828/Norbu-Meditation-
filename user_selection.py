import pandas as pd
import numpy as np
from datetime import datetime, timedelta



# group data on user's pseudo_id, get the earliest and latest session_start date, take their difference as active duration
#def user_active_duration(data):
#    data = data.query('event_name == "session_start"')
#    grouped_data = data.groupby('user_pseudo_id')
#    user_duration = grouped_data['event_date'].agg(['min', 'max'])
#    user_first_touch = grouped_data['user_first_touch_timestamp'].min().dt.floor('d')
#    user_duration = user_duration.join(user_first_touch)
#    user_duration['active_duration'] = user_duration['max'] - user_duration['user_first_touch_timestamp']
#    return user_duration
#def user_active_duration(data):
#    grouped = data.groupby(['user_pseudo_id', 'event_date'])['event_name'].count()
#    grouped = grouped.reset_index()
#    user_duration = (grouped[grouped['event_name'] > 0]
#                     .groupby('user_pseudo_id')['event_date']
#                     .agg(['min', 'max']))
#    user_first_touch = data.groupby('user_pseudo_id')['user_first_touch_timestamp'].min().dt.floor('d')
#    user_duration = user_duration.join(user_first_touch)
#    user_duration['active_duration'] = user_duration['max'] - user_duration['user_first_touch_timestamp']
#   return user_duration
def cal_acitivity(latest_session, fisrt_touch):
    if latest_session == np.nan:
        return timedelta(days=0)
    else:
        return latest_session - fisrt_touch

def user_active_duration(data, sub_data):
    m_group = data.groupby(['user_pseudo_id', 'event_date'])['event_name'].count().reset_index()
    users_list = m_group['user_pseudo_id'].unique()
    max_date = pd.read_csv('latest session date.csv')
    max_date = max_date.drop('Unnamed: 0', axis=1)
    max_date.index = users_list
    max_date.columns = ['max']
    max_date['max'] = pd.to_datetime(max_date['max'], format='%Y-%m-%d')
    user_first_touch = sub_data.groupby('user_pseudo_id')['user_first_touch_timestamp'].min().dt.floor('d')
    max_date = max_date.join(user_first_touch, how='inner')
    max_date['active_duration'] = max_date.apply(lambda x: cal_acitivity(x['max'], x['user_first_touch_timestamp']), axis=1)
    max_date['active_duration'] = max_date['active_duration'].fillna(timedelta(days=0))
    return max_date
    

# calculate retention rate based on the user_active_duration, to note the earliest session_start date shall be at least days ahead of the end of time period. 
# for example: if the end date is 28-02-2022 and days for retention are 7, then the earliest session_start can not be later than 21-02-2022
def retention_rate(data, days):
    #date_end = end_date - pd.Timedelta(days=days)
    #data = data.loc[data['min'] <= date_end]
    retained_user = data.loc[data['active_duration'].dt.days >= days]
    retention_rate = len(retained_user) / len(data)
    return retention_rate

# generate a list of single date from a period of datetimes 
def single_date_generator(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)

# slice data with users, whose first_open is on the same specific date ==> a single day cohort is created
def first_open(data, start_date, end_date):
    cond1 = (data['user_first_touch_timestamp'].dt.floor('d') >=  start_date)
    cond2 = (data['user_first_touch_timestamp'].dt.floor('d') <=  end_date)
    first_open_user = data.loc[cond1 & cond2]['user_pseudo_id'].unique()
    first_open_user_data = data[data['user_pseudo_id'].isin(first_open_user)]
    return first_open_user_data

# calculate the cohort retention rate based on user_active_duration
def cohort_retention_rate(data, days):
    cohort_retention_rate = (data['active_duration'].dt.days >= days).mean()
    return round(cohort_retention_rate, 3)

def user_query(user_id, data):
    return data[data['user_pseudo_id'] == user_id].sort_values(by='event_timestamp')

def event_query(event, data):
    return data[(data['event_name'] == event) | (data['event_name'].str.contains(event))]

def merge_tb(user_behavior, new_table):
    return user_behavior.merge(new_table, on='user_pseudo_id', how='left')


def get_purchase(data):
    cond1 = (data['event_name'] == "norbu_in_app_purchase")
    cond2 = (data['event_name'] == 'scr_premium')
    cond3 = (data['event_name'] == 'in_app_purchase')
    data_purchase = data.loc[cond1 | cond2 | cond3]
    return data_purchase

def del_abnormal(data):
    q1 = data['active_duration'].quantile(0.25)
    q3 = data['active_duration'].quantile(0.75)
    iqr = q3 - q1
    upper = q3 + 2.5 * iqr
    clean_data = data[data['active_duration'] <= upper]
    return clean_data

def get_tariff(data):
    month = 0
    year = 0
    life = 0
    for row in data.rows:
        if row['event_value_in_usd'] < 10:
            month += 1
        elif 10 < row['event_value_in_usd'] < 40:
            year += 1
        else:
            life += 1
    return month, year, life