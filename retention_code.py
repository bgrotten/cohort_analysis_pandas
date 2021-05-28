import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt
import seaborn as sns
import os
from DBcm import UseDatabase
import pymysql as pm
from sql_queries import sql_query_retention
from getpass import getpass


host = 'localhost'
database = str(input('database: '))
user = str(input('db_user: '))
password = getpass()
business_model = str(input('pls enter B2B or B2C: '))

config = {'host': host,
          'user': user,
          'password': password,
          'database': database, }

def query_Result_Logging_Retention():
    "connects to server and return all result from query"

    with UseDatabase(config) as cursor:
        # Run SQL script to insert values
        _SQL = sql_query_retention(business_model)
        print('query running... ')
        cursor.execute(_SQL)
        result = cursor.fetchall()
        dataset = []
        for row in result:
            dataset_dict = {
                'user_id': row[0],
                'log_date': row[1],
                'enrollment': row[2],
                'hardware': row[3],
                'is_connected': row[4],
                'last_app_use': row[5],
                'is_IOS': row[6],
                'is_ANDROID': row[7],
                'is_not_IOS_is_not_ANDROID': row[8],
            }
            dataset.append(dataset_dict)
        print('done.. process results... ')
        return dataset


def cohort_period(df):
    df['CohortPeriodInMonth'] = np.arange(len(df))
    return df

def plot_graphs(df_bar_chart_left, title_bar_chart_left, ylabel_bar_chart_left, df_heatmap, title_heatmap):
    sns.set(style = 'white', font_scale=0.8)
    grid = plt.GridSpec(5, 6, wspace=0.4, hspace=0.3)
    ax1 = plt.subplot(grid[2:5, 0:2])
    ax2 = plt.subplot(grid[2:5, 3:])

    df_bar_chart_left.plot(kind='barh',
                           color='green',
                           ax=ax1,)

    ax1.set_title(title_bar_chart_left)
    ax1.set_ylabel(ylabel_bar_chart_left)
    ax1.invert_yaxis()

    for i, v in enumerate(df_bar_chart_left):
        ax1.text(v + 1, i + 0.10, str(v), color='black', fontsize=8)

    sns.heatmap(df_heatmap,
                ax = ax2,
                cmap = plt.cm.Greens,
                mask = df_heatmap.isnull(),  # data will not be shown where it's True
                annot = True,  # annotate the text on top
                linewidths=.5,
                fmt = '.0%'
                )  # string formatting when annot is True

    ax2.set_title(title_heatmap)
    #sns.savefig(str(title_heatmap) + '.png')
    plt.show()


def modify_dataframe(df):
    """Reading SQL csv result and converting columns to write data format & structure """
    # Reformat dates from DB into a DateTime
    df['enrollment_date'] = pd.to_datetime(df.loc[:, 'enrollment'])
    df['logging_date'] = pd.to_datetime(df.loc[:, 'log_date'], errors='coerce')
    # Formating Date to YYYY-MM
    df['enrollment_month']= df['enrollment_date'].dt.to_period('M')
    df['logging_month']= df['logging_date'].dt.to_period('M')
    return df

def group_logging_data(df):
    """Calculates User Retention by enrollment and cohort - # dropping first column 
    --> signUp month values ensure at least 30 days in program """
    grouped = df.groupby(['enrollment_month', 'logging_month'])
    cohorts = grouped.agg({'user_id': "count"
                        })

    cohorts.rename(columns={'user_id': 'users_logging',
                            }, inplace=True)

    cohorts = cohorts.groupby(level=0).apply(cohort_period)
    cohorts.reset_index(inplace=True)
    cohorts.set_index(['enrollment_month', 'CohortPeriodInMonth'], inplace=True)
    return cohorts

def retention_df(df_group_logging_data, df_sign_ups_per_month):
    logging_retention_users = df_group_logging_data['users_logging'].unstack(0)
    user_retention = logging_retention_users.divide(df_sign_ups_per_month, axis=1)
    user_retention = user_retention.T
    user_retention = user_retention.drop([0], axis=1)
    logging_retention_users = logging_retention_users.T
    logging_retention_users = logging_retention_users.drop([0], axis=1)
    return user_retention

#########################
## START OF CODE
#########################

df = pd.DataFrame(query_Result_Logging_Retention()) #, columns= ['user_id', 'log_date', 'enrollment', 'hardware', 'is_connected', 'last_app_use', 'is_IOS', 'is_ANDROID', 'is_not_IOS_is_not_ANDROID'])
df = modify_dataframe(df)

#########################
## DF for ALL B2B USERS
#########################

# Slices Dataframe to needed columns and erases dublicates (similar to Distinct)
# create a Series holding the total size of each CohortGroup // all users that signed up in month
df_all_users = df.loc[:, ['user_id', 'enrollment_month', 'logging_month']]
df_sign_up_per_month_all_users = df_all_users.loc[:, ['user_id', 'enrollment_month']]
df_sign_up_per_month_all_users = df_sign_up_per_month_all_users.drop_duplicates()
df_sign_up_per_month_all_users = df_sign_up_per_month_all_users.groupby(['enrollment_month'])['user_id'].count()
df_all_users = df_all_users.drop_duplicates()

print(df_all_users)
print(df_sign_up_per_month_all_users)

# calculate user_retention
cohorts_all_users = group_logging_data(df_all_users)
user_retention_all_users = retention_df(cohorts_all_users, df_sign_up_per_month_all_users)
print(cohorts_all_users)
print(user_retention_all_users)
plot_graphs(df_sign_up_per_month_all_users, 'Users: signUp per month - ALL B2B', '# of users', user_retention_all_users, 'Cohorts: Logging Retention')


#########################
## DF for IOS
#########################
df_IOS = df.loc[df['is_IOS'] == 'yes']
df_IOS = df_IOS.loc[:, ['user_id', 'enrollment_month', 'logging_month']]
df_sign_up_per_month_IOS = df_IOS.loc[:, ['user_id', 'enrollment_month']]
df_sign_up_per_month_IOS = df_sign_up_per_month_IOS.drop_duplicates()
df_sign_up_per_month_IOS = df_sign_up_per_month_IOS.groupby(['enrollment_month'])['user_id'].count()
df_IOS = df_IOS.drop_duplicates()

cohorts_IOS = group_logging_data(df_IOS)
user_retention_IOS = retention_df(cohorts_IOS, df_sign_up_per_month_IOS)

plot_graphs(df_sign_up_per_month_IOS, 'Group: IOS signUp per month', '# of users', user_retention_IOS, 'IOS: Logging Retention')


#########################
## DF for ANDROID
#########################

df_ANDROID = df.loc[df['is_ANDROID'] == 'yes']
df_ANDROID = df_ANDROID.loc[:, ['user_id', 'enrollment_month', 'logging_month']]
df_sign_up_per_month_ANDROID = df_ANDROID.loc[:, ['user_id', 'enrollment_month']]
df_sign_up_per_month_ANDROID = df_sign_up_per_month_ANDROID.drop_duplicates()
df_sign_up_per_month_ANDROID = df_sign_up_per_month_ANDROID.groupby(['enrollment_month'])['user_id'].count()
df_ANDROID = df_ANDROID.drop_duplicates()

cohorts_ANDROID = group_logging_data(df_ANDROID)
user_retention_ANDROID = retention_df(cohorts_ANDROID, df_sign_up_per_month_ANDROID)

plot_graphs(df_sign_up_per_month_ANDROID, 'Group: ANDROID signUp per month', '# of users', user_retention_ANDROID, 'ANDROID: Logging Retention')


#########################
## DF for connected Users
#########################

df_is_connected = df.loc[df['is_connected'] == 'yes']
df_is_connected = df_is_connected.loc[:, ['user_id', 'enrollment_month', 'logging_month']]
df_sign_up_per_month_is_connected = df_is_connected.loc[:, ['user_id', 'enrollment_month']]
df_sign_up_per_month_is_connected = df_sign_up_per_month_is_connected.drop_duplicates()
df_sign_up_per_month_is_connected = df_sign_up_per_month_is_connected.groupby(['enrollment_month'])['user_id'].count()
df_is_connected = df_is_connected.drop_duplicates()

cohorts_is_connected = group_logging_data(df_is_connected)
user_retention_is_connected = retention_df(cohorts_is_connected, df_sign_up_per_month_is_connected)

plot_graphs(df_sign_up_per_month_is_connected, 'Group: is_connected signUp per month', '# of users', user_retention_is_connected, 'is_connected: Logging Retention')


##############################
## DF for non connected Users
##############################

df_is_not_connected = df.loc[df['is_connected'] == 'no']
df_is_not_connected = df_is_not_connected.loc[:, ['user_id', 'enrollment_month', 'logging_month']]
df_sign_up_per_month_is_connected = df_is_not_connected.loc[:, ['user_id', 'enrollment_month']]
df_sign_up_per_month_is_connected = df_sign_up_per_month_is_connected.drop_duplicates()
df_sign_up_per_month_is_connected = df_sign_up_per_month_is_connected.groupby(['enrollment_month'])['user_id'].count()
df_is_not_connected = df_is_not_connected.drop_duplicates()

cohorts_is_connected = group_logging_data(df_is_not_connected)
user_retention_is_connected = retention_df(cohorts_is_connected, df_sign_up_per_month_is_connected)

plot_graphs(df_sign_up_per_month_is_connected, 'Group: is_not_connected signUp per month', '# of users', user_retention_is_connected, 'is_not_connected: Logging Retention')