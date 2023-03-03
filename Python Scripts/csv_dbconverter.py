import pandas as pd
import sqlite3
import numpy as np
import requests
import time
import io
from datetime import date
from datetime import timedelta
#
# def dataframe_difference(df1, df2, which=None):
#     """Find rows which are different between two DataFrames."""
#     comparison_df = df1.merge(
#         df2,
#         indicator=True,
#         how='outer'
#     )
#     if which is None:
#         diff_df = comparison_df[comparison_df['_merge'] != 'both']
#     else:
#         diff_df = comparison_df[comparison_df['_merge'] == which]
#     #diff_df.to_csv('data/diff.csv')
#     return diff_df

# Read sqlite query results into a pandas DataFrame
con1 = sqlite3.connect("PeruData.db")
print(con1)
df_deaths = pd.read_sql_query("SELECT * from Deaths", con1)

# sort based on date acquired (fecha_resultado) feature.
df_deaths_sorted = df_deaths.sort_values(by='fecha_resultado')

df_deaths_sorted.drop('fecha_recopilacion', axis=1, inplace=True)
# print("dataframe after sorting 1")
# print(df_deaths_sorted)
# df_deaths_group = df_deaths_sorted.groupby('fecha_resultado').size().reset_index(name='Total Deaths')
# print(df_deaths_group)

# Get today's date
today = date.today()
today_str = str(today)
file_name = 'Deaths_'+today_str+'.csv'
print(file_name)
df = pd.read_csv(file_name)


# sort based on date acquired (fecha_resultado) feature.
df = df.sort_values(by='fecha_resultado')
df.drop('fecha_recopilacion', axis=1, inplace=True)
print("database df rows: ", df_deaths_sorted.shape[0])
print(df_deaths_sorted.tail(5))
print("yesterday's df rows: ", df.shape[0])
# print("dataframe after sorting df")
# print(df)

if df.shape[0]>df_deaths_sorted.shape[0]:
    df3 = df[df_deaths_sorted.shape[0]:]
    #df_deaths_sorted.append(df3, ignore_index = True)
    res = [df_deaths_sorted, df3]
    df_deaths_sorted = pd.concat(res)

# print(df_deaths_sorted.shape[0])
df_deaths_sorted = df_deaths_sorted.reset_index()
df = df.reset_index()
df.drop('index', axis=1, inplace=True)
df_deaths_sorted.drop('index', axis=1, inplace=True)
# print(df_deaths_sorted.tail())
# print(df.tail())
#
# print(df_deaths_sorted.equals(df))
# print(df.shape)
# print(df_deaths_sorted.shape)
print(df3)
print(df.tail(31))
print(df_deaths_sorted.tail(31))

# df_cases = pd.read_sql_query("SELECT * from Positive_Cases", con1)
#
# # sort based on date acquired (fecha_resultado) feature.
# df_cases_sorted = df_cases.sort_values(by='fecha_resultado')
#
# print("dataframe after sorting 2")
# print(df_cases_sorted)
# df_cases_group = df_cases_sorted.groupby('fecha_resultado').size().reset_index(name='Total Positive Cases')
# print(df_cases_group)
#
# # OUTER JOIN -take all Values
# merged_df = pd.merge(df_deaths_group, df_cases_group, how='outer')
# merged_df = merged_df.sort_values(by='fecha_resultado')
# merged_df = merged_df.fillna(0)
# merged_df.to_csv('PeruCasesDeaths.csv', encoding='utf-8', index=False)
con1.close()
