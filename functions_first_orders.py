#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
from google.cloud import bigquery
import os
import pandas as pd
import re
import numpy as np 
from datetime import date, timedelta
import MySQLdb
from sqlalchemy import create_engine
import configparser
import pygsheets
pd.options.mode.chained_assignment = None

    
def connect_to_gbq(google_bigquery_credentials):
    """Создание подключения к GBQ"""
 
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_bigquery_credentials
    bqc = bigquery.Client()
    website_activity_events = ['clicked_cancel_button_subscription', 'completed_subscription']
    query_time_in_service = 'select user_id,timestamp from production.tracks where event in '    +'(\''+'\', \''.join(website_activity_events)+'\')'
    df_t = bqc.query(query_time_in_service).to_dataframe()      
    return (bqc)


def init_db_connect(ded_db_conf):
    """Подключение к MySQL"""
    #ded_db_conf = get_cred()
    analytics_db_host = ded_db_conf['param']['host']
    analytics_db_port = int(ded_db_conf['param']['port'])
    analytics_db_user = ded_db_conf['param']['user']
    analytics_db_pass = ded_db_conf['param']['pass']
    analytics_db_db = ded_db_conf['param']['db']
    conn = MySQLdb.connect(host=analytics_db_host,
                           port=analytics_db_port,
                           user=analytics_db_user,
                           passwd=analytics_db_pass,
                           db=analytics_db_db,
                           charset='utf8')
    engine = create_engine('mysql://'+ analytics_db_user +':'+
                           analytics_db_pass+'@' + analytics_db_host +'/' + analytics_db_db )
    con = engine.connect()
    return con, conn


def get_orders_data(query_orders, conn):
    """Загрузка ордеров"""
    data = pd.read_sql(sql=query_orders, con=conn)
    return data

def get_client_data(query_clients, conn):
    """Загрузка клиентов"""
    client = pd.read_sql(sql=query_clients, con=conn)
    return client

def preprocess_orders(data, rub_ex = 0.015, eur_ex = 1.11, gbp_ex = 1.214):
    """Препроцессинг ордеров"""
    
    orders = data[data.pid == 0][['id', 'pid', 'client_id', 'start_date', 'until_date', 'currency', 'month', 
                                  'status', 'amount_total']]
    orders = orders[((orders.status == 'accepted') | (orders.status == 'payment_delay'))]
    orders = orders.reset_index()
    orders['mrr'] = orders['amount_total']/orders['month']
    orders['mrr_usd'] = 0
    orders['mrr_usd'][orders.currency == 'RUB'] = orders['mrr']*rub_ex
    orders['mrr_usd'][orders.currency == 'EUR'] = orders['mrr']*eur_ex
    orders['mrr_usd'][orders.currency == 'GBP'] = orders['mrr']*gbp_ex
    orders['mrr_usd'][orders.currency == 'USD'] = orders['mrr']
    return orders

def get_start_end_dates(year, week):
    """Конвертация номера недели в формат номер_недели_дата_начала_недели - дата_конца_недели """
    d = date(year,1,1)
    if(d.weekday()<= 3):
        d = d - timedelta(d.weekday())             
    else:
         d = d + timedelta(7-d.weekday())
    dlt = timedelta(days = (week-1)*7)
    return week,d + dlt,  d + dlt + timedelta(days=6)

def main_transform(orders, client):
    """Мердж ордеров и клиентов"""
    join_data = pd.merge(left= client[['id','start_date', 'company']],right=orders, how='left', left_on='id',
                         right_on='client_id')
    total = join_data[['id_x','id_y','company','start_date_y','until_date','month','mrr_usd']]
    total["id_y"] =  total["id_y"].fillna("0").astype(int)
    return (total)

def main_preprocesing(total, data, in_sales = 28864):
    """Переформирования объединенного фрейма для выделения первых ордеров"""
    dfc = total.groupby('id_x')['start_date_y']
    df_dates = total[['id_x','company']].assign(min=dfc.transform(min), max=dfc.transform(max)).drop_duplicates()   
    df_dates_min =  df_dates[['id_x','min']]
    second = df_dates['min'] == df_dates['max'] 
    df_dates_max = df_dates[-second]
    merge1 = pd.merge(left=df_dates,right=total[['id_x','id_y','start_date_y','month','until_date','mrr_usd']], 
                      how='left', left_on=['id_x','min'], right_on=['id_x','start_date_y'])
    merge1 = merge1.drop('start_date_y',axis=1)
    merge1 = pd.merge(left=merge1,right=total[['id_x','id_y','start_date_y','month','until_date','mrr_usd']], 
                      how='left', left_on=['id_x','max'], right_on=['id_x','start_date_y'])
    merge1 = merge1.drop('start_date_y',axis=1) 
    columnsTitles=["id_x","company", "id_y_x","min","month_x","until_date_x","mrr_usd_x","id_y_y","max",
                   "month_y","until_date_y",'mrr_usd_y']
    merge_min=merge1.reindex(columns=columnsTitles)
    merge_min.columns = ['client_id','company','order_1_id', 'order_1_start_date','order_1_month','order_1_until_date',
                         'first_order_mrr','order_last_id', 'order_last_start_date','order_last_until_date',
                         'order_last_month','last_order_mrr']

    merge_min = merge_min.dropna()
    merge_min['month'] = pd.DatetimeIndex(merge_min['order_1_start_date']).month
    merge_min['year'] = pd.DatetimeIndex(merge_min['order_1_start_date']).year
    merge_min['bins'] = pd.cut(merge_min['order_1_month'],bins=[0,1,11,12], labels=["1 month"," 2-11 month","12+month"])

    merge_min = pd.merge(left=merge_min,right=data[['id','start_date','is_sales_person']], how='left', 
                         left_on=['order_1_id','order_1_start_date'], right_on=['id','start_date'])
    merge_min.loc[pd.isna(merge_min.is_sales_person), 'is_sales'] = 'not-sales'
    merge_min.loc[(merge_min.is_sales_person == in_sales), 'is_sales'] = 'incoming_sales'
    merge_min.loc[(merge_min.is_sales_person != in_sales) & (~pd.isna(merge_min.is_sales_person)), 'is_sales'] = 'sales'
    return(merge_min)
    
    
def load_to_bigquery(merge_min,google_bigquery_credentials, table, dataset, bqc):
    """Загрузка данных в Google Big Query"""
    bqc = bigquery.Client()
    load_product_split = functions_gs.load_table_bigquery(
    df = merge_min.astype(str),
    table = table,
    dataset = dataset,
    google_bigquery_credentials = google_bigquery_credentials,
    bqc = bqc,  )

def week_preprocessing(merge_min, data, in_sales = 28864):
    """Разбивка месячного фрейма на недели и препроцессинг"""
    week_data = merge_min[['client_id','company','order_1_id','order_1_start_date','first_order_mrr','order_1_month']]
    week_data = pd.merge(left=week_data,right=data[['id','start_date','is_sales_person']], how='left', 
                         left_on=['order_1_id','order_1_start_date'], right_on=['id','start_date'])
    week_data['week'] = pd.DatetimeIndex(week_data['order_1_start_date']).week
    week_data['year'] = pd.DatetimeIndex(week_data['order_1_start_date']).year
    week_data.loc[pd.isna(week_data.is_sales_person), 'is_sales'] = 'not-sales'
    week_data.loc[(week_data.is_sales_person == in_sales), 'is_sales'] = 'incoming_sales'
    week_data.loc[(week_data.is_sales_person != in_sales) & (~pd.isna(week_data.is_sales_person)), 'is_sales'] = 'sales'
    week_data['bins'] = pd.cut(week_data['order_1_month'],bins=[0,1,11,12], labels=["1 месяц","меньше года","год и больше"])
    weeks = []
    for i in range(0,len(week_data)):
        w,f,g = get_start_end_dates(2018,week_data['week'][i].item())
        weeks.append('week_0'+ str(w) + f.strftime("_%d%m")+g.strftime("_%d%m"))
    week_data['weeks'] = weeks
    return (week_data)

