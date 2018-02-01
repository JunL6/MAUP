import mysql.connector
from mysql.connector import Error
import pandas as pd
import geoplotlib


''''
step 1
'''
#global list
list_battery_data_50 = []
list_battery_data_70 = []

#global dataframe to store gpsdata, for plotting
df_gpsdata_all = pd.DataFrame()
df_gpsdata_filter50_saskatoon = pd.DataFrame()
df_gpsdata_oneoffilter50_saskatoon = pd.DataFrame()

#function: get user_id where number of record > 50%
sql_filter50 = "select user_id as user_id_50, count_batteryrecord from (select user_id, count(*) as count_batteryrecord from battery group by user_id) as T where count_batteryrecord > 4176"
def getuserid50(cursor):
    cursor.execute(sql_filter50)
    row = cursor.fetchone()
    while row is not None:
        list_battery_data_50.append(row)
        print(row)
        row = cursor.fetchone()
    print("number of 50% id: ", len(list_battery_data_50))

#function: get user_id where number of record > 70%
sql_filter70 = "select user_id as user_id_70, count_batteryrecord from (select user_id, count(*) as count_batteryrecord from battery group by user_id) as T where count_batteryrecord > 5846"
def getuserid70(cursor):
    cursor.execute(sql_filter70)
    row = cursor.fetchone()

    while row is not None:
        list_battery_data_70.append(row)
        print(row)
        row = cursor.fetchone()

    print("number of 70% id: ", len(list_battery_data_70))
    df_batterydata70 = pd.DataFrame(list_battery_data_70, columns=['user_id_70', 'count'])
    print(df_batterydata70)

#function: get accurate gps data of the 50% battery participants inside Saskatoon
sql_gpsdata_filter50 = 'select T1.user_id, gps.lat, gps.lon, gps.provider from (select user_id, count_batteryrecord from (select user_id, count(*) as count_batteryrecord from battery group by user_id) as T where count_batteryrecord > 4176) as T1 left join gps on T1.user_id = gps.user_id'
sql_gpsdata_filter50_saskatoon = 'select T2.user_id, T2.lat, T2.lon, T2.provider, T2.accu, T2.record_time, T2.date from (select T1.user_id, gps.lat, gps.lon, gps.provider, gps.accu, gps.record_time, gps.date from (select user_id, count_batteryrecord from (select user_id, count(*) as count_batteryrecord from battery group by user_id) as T where count_batteryrecord > 4176) as T1 left join gps on T1.user_id = gps.user_id) as T2 where T2.lat between 52.058367 and 52.214608 and T2.lon between -106.7649138128 and -106.52225318 and T2.accu<100'
def get_gpsdata_filter50_saskatoon(cursor):
    cursor.execute(sql_gpsdata_filter50_saskatoon)
    global df_gpsdata_filter50_saskatoon
    # df_gpsdata_filter50_saskatoon = df_gpsdata_filter50_saskatoon.append(cursor.fetchall())
    df_gpsdata_filter50_saskatoon = df_gpsdata_filter50_saskatoon.append(cursor.fetchmany(9999))

    df_gpsdata_filter50_saskatoon.columns = ['user_id', 'lat', 'lon', 'provider', 'accu', 'record_time', 'date']

#function: get accurate gps data of the one of 50% battery participants inside Saskatoon
def get_gpsdata_oneoffilter50_saskatoon(cursor):
    cursor.execute(sql_gpsdata_filter50_saskatoon)
    global df_gpsdata_oneoffilter50_saskatoon
    # df_gpsdata_filter50_saskatoon = df_gpsdata_filter50_saskatoon.append(cursor.fetchall())
    df_gpsdata_oneoffilter50_saskatoon = df_gpsdata_oneoffilter50_saskatoon.append(cursor.fetchmany(999))
    df_gpsdata_oneoffilter50_saskatoon.columns = ['user_id', 'lat', 'lon', 'provider', 'accu', 'record_time', 'date']

#function: get gps data from one participant
sql_gpsdata_one = 'SELECT user_id, record_time, date, lat, lon, provider, accu FROM gps where user_id=777;'
def get_gpsdata_one(cursor):
    cursor.execute(sql_gpsdata_one)
    df_gpsdata_one = pd.DataFrame(cursor.fetchall(), columns=['user_id', 'record_time', 'date', 'lat', 'lon', 'provider', 'accu'])
    print(df_gpsdata_one)
    geoplotlib.dot(df_gpsdata_one)
    geoplotlib.show()

#function: get all gps data from all participants
sql_gpsdata_all = 'SELECT user_id, record_time, date, lat, lon, provider, accu FROM gps;'
def get_gpsdata_all(cursor):
    cursor.execute(sql_gpsdata_all)
    global df_gpsdata_all

    df_gpsdata_all = df_gpsdata_all.append(cursor.fetchall())
    df_gpsdata_all.columns = ['user_id', 'record_time', 'date', 'lat', 'lon', 'provider', 'accu']

#function: plot gps dots
def plot_gps(dataframe):
    geoplotlib.dot(dataframe)
    geoplotlib.show()

#Connect to database
def connectdatabase():
    try:
        conn = mysql.connector.connect(user='jul077', password='abgEFIJXl_%Q17',
                                       host='crepe.usask.ca',
                                       database='SHED10')
        if conn.is_connected():
            print("PPP:Successfully connected to MYSQL database")
            cursor = conn.cursor()

            #get data from Battery table
            #gettable(tableName = "battery", cursor = cursor)

            #get user_id where number of record > 50%
            get_gpsdata_oneoffilter50_saskatoon(cursor)

            cursor.close()
    except Error as e:
        print(e)
    finally:
        conn.close()
        print("PPP:Connection closed")


if __name__ == '__main__':
    connectdatabase()
    print(df_gpsdata_oneoffilter50_saskatoon['record_time'][1])
    print(df_gpsdata_oneoffilter50_saskatoon['record_time'][9])
    # plot_gps(df_gpsdata_oneoffilter50_saskatoon)
