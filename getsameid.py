import mysql.connector
from mysql.connector import Error
import pandas as pd

#display dataframe
pd.set_option('display.height',1000)
pd.set_option('display.max_rows',500)
pd.set_option('display.max_columns',500)
pd.set_option('display.width',1000)

#global dataframe
df_origindata = pd.DataFrame()
df_aggregated = pd.DataFrame()

### get data
def connectdatabase():
    try:
        conn = mysql.connector.connect(user='jul077', password='abgEFIJXl_%Q17',
                                       host='crepe.usask.ca',
                                       database='SHED10')
        if conn.is_connected():
            print("PPP:Successfully connected to MYSQL database")
            cursor = conn.cursor()

            # get_gpsdata_oneoffilter50_saskatoon(cursor)
            get_data(cursor)

            cursor.close()
    except Error as e:
        print(e)
    finally:
        conn.close()
        print("PPP:Connection closed")

sql_gpsdata_filter50_saskatoon = 'select T2.user_id, T2.lat, T2.lon, T2.provider, T2.accu, T2.record_time, T2.date from (select T1.user_id, gps.lat, gps.lon, gps.provider, gps.accu, gps.record_time, gps.date from (select user_id, count_batteryrecord from (select user_id, count(*) as count_batteryrecord from battery group by user_id) as T where count_batteryrecord > 4176) as T1 left join gps on T1.user_id = gps.user_id) as T2 where T2.lat between 52.058367 and 52.214608 and T2.lon between -106.7649138128 and -106.52225318 and T2.accu<100'
def get_data(cursor):
    cursor.execute(sql_gpsdata_filter50_saskatoon)
    global df_origindata
    # df_origindata = df_origindata.append(cursor.fetchmany(127945))
    df_origindata = df_origindata.append(cursor.fetchmany(12))
    df_origindata.columns = ['user_id', 'lat', 'lon', 'provider', 'accu', 'record_time', 'date']

### get same id record in one dataframe
def contain_sameid():
    df_singleid = pd.DataFrame()
    global df_origindata
    id_current = df_origindata['user_id'][0]
    df_singleid = df_singleid.append(df_origindata[0: 1])

    i = 1

    while i < len(df_origindata):
        record = df_origindata[i: i+1]

        #判断是否同一id
        if record['user_id'][i] == id_current:
            df_singleid = df_singleid.append(record)
        else:
            pass
            aggregate_data(df_singleid)
            id_current = record['user_id'][i]
            df_singleid = df_singleid.drop(df_singleid.index, inplace=True)
            df_singleid = df_singleid.append(record)
        i = i + 1


###
#function: aggregate gps data
def aggregate_data(df_sameid):
    pass

#function: aggregate gps data within same ducy circle
def aggregate_singledc(df_singledc):
    pass


### main function
if __name__ == '__main__':
    connectdatabase()
    # print(df_origindata)
    # print("..............................")
    contain_sameid()
    # df_origindata = df_origindata.sort_values(by=['user_id', 'record_time'])
