
import air_class
import time
import os
import pymysql
import pymysql.cursors


env_dist = os.environ
HOST = env_dist.get('HOST')
PORT = env_dist.get('PORT')
USER = env_dist.get('USER')
PASSWORD = env_dist.get('PASSWORD')
DATABASE = env_dist.get('DATABASE')

def batch_insert(connection, sql, data, context):

    with connection.cursor() as cursor:
        try:
            cursor.executemany(sql, data)
            connection.commit()
            context.getLogger().info(f"{sql} 插入成功")

        except Exception as e:
            connection.rollback()
            connection.commit()
            context.getLogger().info(f"{sql} 插入数据时发生异常,详情如下： {e}")

def select(connection, sql, context):

    with connection.cursor() as cursor:
        
        try:
            cursor.execute(sql)
            result = cursor.fetchall()
            context.getLogger().info(f"{sql} 查询成功")

        except Exception as e:
            context.getLogger().info(f"{sql} 查询数据时发生异常,详情如下： {e}")

    return result

def air_crawler(event, context):
    
    air = air_class.airChina()
    # Get data of all air matters from each station in China
    max_retry = 0
    while max_retry < 3:
        try:
            
            allData = air.getAllStationsData()
            break
        except Exception as e:
            # todo 进行记录
            context.getLogger().info(f"请求API接口发生异常,详情如下： {e}")
            time.sleep(3)
            max_retry += 1

    # Connect to the database
    try:
        connection = pymysql.connect(host = HOST,
                                    user = USER,
                                    password = PASSWORD,
                                    database = DATABASE,
                                    port = PORT,
                                    cursorclass=pymysql.cursors.DictCursor)
        context.getLogger().info(f"{HOST} 数据库连接成功")
    except Exception as e:
        context.getLogger().info(f"{HOST} 数据库连接失败,详情如下： {e}")

    # 查询站点表中的所有站点
    sql = "SELECT `station_code` FROM `station`"
    result = select(connection, sql, context)
    staion_list = []
    if len(result) != 0:
        for station in result:
            staion_list.append(station['station_code'])

    # 构建要插入的数据
    data_station = []
    data_air = []

    try:
        for item in allData:
            data_air.append((item['StationCode'], item['AQI'], item['CO'], item['NO2'], item['O3'], item['PM10'], item['PM2_5'], item['SO2'], item['TimePoint'].replace('T', ' '), item['CO_24h'], item['NO2_24h'], item['O3_24h'], item['O3_8h'], item['O3_8h_24h'], item['PM10_24h'], item['PM2_5_24h'], item['SO2_24h']))
            if item['StationCode'] not in staion_list:
                data_station.append((item['StationCode'], item['PositionName'], item['Area'], item['Latitude'], item['Longitude'], '国'))
    except Exception as e:
        context.getLogger().info(f"构建插入数据错误,详情如下： {e}")

    # 插入air_quality
    if len(data_air) != 0:
        sql = "INSERT IGNORE INTO `air_quality` (`station_code`, `aqi`, `co`, `no2`, `o3`, `pm10`, `pm2_5`, `so2`, `time_point`, `co_24h`, `no2_24h`, `o3_24h`, `o3_8h`, `o3_8h_24h`, `pm10_24h`, `pm2_5_24h`, `so2_24h`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        batch_insert(connection, sql, data_air, context)
    
    # 插入station
    if len(data_station) != 0:
        sql = "INSERT INTO `station` (`station_code`, `position_name`, `area`, `latitude`, `longitude`, `position_type`) VALUES (%s, %s, %s, %s, %s, %s)"
        batch_insert(connection, sql, data_station, context)
        
    connection.close()

    
if __name__ == '__main__' :
    pass