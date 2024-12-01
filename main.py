import requests, pymysql
from db import create_connection_to_database, execute_insert

api_key = "c06cdc08dbe36d48d7af5f2d2c9f5c29"
latitudes = [-23.5489, -22.9028, -25.4278]
longitudes = [-46.6388, -43.2078, -49.2731]

conn = create_connection_to_database("localhost", 3306, "root", "q439;he'4LUZ'D<L&41>yTK&du<j", "integracao_openweathermap")

insert_city = "INSERT INTO Cities(id, city, latitude, longitude) VALUES(%s, %s, %s, %s)"
insert_weather = '''INSERT INTO WeatherData(id, id_city, temperature, feels_like, temp_min, temp_max, pressure, sea_level, grnd_level, humidity) 
                           VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE temperature = VALUES(temperature),
                                            feels_like = VALUES(feels_like),
                                            temp_min = VALUES(temp_min),
                                            temp_max = VALUES(temp_max),
                                            pressure = VALUES(pressure),
                                            sea_level = VALUES(sea_level),
                                            grnd_level = VALUES(grnd_level),
                                            humidity = VALUES(humidity)
                    '''

for latitude, longitude in zip(latitudes, longitudes):
    raw_data = requests.get(f'https://api.openweathermap.org/data/2.5/weather?lat={latitude}&lon={longitude}&appid={api_key}')
    data = raw_data.json()

    info_city = (data["id"], data["name"], data["coord"]["lat"], data["coord"]["lon"])
    execute_insert(conn, insert_city, info_city)

    info_weather = (data["dt"], data["id"], data["main"]["temp"], data["main"]["feels_like"], 
                 data["main"]["temp_min"], data["main"]["temp_max"], data["main"]["pressure"], data["main"]["sea_level"],
                 data["main"]["grnd_level"], data["main"]["humidity"])
    execute_insert(conn, insert_weather, info_weather)


conn.close()