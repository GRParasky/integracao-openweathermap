import requests, pymysql

api_key = "c06cdc08dbe36d48d7af5f2d2c9f5c29"
latitude = -25.4278
longitude = -49.2731

# raw_data = requests.get(f'https://api.openweathermap.org/data/2.5/forecast?lat={latitude}&lon={longitude}&appid={api_key}')

raw_data = requests.get(f'https://api.openweathermap.org/data/2.5/weather?lat={latitude}&lon={longitude}&appid={api_key}')

data = raw_data.json()

# print(type(data["city"]["id"]))
# print(type(data["city"]["coord"]["lat"]))
# print(type(data["city"]["coord"]["lon"]))

def create_connection_to_database(hst: str, prt: int, usr: str, pwd: str, db: str) -> pymysql.connect | None:
    """
    Método responsável por criar uma conexão utilizando método .connect do pymysql.
    
    Args:
        hst (str): Host do Banco de Dados
        prt (int): Porta do Banco de Dados
        usr (str): Usuário do Banco de Dados
        pwd (str): Senha do Banco de Dados
        db (str): Schema do Banco de Dados

    Returns:
        pymysql.connect: Retorna um objeto de conexão do pymysql
        None: Retorna None em caso de erro na criação da conexão
    """
    try:
        connection = pymysql.connect(
            host=hst,
            port=prt,
            user=usr,
            password=pwd,
            database=db,
            cursorclass=pymysql.cursors.DictCursor
        )
        print("Conexão estabelecida com sucesso!")
        return connection
    except Exception as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
        return None


def execute_insert(connection: pymysql.connect, query: str, parameters: tuple = None) -> None:
    """
    Método responsável por executar comandos de INSERT no Banco de Dados.

    connection (pymysql.connect): Objeto de conexão do Banco de Dados.
    query (str): Query a ser executada como insert.
    parameters (tuple): Lista de valores a serem inseridos.
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, parameters)
            connection.commit()
            print(f"Registro inserido com sucesso!")
            # return cursor.lastrowid  # Retorna o ID do último registro inserido
    except Exception as e:
        print(f"Erro ao realizar a inserção: {e}")
    

conn = create_connection_to_database("localhost", 3306, "root", "q439;he'4LUZ'D<L&41>yTK&du<j", "integracao_openweathermap")

insert_city = "INSERT INTO Cities(id, city, latitude, longitude) VALUES(%s, %s, %s, %s)"
info_city = (data["id"], data["name"], data["coord"]["lat"], data["coord"]["lon"])

execute_insert(conn, insert_city, info_city)

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

# weather_data = data["list"]

# for row in weather_data:
#     info_weather = ({row["dt"]}, data["city"]["id"], row["main"]["temp"], row["main"]["feels_like"], 
#                      row["main"]["temp_min"], row["main"]["temp_max"], row["main"]["pressure"], row["main"]["sea_level"],
#                      row["main"]["grnd_level"], row["main"]["humidity"], row["main"]["temp_kf"])
#     execute_insert(conn, insert_weather, info_weather)

info_weather = (data["dt"], data["id"], data["main"]["temp"], data["main"]["feels_like"], 
                 data["main"]["temp_min"], data["main"]["temp_max"], data["main"]["pressure"], data["main"]["sea_level"],
                 data["main"]["grnd_level"], data["main"]["humidity"])

execute_insert(conn, insert_weather, info_weather)


conn.close()