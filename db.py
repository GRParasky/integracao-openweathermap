import pymysql


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