import snowflake.connector

class SnowflakeConnector:
    # Caminho do arquivo de configuração estático
    CONFIG_PATH = r'C:\Users\gpira\Códigos Python\dados_conexao_snowflake.txt'

    def __init__(self):
        self.usuario = None
        self.senha = None
        self.conta = None
        self.conn = None
        self._load_credentials()

    def _load_credentials(self):
        with open(self.CONFIG_PATH, 'r') as arquivo:
            conteudo = arquivo.readlines()
            lista = [x.split(':') for x in conteudo]
            self.usuario = lista[0][1].strip(' ').rstrip()
            self.senha = lista[1][1].strip(' ').rstrip()
            self.conta = lista[2][1].strip(' ').rstrip()

    def connect(self):
        self.conn = snowflake.connector.connect(
            user=self.usuario,
            password=self.senha,
            account=self.conta
        )
        return self.conn

    def close_connection(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def execute_query(self, query):
        if self.conn is None:
            raise ConnectionError("You must connect first!")
        cursor = self.conn.cursor()
        try:
            cursor.execute(query)
            return cursor.fetchall()
        finally:
            cursor.close()

if __name__ == "__main__":
    snowflake_conn = SnowflakeConnector()