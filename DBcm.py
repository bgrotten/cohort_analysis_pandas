import pymysql as pm

class UseDatabase:

    # Initiert den DB-Aufruf und forder Zugangsdaten for config
    def __init__(self, config: dict) -> None:
        self.configuration = config

    # connecting to DB with given credentials and prepare a cursor object using cursor() method
    def __enter__(self) -> 'cursor':
        self.conn = pm.connect(**self.configuration)
        self.cursor = self.conn.cursor()
        return self.cursor

    # committing and closing connection
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()
