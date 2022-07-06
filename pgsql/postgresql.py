import psycopg2
from configuration.config import Config
from psycopg2 import errors
from psycopg2.errorcodes import INVALID_DATETIME_FORMAT


class Database:
    @property
    def connection(self):
        cfg = Config()
        return psycopg2.connect(
            database=cfg.dbase,
            user=cfg.user,
            password=cfg.password,
            host=cfg.host,
            port='5432'
        )

    def execute(self, sql: str, parameters: tuple = None, fetchone=False, fetchall=False, commit=False):
        if not parameters:
            parameters = tuple()
        connection = self.connection
        cursor = connection.cursor()
        data = None
        cursor.execute(sql, parameters)
        if commit:
            connection.commit()
        if fetchone:
            data = cursor.fetchone()
        if fetchall:
            data = cursor.fetchall()
        connection.close()
        return data

    def get_users(self, group: int):
        sql = '''
            SELECT rest_id, name_rest, long_id, login, password 
            FROM settingsrest 
            WHERE group_index=%s and rest_id=1361
            ORDER BY rest_id
        '''
        parameters = (group,)
        return self.execute(sql, parameters=parameters, fetchall=True)
