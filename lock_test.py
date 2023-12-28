import string
import random

from pymysql.cursors import Cursor, DictCursor
from pymysql.connections import Connection


class DbConnection:
    def __init__(self, host: str, 
                 user: str, 
                 password: str, 
                 port: int, 
                 autocommit: bool = True,
                 database: str = None) -> None:
        self._connection = Connection(
            host=host,
            user=user,
            password=password,
            port=port,
            autocommit=autocommit,
            database=database,
            cursorclass=DictCursor
        )
        self._cursor: Cursor = self._connection.cursor()
        
    def close(self) -> None:
        try:
            self._cursor.close()
            self._connection.close()
        except Exception as e:
            raise e

    def set_database(self, database: str) -> None:
        self._connection.select_db(database)
    
    def commit(self) -> None:
        self._connection.commit()
        
    def rollback(self) -> None:
        self._connection.rollback()
        
    def execute(self, sql: str) -> int:
        self._cursor.execute(sql)

    def fetchone(self) -> dict:
        return self._cursor.fetchone()


def random_string_generator(length: int) -> str:
    allowed_chars = string.ascii_letters + string.digits
    return "".join(random.choice(allowed_chars) for _ in range(length))


def random_int(start: int, end: int) -> int:
    return random.randint(start, end)


def lock_test() -> None:
    sql_create_test_table = "CREATE TABLE IF NOT EXISTS lock_test (id INT NOT NULL PRIMARY KEY AUTO_INCREMENT, a VARCHAR(20), b INT)"
    sql_insert_data = f"INSERT INTO lock_test (a,b) VALUES('{random_string_generator(random_int(1, 20))}', {random_int(100, 99999999)})"
    sql_update_creating_lock = f"UPDATE lock_test SET a='{random_string_generator(20)}', b={random_int(100, 99999999)} WHERE id=13"
    sql_session_lock = "SET @@SESSION.innodb_lock_wait_timeout = 10"
    sql_check_lock = "SELECT @@SESSION.innodb_lock_wait_timeout"
    sql_drop_test_table = "DROP TABLE lock_test"
    
    lock_conn = DbConnection(
        host="127.0.0.1",
        user="root",
        password="",
        database="lock_test",
        port=3308,
        autocommit=False
    )
    
    lock_conn.execute(sql_create_test_table)
    lock_conn.commit()
    
    for _ in range(50):
        lock_conn.execute(sql_insert_data)
    lock_conn.commit()
    
    
    timeout_conn = DbConnection(
        host="127.0.0.1",
        user="root",
        password="",
        database="lock_test",
        port=3308,
        autocommit=True
    )
    
    try:
        lock_conn.execute(sql_update_creating_lock)
        timeout_conn.execute(sql_session_lock)
        timeout_conn.execute(sql_check_lock)
        print(timeout_conn.fetchone())
        timeout_conn.execute(sql_update_creating_lock)
    except Exception as e:
        print(e)
    finally:
        lock_conn.rollback()
        lock_conn.execute(sql_drop_test_table)
        lock_conn.commit()


if __name__ == "__main__":
    lock_test()
