from pgsql.postgresql import Database
from datetime import datetime, timedelta
from operation.orders import get_order
from operation.changes import get_changes
from writter.autorization import auto
import time


def start():
    db = Database()
    dt = datetime.now().date() - timedelta(days=1)
    for i in db.get_users(group=0):
        get_order(i[0], i[2], i[3], i[4], dt)
        df = get_changes(i[0])
        auto(i[1], df, dt)
        time.sleep(15)
        print(i[1])


if __name__ == '__main__':
    start()
