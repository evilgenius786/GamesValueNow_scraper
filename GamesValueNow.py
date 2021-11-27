import os
import platform

import pymysql
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import config as config

gamesvalue = "https://gamevaluenow.com"

# file = "GamesValueNow.csv"
content = BeautifulSoup(requests.get(gamesvalue).text, 'lxml')

CHROMEDRIVER_PATH = os.path.join(
    os.path.dirname(__file__),
    f"../chromedriver/{platform.system().lower()}/chromedriver"
)


class DBHandler:
    DB_CONN = None

    def __init__(self):

        self.DB_HOST = config.DB_CONFIG["host"]
        self.DB_USER = config.DB_CONFIG["user"]
        self.DB_PW = config.DB_CONFIG["pw"]
        self.DB_NAME = config.DB_CONFIG["name"]

    def openConnection(self):
        self.DB_CONN = pymysql.connect(self.DB_HOST, self.DB_USER, self.DB_PW, self.DB_NAME, autocommit=True)

    def closeConnection(self):
        self.DB_CONN.close()

    def executeSQL(self, sql, args=None):
        try:
            with self.DB_CONN.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(sql, args)
                queryResult = cursor
        except:
            print("Error: ")
            print(sql, args)
        return queryResult

    def check_if_game_exists(self, title, games):
        for game in games:
            if game['title'] == title:
                print(title)
                return True
        return False

    def insert_new_game(self, items):
        self.openConnection()
        games = self.get_all_data()
        for item in items:
            sql = "INSERT INTO ConsolePrice(console, datetime, title, loose, complete, new) VALUES(%s, %s, %s, " \
                  "%s, %s, %s);"
            title = pymysql.escape_string(item['Title'])
            if not self.check_if_game_exists(title, games):
                self.executeSQL(sql, (
                    item['Console'], item['DateTime'], title, item['Loose'], item['Complete'], item['New'],))
                print(title + " inserted!")
            else:
                print(title + ' already in db!')
        self.closeConnection()
        return

    def createDB(self):
        self.openConnection()
        sql = """CREATE TABLE ConsolePrice(
                id INTEGER NOT NULL AUTO_INCREMENT,
                console TEXT DEFAULT NULL,
                datetime DATETIME DEFAULT NULL,
                title TEXT DEFAULT NULL,
                loose TEXT DEFAULT NULL,
                complete TEXT DEFAULT NULL,
                new TEXT DEFAULT NULL,
                PRIMARY KEY(id)
                );
                """
        self.executeSQL(sql)
        self.closeConnection()
        return

    def get_all_data(self):
        sql = "SELECT * FROM ConsolePrice"
        return self.executeSQL(sql).fetchall()


# def write(items):
#     with open(file, 'a', newline='') as f:
#         writer = csv.writer(f)
#         for item in items:
#             writer.writerow(item.values())


def get(url, console):
    data = BeautifulSoup(requests.get(gamesvalue + url).text, 'lxml')
    body = data.find('table')
    items = []
    for tr in body.find_all('tr')[1:]:
        td = tr.find_all('td')
        items.append({
            "Console": console,
            "DateTime": datetime.now(),
            "Title": td[0].text,
            "Loose": td[1].text,
            "Complete": td[2].text,
            "New": td[3].text,
        })
    handler = DBHandler()
    handler.insert_new_game(items)
    # write(items) // write to CSV


# with open(file, 'w') as f:
#     f.write("Console, Date, Title, Loose, Complete, New \n")

for x in content.find_all('li', {'class': 'list-group-item under'}):
    print("Working on " + x.find('a').text + " " + gamesvalue + x.find('a')['href'])
    get(x.find('a')['href'], x.find('a').text)
