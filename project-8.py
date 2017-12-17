"""
1) Получить список репозиториев по заданому хеш-тегу в заданный период времени
2) получить список популярных репозиториев за неделю
3) получить топ-10 популярных хештегов, у кого больше репозиториев
4) получить список самых активных пользователей, у кого больше всего репозиториев
"""

import psycopg2
import urllib.request
import json

dbname = "dbname='project_8'"
user = "user='Nadya'"
host = "host='localhost'"
password = "password=''"

connection = dbname+user+host+password
b_con = psycopg2.connect(connection)

#Создаём таблицы базы данных
with b_con:
    with b_con.cursor() as b_cur:
        try:
            create_users = """CREATE TABLE users (id_u serial PRIMARY KEY, login TEXT,
                             repos_number INTEGER);"""
            b_cur.execute(create_users)
            b_con.commit()
        except psycopg2.ProgrammingError:
            pass

with b_con:
    with b_con.cursor() as b_cur:
        try:
            create_repos = """CREATE TABLE repos (id_r serial PRIMARY KEY, name TEXT,
                time DATE, id_u INTEGER, FOREIGN KEY (id_u) REFERENCES users (id_u),
                stars INTEGER); """
            b_cur.execute(create_repos)
            b_con.commit()
        except psycopg2.ProgrammingError:
            pass

with b_con:
    with b_con.cursor() as b_cur:
        try:
            create_tags = """CREATE TABLE tags (id_t serial PRIMARY KEY, language TEXT,
                             id_r INTEGER, FOREIGN KEY (id_r) REFERENCES repos (id_r));"""
            b_cur.execute(create_tags)
            b_con.commit()
        except psycopg2.ProgrammingError:
            pass

#Получаем данные и записываем их в таблицы
try:
    url = "https://api.github.com/users?page=2&per_page=70"
    response1 = urllib.request.urlopen(url)
    obj = json.loads(response1.read().decode())
except urllib.error.URLError:
    print("Ошибка подключения")
    try:
        url = "https://api.github.com/users?access_token=0f995f8b11d34f21842e26c95cdccd692379b799&page=2&per_page=70"
        response1 = urllib.request.urlopen(url)
        obj = json.loads(response1.read().decode())
    except urllib.error.URLError:
        print("Ошибка подключения")
repos = []

with b_con:
    with b_con.cursor() as b_cur:
        for item in obj:
            repos.append(item['repos_url'])
            #добавить логины в табл юзеров
            qur_insert = "INSERT INTO users (login) VALUES ('{}');".format(item['login'])
            b_cur.execute(qur_insert)
            b_con.commit()

    id_u = 1
    id_r = 1
    tags = {}
    for repo in repos:
        try:
            response = urllib.request.urlopen(repo+"?access_token=0f995f8b11d34f21842e26c95cdccd692379b799")
            tmp_repos = json.loads(response.read().decode())
        except urllib.error.URLError:
            print("Ошибка")
        repo_count = 0
        with b_con.cursor() as b_cur:
            for tmp_repo in tmp_repos:
                time = tmp_repo['created_at'].split(" ")
                # добавить поля в таблицу репо
                qur_insert = "INSERT INTO repos (name, time, id_u, stars) VALUES ('{}','{}', '{}', '{}');".format(tmp_repo['name'],
                                                                                                              time[0],
                                                                                                              id_u,
                                                                                                              tmp_repo['stargazers_count'])
                b_cur.execute(qur_insert)
                b_con.commit()

                # добавить в таблицу  репо
                qur_insert = "INSERT INTO tags (language, id_r) VALUES ('{}','{}');".format(tmp_repo['language'],id_r)
                b_cur.execute(qur_insert)
                b_con.commit()

                repo_count += 1
                id_r += 1
        # записать число репозиториев в таблицу юзеров
        with b_con.cursor() as b_cur:
            qur_insert = "UPDATE users SET repos_number = {} WHERE id_u = {};".format(repo_count, id_u)
            b_cur.execute(qur_insert)
            b_con.commit()
        id_u += 1

b_con.close()
