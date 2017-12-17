"""
1) Получить список репозиториев по заданому хеш-тегу в заданный период времени
2) получить список популярных репозиториев за неделю
3) получить топ-10 популярных хештегов, у кого больше репозиториев
4) получить список самых активных пользователей, у кого больше всего репозиториев

NOTE:
    Вводите через консоль тэги-языки программирования в формате:
    --tag Puthon
    даты:
    --date1 2017-01-01
    --date2 2017-12-17

"""
import sys
import argparse
import psycopg2
from datetime import datetime

dbname = "dbname='project_8'"
user = "user='Nadya'"
host = "host='localhost'"
password = "password=''"

connection = dbname+user+host+password
b_con = psycopg2.connect(connection)


def create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--select_num")
    parser.add_argument("--tag")
    parser.add_argument("--date1")
    parser.add_argument("--date2")
    return parser


# Выборка по хештегам
def select1(input_language, input_date2, input_date1):
    with b_con.cursor() as b_cur:
        input_language = input_language.capitalize()
        try:
            datetime.strptime(input_date2, "%Y-%m-%d")
        except ValueError:
            print("Попробуйте ввести дату снова.")

        try:
             datetime.strptime(input_date1, "%Y-%m-%d")
        except ValueError:
            print("Попробуйте ввести дату снова.")

        qur_select1 = "SELECT repos.name from repos, tags WHERE tags.language = '{}'" \
                          " and repos.id_r = tags.id_r and repos.time <= '{}' and repos.time >= '{}';".format(input_language,
                                                                                                              input_date1, input_date2)

        try:
            b_cur.execute(qur_select1)
        except psycopg2.Error as exception:
            print(exception)
        select1_list = b_cur.fetchall()
        print("Список репозиториев по языку программироания и интервалу времени:")
        for result in select1_list:
            print(str(result)[2:-3])
        b_con.commit()


# Выборка самых популярных репозиториев за неделю
def select2():
    with b_con.cursor() as b_cur:
        qur_select2 = "SELECT repos.name from repos WHERE repos.time <= current_date and" \
                          " repos.time >= current_date - interval '7 days' order by repos.stars desc limit 20;"
        try:
            b_cur.execute(qur_select2)
        except psycopg2.Error as exception:
            print(exception)
        select1_list = b_cur.fetchall()
        print("\n20 самых популярных репозиториев за неделю:")
        for result in select1_list:
            print(str(result)[2:-3])
        b_con.commit()


# Выборка 10 самых популярных хештегов
def select3():
    with b_con.cursor() as b_cur:
        qur_select3 = "SELECT tags.language FROM tags WHERE tags.language <> 'None' GROUP BY tags.language" \
                          " ORDER BY COUNT(tags.id_r) DESC limit 10;"
        try:
            b_cur.execute(qur_select3)
        except psycopg2.Error as exception:
            print(exception)
        select1_list = b_cur.fetchall()
        print("\nТоп-10 популярных хештегов:")
        for result in select1_list:
            print(str(result)[2:-3])
        b_con.commit()

#Выборка самых активных пользователей
def select4():
    with b_con.cursor() as b_cur:
        qur_select4 = "SELECT users.login FROM users ORDER BY users.repos_number DESC limit 20;"
        try:
            b_cur.execute(qur_select4)
        except psycopg2.Error as exception:
            print(exception)
        select1_list = b_cur.fetchall()
        print("\nСписок 20 самых активных пользователей:")
        for result in select1_list:
            print(str(result)[2:-3])
        b_con.commit()


def main():
    if namespace.select_num == "1":
        with b_con:
            select1(namespace.tag, namespace.date1, namespace.date2)
        b_con.close()
    elif namespace.select_num == "2":
        with b_con:
            select2()
        b_con.close()
    elif namespace.select_num == "3":
        with b_con:
            select3()
        b_con.close()
    elif namespace.select_num == "4":
        with b_con:
            select4()
        b_con.close()
    elif namespace.select_num == "5":
        with b_con:
            select1(namespace.tag, namespace.date1, namespace.date2)
            select2()
            select3()
            select4()
        b_con.close()

if __name__ == "__main__":
    parser = create_parser()
    namespace = parser.parse_args()
    main()