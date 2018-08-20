#!/usr/bin/env python3
# Sina Serati
# Aug 18th 2018
# About:
#       "Database code" for news database.
#       this code preforms 3 query in 3 fuctions.
#       Query 1:
#               What are the most popular 3 articles
#       Query 2:
#               Who are the most popular three articles of all time
#       Query 3:
#               On which days did more then 1% of requests lead to erros?
# Running code:
#   just use python news.py in terminal to run.


import psycopg2
DBNAME = "news"


def query_1():
    # what are the most popular 3 articles
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute("""SELECT title, COUNT(title) AS views
    FROM articles, log where log.path = CONCAT('/article/' , articles.slug)
    GROUP BY title ORDER BY views DESC LIMIT 3;""")
    return c.fetchall()
    db.close()


def query_2():
    # who are the most popular three articles of all time
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute("""SELECT name, COUNT(path) AS views FROM authors,
    log, articles WHERE log.path = CONCAT('/article/',articles.slug)
    and articles.author = authors.id GROUP BY name ORDER BY views DESC;""")
    return c.fetchall()
    db.close()


def query_3():
    # on which days did more then 1% of requests lead to errors?
    query = """WITH calculate_request_count AS
    ( SELECT time::date AS day, count(*)
    AS tot FROM log GROUP BY day ORDER BY day ),
    calculate_error_count AS
    ( SELECT time::date AS day, count(*)
    AS tot FROM log WHERE status != '200 OK'
    GROUP BY day ORDER BY day ),
    final_prec AS
    ( SELECT calculate_request_count.day as the_day,
    calculate_error_count.tot / calculate_request_count.tot::float * 100.00
    AS precent
    FROM calculate_request_count,
    calculate_error_count
    WHERE calculate_request_count.day = calculate_error_count.day )
    SELECT the_day, precent
    FROM final_prec WHERE precent > 1;"""
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(query)
    return c.fetchall()
    db.close()


if __name__ == '__main__':
    """ main function of the code """
    print("What are the most popular three articles of all time?")
    for title, views in query_1():
        print(" \"" + title + "\" -- " + str(views) + " views")

    print("Who are the most popular article authors of all time?")
    for name, views in query_2():
        print(name + " -- " + str(views))

    print("On which days did more than 1% of requests lead to errors?")
    for day, prec in query_3():
        print(str(day) + " -- " + str(round(prec, 2)))
