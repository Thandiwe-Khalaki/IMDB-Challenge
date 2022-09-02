import sqlite3 as sq
import pandas as pd
import re
import warnings
import argparse

warnings.filterwarnings("ignore")

parser = argparse.ArgumentParser(description="Database name")
parser.add_argument("--database", type=str)
parser.add_argument("--table", type=str)
parser.add_argument("--year", type=int)
parser.add_argument("--movie", type=str)
parser.add_argument(
    "--query", choices=("top_10_movies", "longest_movies", "find_movie")
)

args = parser.parse_args()


class Database:
    def __init__(self):
        self.database_name = args.database
        self.conn = sq.connect(args.database)
        self.c = self.conn.cursor()

    def create_table(self, table):
        try:
            self.c.execute(
                f"""CREATE TABLE IF NOT EXISTS {args.table} (Poster_Link TEXT,Series_Title TEXT, Released_Year TEXT, \
                           Certificate TEXT, Runtime INTEGER, Genre TEXT, IMDB_Rating REAL,Overview TEXT, Meta_Score INTEGER, \
                           Director TEXT,Star1 TEXT, Star2 TEXT, Star3 TEXT, Star4 TEXT, No_of_Votes INTEGER, Gross INTEGER, \
                            UNIQUE (Poster_Link, Series_Title, Released_Year, Certificate, Runtime, Genre,\
                                IMDB_Rating,Meta_Score, Director,Star1, Star2, Star3,Star4,No_of_Votes,Gross) ON CONFLICT IGNORE

                           )"""
            )
        except Exception as e:
            print(e)

    def load_csv(self):
        df = pd.read_csv("imdb_top_1000.csv")
        df["Runtime"] = df["Runtime"].str.replace("\D+", "")
        df.dropna(inplace=True)
        df.to_sql("movies", self.conn, if_exists="append", index=False)
        self.conn.commit()

    def top_10_movies(self):
        print(
            self.c.execute(
                """SELECT Series_Title, IMDB_Rating
            FROM movies
            ORDER BY IMDB_Rating DESC
            LIMIT 10 ;"""
            ).fetchall()
        )

    def top_10_actors(self):
        print(
            self.c.execute(
                """SELECT Star1, round(AVG(IMDB_Rating),2) as avg_rating
            FROM movies
            GROUP BY Star1
            ORDER BY avg_rating DESC
            LIMIT 10;"""
            ).fetchall()
        )

    def year(self):
        print(
            self.c.execute(
                f"""SELECT Series_Title, IMDB_Rating
            FROM movie
            WHERE Released_Year = {args.year}
            ORDER BY IMDB_Rating DESC"""
            ).fetchall()
        )

    def longest_movie(self, year):
        print(
            self.c.execute(
                f"""SELECT Series_Title, MAX(Runtime)/60 as hrs
            FROM movies
            WHERE Released_Year = {args.year} """
            ).fetchall()
        )

    def gross_year(self):
        print(
            self.c.execute(
                """SELECT Released_Year, AVG(max_cross)
            FROM (select Released_Year, max(Gross) as max_cross from movies group by Released_Year) """
            ).fetchall()
        )

    def find_movie(self):
        print(
            self.c.execute(
                f"""SELECT Series_Title, IMDB_Rating
            FROM movies
            WHERE Series_Title = {args.movie}
            ORDER BY IMDB_Rating"""
            ).fetchall()
        )


if __name__ == "__main__":
    imbd = Database()
    imbd.create_table()
    imbd.load_csv()
    imbd.top_ten_movies()
    imbd.top_ten_actors()
    imbd.year()
    imbd.longest_movie(1920)
    imbd.gross_year()
    imbd.find_movie()
