#!/usr/bin/env python3.9

from csv import QUOTE_ALL
import os
from zipfile import ZipFile
import requests
from io import TextIOWrapper, StringIO
import html
import pandas as pd
import sqlite3
from isbnlib import is_isbn10


class DataDownloader:
    """
    Class for data fetching, preprocessing and storing

    Attributes
    ----------
    url: str
        url of online data source
    datafile: str
        filename of local data source
    """

    def __init__(self, url="http://www2.informatik.uni-freiburg.de/~cziegler/BX/BX-CSV-Dump.zip", datafile="data/csv-dump.zip"):
        """
        Parameters
        ----------
        url: str
            url of online data source
        datafile: str
            filename of local data source
        """

        self.url = url
        self.datafile = datafile

    def download_data(self):
        """
        Downloads data files from url attribute on books and book ratings.
        """

        # create dir for data
        dir = os.path.split(self.datafile)[0]
        if not os.path.exists(dir):
            os.makedirs(dir)

        # download & save data to specified file
        with requests.get(self.url) as r:
            with open(self.datafile, "wb") as fp:
                fp.write(r.content)

    def fetch_data(self, zipfile, filename):
        """
        Fetch data from given file inside specified zipfile.

        Parameters
        ----------
        zipfile: str
            filename of zipfile with data
        filename: str
            filename of desired file inside zipfile

        Returns
        -------
            pd.DataFrame
                dataframe with data from specified file
        """

        with ZipFile(zipfile, "r") as zf:
            with zf.open(filename, "r") as input:
                # load & fix HTML escape characters
                text = StringIO(html.unescape(
                    TextIOWrapper(input, "cp1251").read()))
                # create DataFrame
                return pd.read_csv(text, encoding="cp1251",
                                   escapechar="\\", quotechar="\"", sep=";")

    def fix_columns(self, df, con, table):
        """
        Rename columns of dataframe in accordance with provided table

        Parameters
        ----------
        df: pd.DataFrame
            dataframe whose columns are to be renamed
        con
            connection to SQLite DB
        table: str
            name of respective table

        Returns
        -------
        df: pd.DataFrame
            dataframe with fixed column names
        """

        # fetch column names
        cursor = con.execute('SELECT * FROM ' + table)
        cols = [description[0] for description in cursor.description]
        # fix index name
        df.index.name = "id"
        # rename columns
        return df.rename(columns=dict(zip(df.columns, cols[1:])))

    def fetch_to_csv(self, force_download=False, ratings_in="BX-Book-Ratings.csv", ratings_out="data/book-ratings.csv", books_in="BX-Books.csv", books_out="data/books.csv"):
        """
        Fetches data on book ratings to specified files.

        Parameters
        ----------
        force_download: bool
            force download of source zipfile
        ratings_in: str
            source filename of ratings datafile
        ratings_out: str
            destination filename of ratings datafile
        books_in: str
            source filename of books datafile
        books_out: str
            destination filename of books datafile
        """

        # download data if necessary
        if force_download or not os.path.isfile(self.datafile):
            self.download_data()

        # fetch data
        ratings, books = [self.fetch_data(self.datafile, src)
                          for src in [ratings_in, books_in]]
        # validate ISBNs
        ratings = ratings[ratings.apply(
            lambda x: is_isbn10(x["ISBN"]), axis=1)]
        # write to CSV
        ratings.to_csv(ratings_out, index=False, sep=";",
                       quoting=QUOTE_ALL, escapechar="\\", encoding="cp1251")
        books.to_csv(books_out, index=False, sep=";",
                     quoting=QUOTE_ALL, escapechar="\\", encoding="cp1251")

    def fetch_to_sqlite(self, force_download=False, ratings_in="BX-Book-Ratings.csv", books_in="BX-Books.csv", out="db.sqlite3"):
        """
        Fetches data on book ratings to local SQLite DB file.

        Parameters
        ----------
        force_download: bool
            force download of source zipfile
        ratings_in: str
            source filename of ratings datafile
        books_in: str
            source filename of books datafile
        out: str
            filename of local SQLite DB file
        """

        # download data if necessary
        if force_download or not os.path.isfile(self.datafile):
            self.download_data()

        # fetch data
        ratings, books = [self.fetch_data(self.datafile, src)
                          for src in [ratings_in, books_in]]
        # validate ISBNs
        ratings = ratings[ratings.apply(
            lambda x: is_isbn10(x["ISBN"]), axis=1)]
        # fix column names
        with sqlite3.connect(out) as con:
            ratings = self.fix_columns(ratings, con, "bookrec_rating")
            books = self.fix_columns(books, con, "bookrec_book")

        # # get book ratings
        means = ratings.groupby(
            "isbn")["rating"].mean().to_frame().reset_index()
        # books["rating"] = books["isbn"].map(means)
        books["count"] = books["isbn"].map(ratings["isbn"].value_counts())
        books["count"] = books["count"].fillna(0).astype(int)
        books = books.merge(means, on="isbn", how="left").fillna(0)

        # export to DB
        with sqlite3.connect(out) as con:
            books.to_sql("bookrec_book", con,
                         if_exists="replace", index_label="id")
            ratings.to_sql("bookrec_rating", con,
                           if_exists="replace", index_label="id")


if __name__ == "__main__":
    dd = DataDownloader()
    # dd.fetch_to_csv()
    dd.fetch_to_sqlite(out="../db.sqlite3")
