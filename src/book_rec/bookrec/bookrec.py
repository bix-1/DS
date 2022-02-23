#!/usr/bin/env python3.9

import os
import pandas as pd
if __name__ == "__main__":
    from download import DataDownloader
else:
    from bookrec.download import DataDownloader


class Model:
    """
    Class for book recommendation predictions.

    Attributes
    ----------
    dataset: pd.DataFrame
        dataset on books and book ratings
    """

    def __init__(self):
        self.dataset = pd.DataFrame()

    def set_dataset(self, ratings, books):
        """
        Creates dataset from provided ratings & books dataframes

        Parameters
        ----------
        ratings: pd.DataFrame
            ratings dataframe
        books: pd.DataFrame
            books dataframe
        """

        self.dataset = pd.merge(
            ratings[ratings["rating"] != 0], books, on=["isbn"])
        self.dataset = self.dataset.apply(
            lambda x: x.str.lower() if(x.dtype == "object") else x)

    def load_csv(self, ratings_file="data/ratings.csv", books_file="data/books.csv"):
        """
        Initialise dataset from csv file

        Parameters
        ----------
        ratings_file: str
            filename of csv file with rating data
        books_file: str
            filename of csv file with book data
        """

        # download data if necessary
        if not os.path.exists(ratings_file) or not os.path.exists(books_file):
            dd = DataDownloader()
            dd.fetch_to_csv(ratings_out=ratings_file, books_out=books_file)

        # load ratings
        ratings = pd.read_csv(ratings_file, encoding="cp1251", sep=";")
        ratings = ratings[ratings["rating"] != 0]
        # load books
        books = pd.read_csv(books_file, encoding="cp1251",
                            escapechar="\\", quotechar="\"", sep=";")

        # get dataset of books and their respective ratings
        self.dataset = pd.merge(ratings, books, on=["isbn"])
        self.dataset = self.dataset.apply(
            lambda x: x.str.lower() if(x.dtype == "object") else x)

    def get_relevant_reviews(self, title):
        """
        Get reviews produces by reviewers, that reviewed the given book

        Parameters
        ----------
        title: str
            book title for prediction
        """

        author = self.dataset["author"][self.dataset["title"]
                                        == title][:1].squeeze()
        # get users that reviewed the book
        book_reviewers = self.dataset["userID"][(self.dataset["title"] == title) & (
            self.dataset["author"] == author)].unique()

        # final dataset -- relevant reviews (by reviewers of the given book)
        return self.dataset[(self.dataset["userID"].isin(book_reviewers))]

    def get_relevant_books(self, dataset, threshold=8):
        """
        Create book review dataset with reviews above specified threshold.

        Parameters
        ----------
        dataset: pd.DataFrame
            book review dataset
        threshold: int
            min ammount of reviews per book

        Returns
        -------
        df: pd.DataFrame
            book review dataframe
        """

        # Number of ratings per book
        rating_counts = dataset.groupby(["title"]).agg("count").reset_index()
        # filter out books with low number of reviews
        threshold = 8
        books_to_compare = rating_counts[
            "title"][rating_counts["userID"] >= threshold].unique()
        if books_to_compare.size < 1:
            return pd.DataFrame()
        # create dataset
        return dataset[[
            "userID", "rating", "title", "isbn"]][dataset["title"].isin(books_to_compare)]

    def predict(self, title, max_entries=10):
        """
        Predict book recommendations for given book title using correlation of book reviews.

        Parameters
        ----------
        title: str
            book title for prediction
        max_entries: int
            max number of recommended book titles to be returned

        Returns
        -------
        df: pd.DataFrame
            dataframe of predicted book title recommendations
        """

        title = title.lower()
        if title not in self.dataset["title"].values:
            print("No prediction available")
            return pd.DataFrame()

        # create book review dataset
        dataset = self.get_relevant_reviews(title)
        ratings_data_raw = self.get_relevant_books(dataset)
        if ratings_data_raw.empty:
            print("No prediction available")
            return pd.DataFrame()

        # get mean rating per each book and reviewer
        ratings_data_raw_nodup = ratings_data_raw.groupby(
            ["userID", "title"])["rating"].mean().to_frame().reset_index()
        # create correlation dataset
        corr_dataset = ratings_data_raw_nodup.pivot(
            index="userID", columns="title", values="rating")
        # take out the given book from the correlation dataset
        other_books = corr_dataset.loc[:, corr_dataset.columns != title]

        # list of book titles
        book_titles = list(other_books.columns.values)
        # compute correlations
        correlations = [corr_dataset[title].corr(
            other_books[t]) for t in book_titles]
        # compute average rating
        tabs = [ratings_data_raw[ratings_data_raw["title"] ==
                                 t].groupby(ratings_data_raw["title"]).mean() for t in book_titles]
        avgrating = [tab["rating"].min() for tab in tabs]

        # final dataframe of all correlation of each book
        isbns = [ratings_data_raw["isbn"][ratings_data_raw["title"]
                                          == t].iloc[0] for t in book_titles]
        df = pd.DataFrame(list(zip(isbns, book_titles, correlations, avgrating)), columns=[
                          "isbn", "book", "corr", "avg_rating"])

        # sort values by correlation output
        return df.sort_values("corr", ascending=False).head(max_entries)


if __name__ == "__main__":
    model = Model()
    model.load_csv()
    title = "The Fellowship of The Ring (The Lord of The Rings, Part 1)"
    print("Prediction for:\n\t{}\n{}".format(
        title, model.predict(title)[["book", "avg_rating"]]))
