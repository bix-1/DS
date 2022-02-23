from django.apps import AppConfig
from bookrec.bookrec import Model
import pandas as pd
import sqlite3

model = None


class BookrecConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'bookrec'

    def ready(self):
        global model
        model = Model()
        # model.load_csv()
        with sqlite3.connect("db.sqlite3") as con:
            ratings = pd.read_sql_query("SELECT * FROM bookrec_rating", con)
            books = pd.read_sql_query("SELECT * FROM bookrec_book", con)
            model.set_dataset(ratings, books)
