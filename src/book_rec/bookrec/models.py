from optparse import Values
from turtle import title
from django.db import models
from django.db.models import Avg
import datetime
import pandas as pd


def YEAR_CHOICES():
    return [(y, y) for y in range(0, datetime.date.today().year+1)]


class Book(models.Model):
    isbn = models.CharField(max_length=20, blank=False, null=False, primary_key=True)
    title = models.CharField(max_length=100, blank=False, null=False)
    author = models.CharField(max_length=50, blank=False, null=False)
    year = models.IntegerField(
        choices=YEAR_CHOICES(), default=datetime.datetime.now().year, blank=False, null=False)
    publisher = models.CharField(max_length=50, blank=False, null=False)
    image_s = models.URLField("Small Image", blank=False, null=False)
    image_m = models.URLField("Medium Image", blank=False, null=False)
    image_l = models.URLField("Large Image", blank=False, null=False)

    def __str__(self):
        return self.title

    def count(self):
        return self.rating_set.all().count()

    def rating(self):
        return self.rating_set.all().aggregate(avg=Avg("rating"))["avg"]


class Rating(models.Model):
    userID = models.CharField(max_length=10, blank=False, null=False)
    book = models.ForeignKey(Book, on_delete=models.CASCADE, db_column="isbn")
    rating = models.IntegerField(blank=False, null=False)
