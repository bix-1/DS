from django.urls import path
from .views import SearchView

from . import views

urlpatterns = [
    path('', SearchView.as_view(), name='search'),
]
