from django.apps import AppConfig
from bookrec.bookrec import Model

model = None

class BookrecConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'bookrec'

    def ready(self):
        global model
        model = Model()
        model.load_csv()
