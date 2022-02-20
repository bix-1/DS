from django.views.generic import ListView
from .models import Book
from bookrec.apps import model


class SearchView(ListView):
    model = Book
    template_name = "search_results.html"

    def get_queryset(self):
        # search book title
        if "search" in self.request.GET:
            query = self.request.GET.get("search")
            return Book.objects.filter(title__icontains=query).order_by("-count") if query else None

        # get book recommendations
        elif "bookrec" in self.request.GET:
            # get predictions
            preds = model.predict(self.request.GET.get("bookrec"))
            if preds.empty:
                return None
            # get book details
            isbn_list = preds["isbn"].values.tolist()
            ordering = {isbn: i for i, isbn in enumerate(isbn_list)}
            return sorted(Book.objects.filter(isbn__in=isbn_list), key=lambda x: ordering.get(x.isbn))
