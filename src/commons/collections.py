from itertools import islice

def paginated(page_size, iterable):
    i = iter(iterable)
    piece = list(islice(i, page_size))
    while piece:
        yield piece
        piece = list(islice(i, page_size))
