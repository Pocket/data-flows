def chunks(index, n):
    """Yield successive n-sized chunks from index."""
    for i in range(0, len(index), n):
        yield index[i: i + n]
