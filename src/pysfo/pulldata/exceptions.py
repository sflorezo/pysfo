class SeriesNotFoundError(Exception):
    """Raised when a series pulled through pysfo.pulldata is not found."""
    def __init__(self, not_found_list, message=None):
        if message is None:
            message = f"Series '{not_found_list}' not found."
        super().__init__(message)
        self.not_found_list = not_found_list