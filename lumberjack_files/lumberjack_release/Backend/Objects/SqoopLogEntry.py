class SqoopLogEntry:
    line_number = None

    date = None

    thread_id = None

    message = None

    retries = None

    errors = None

    bytes_transferred = None

    additional_info = None

    def __init__(self, line_number, message):
        self.line_number = line_number
        self.message = message
