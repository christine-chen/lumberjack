class RawEvent:
    date = None

    line_number = None

    thread_id = None

    operation_type = None

    message = None

    retries = None

    error = None

    log_type = None

    bytes_transferred = None

    rows_pulled = 0

    additional_info = None

    is_starting = True



    def __init__(self, line_number, date, thread_id, operation_type, message, retries, bytes_transferred, is_starting, rows_pulled=0):
        self.date = date
        self.line_number = line_number
        self.thread_id = thread_id
        self.operation_type = operation_type
        self.message = message
        self.retries = retries
        self.bytes_transferred = bytes_transferred
        self.is_starting = is_starting
        self.rows_pulled = rows_pulled

