class LogLine:
    line_number = None

    date = None

    thread_id = None

    message = None

    log_type = None

    additional_info = None

    def __init__(self, line_number, date, thread_id, message, log_type):
        self.line_number = line_number
        self.date = date
        self.thread_id = thread_id
        self.message = message
        self.log_type = log_type
