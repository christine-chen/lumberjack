class Error:

    # Database fields

    # application name + line number
    operation_link_id = None

    operation_type = None

    line_number = None

    thread_id = None

    heading = None

    date = None

    traceback = None

    workflow_name = None

    # Additional fields

    is_error_heading_assigned = None

    def __init__(self, operation_type, thread_id, line_number, date, traceback):
        self.operation_type = operation_type
        self.is_error_heading_assigned = False
        self.heading = ""
        self.thread_id = thread_id
        self.line_number = line_number
        self.date = date
        self.traceback = traceback
        self.line_number = line_number
