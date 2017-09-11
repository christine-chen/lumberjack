# Represents a single operation within a workflow
class Operation:

    # Database fields

    # same as Error.operation_link_id
    error_link_id = None

    duration = 0

    start_line_number = 0

    end_line_number = 0

    thread_id = 0

    operation_type = None

    bytes_transferred = 0

    message = None

    workflow_name = None

    completed = False

    # Additional fields

    errors_list = None

    def __init__(self, timestamp, start_line_number, end_line_number, thread_id, operation_type, message, completed,
                 bytes_transferred, error_link_id, workflow_name):
        self.error_link_id = error_link_id
        self.duration = timestamp
        self.start_line_number = start_line_number
        self.end_line_number = end_line_number
        self.thread_id = thread_id
        self.operation_type = operation_type
        self.message = message
        self.completed = completed
        self.bytes_transferred = bytes_transferred
        self.workflow_name = workflow_name
        self.errors_list = []
