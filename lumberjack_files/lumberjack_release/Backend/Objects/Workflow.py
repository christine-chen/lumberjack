# Represents a single workflow within a Kobayashi ingestion run
from .. Constants.Constants import Constants


class Workflow:

    # Database fields
    id = None

    name = None

    date = None

    total_duration = 0

    import_duration = 0

    transform_duration = 0

    validation_duration = 0

    merge_duration = 0

    bytes_transferred = 0

    rows_pulled = 0

    has_succeeded = False

    retries = 0

    is_r3d3 = False

    application_name = None

    log_file_name = None

    log_file_path = None

    # Additional fields

    operations_list = None

    errors_list = None

    def __init__(self, name, id=0):
        self.name = name
        self.id = id
        self.operations_list = []
        self.errors_list = []

    def add_operation(self, operation):
        if operation.operation_type == Constants.IMPORT:
            self.import_duration += operation.duration
        elif operation.operation_type == Constants.TRANSFORM:
            self.transform_duration += operation.duration
        elif operation.operation_type == Constants.VALIDATE:
            self.validation_duration += operation.duration
        elif operation.operation_type == Constants.MERGE:
            self.merge_duration += operation.duration
        self.operations_list.append(operation)
