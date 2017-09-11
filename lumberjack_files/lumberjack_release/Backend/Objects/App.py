# Represents a full Kobayashi ingestion app
from collections import OrderedDict


class App:

    # Database fields
    id = None

    name = None

    start_date = None

    end_date = None

    bytes_transferred = 0

    total_duration = 0

    import_duration = 0

    total_ingestion_time = 0

    transform_duration = 0

    validation_duration = 0

    merge_duration = 0

    rows_pulled = 0

    log_file_name = None

    log_file_path = None

    # Additional fields

    # Specifies if one or more workflows within the current app contain any errors
    are_errors_in_workflows = None

    successful_workflows_list = None

    failed_workflows_list = None

    errors_list = None

    workflow_dict = None

    def __init__(self):
        self.workflow_dict = OrderedDict()
        self.errors_list = []
        self.successful_workflows_list = []
        self.failed_workflows_list = []
        self.workflow_dict = {}
        self.are_errors_in_workflows = False
