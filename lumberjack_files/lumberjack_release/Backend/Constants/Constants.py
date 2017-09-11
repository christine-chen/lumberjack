class Constants:

    # Constant values

    START = "Start"
    END = "End"
    DURATION = "Duration"
    RUN_ID = "Run Id"
    FINAL_STATUS = "Final Status"
    RUNNING_WORKFLOW = "Running Workflow"
    INGESTING_WORKFLOW = "Ingesting Workflow"
    WORKFLOW_ERRORS = "Workflow Error Log"
    SUCCESS = "Success"
    FAILED = "Failed"
    SKIP_TRANSFORMATION = "Skipping Transformation"
    SKIP_VALIDATION = "Skipping Validation"

    # Error severity types
    WARNING = "Warning"
    ERROR = "Error"
    FATAL = "Fatal"

    # Operation type constants
    IMPORT = "Import"
    SQOOP_JAVA_LOG = "Sqoop (Java Log)"
    TRANSFORM = "Transform"
    VALIDATE = "Validate"
    MERGE = "Merge"

    # Constant indexes for parsing a raw tokenized line from the log file
    DATE_INDEX_RAW_LINE = 0
    LOG_TYPE_RAW_LINE = 2
    THREAD_INDEX_RAW_LINE = 3
    MESSAGE_INDEX_RAW_LINE = 4

    # Additional constants
    R3D3_Status = "R3D3Status"
    WF_ERROR = "WF Error"
    RETRY = "Retry"
    LOG_NAME = "Log Name"
    VALUE = None
    NULL_WORKFLOW = "Null Workflow"

    def __init__(self, value):
        self.VALUE = value


