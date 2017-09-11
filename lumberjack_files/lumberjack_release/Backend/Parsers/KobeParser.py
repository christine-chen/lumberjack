from datetime import datetime
from collections import OrderedDict
from .. Constants.Constants import Constants
from .. Objects.Operation import Operation
from .. Objects.Workflow import Workflow
from .. Objects.Error import Error
from .. Objects.App import App


class KobeParser:

    # The list of raw events from the FileParser
    raw_event_list = None

    # The object representing the current ingestion app
    current_app = None

    # The object representing the current workflow which will be appended to the current_app
    current_workflow_name = None

    # A list of valid thread numbers
    thread_id_list = None

    # A bool value specifying if a [FINAL STATUS] message is present in the log file
    is_final_status_present = False

    after_final_status = False

    # Specifies if the previous raw event is a starting event
    is_previous_event_starting_type = False

    # Starting events
    import_starting_event = None
    sqoop_java_starting_event = None
    validate_starting_event = None
    transform_starting_event = None
    merge_starting_event = None

    current_line_number = None

    previous_raw_event = None

    def __init__(self):
        self.current_workflow_name = Constants.NULL_WORKFLOW
        self.current_app = App()
        self.thread_id_list = []
        self.current_line_number = 0

    def run(self, raw_event_list):
        self.raw_event_list = raw_event_list
        self.initialize_workflow_dict()
        self.generate_list_of_thread_numbers(self)
        self.parse_event_list(self)
        self.calculate_total_durations_for_workflow_and_app(self)
        self.successes(self)
        self.calculate_app_bytes_transferred(self)
        self.calculate_app_rows_pulled(self)
        self.remove_errors_after_final_message(self)
        self.sort_operations_by_line_numbers(self)
        self.link_error_to_operation(self)
        self.calculate_total_time(self)

        return self.current_app

    @staticmethod
    def calculate_total_time(self):
        """
        :summary: Calculates the duration of the application from starting to end timestamp

        :param self: The current instance of the class

        """
        if self.current_app.start_date is not None and self.current_app.end_date is not None:
            self.current_app.total_ingestion_time = int((
                                                            self.current_app.end_date - self.current_app.start_date).total_seconds())

    @staticmethod
    def remove_errors_after_final_message(self):
        """
        :summary: errors at the end of the log are removed, because they are redundant

        :param self: The current instance of the class

        """
        workflow = self.current_app.workflow_dict.get(Constants.NULL_WORKFLOW)
        for error in workflow.errors_list:
            if error.traceback == "":
                self.current_app.errors_list.remove(error)

    @staticmethod
    def successes(self):
        """
        :summary: Puts all successful workflows in a list for the current app so it is easier to display on UI

        :param self: The current instance of the class

        """
        for workflow in self.current_app.workflow_dict.values():
            if workflow.name not in self.current_app.failed_workflows_list:
                workflow.has_succeeded = True
                self.current_app.successful_workflows_list.append(workflow)

    @staticmethod
    def parse_event_list(self):
        """
        :summary: Parse the raw event list one event at a time

        :param self: The current instance of the class

        """
        for thread_id in self.thread_id_list:
            for raw_event in self.raw_event_list:
                if thread_id == raw_event.thread_id:
                    self.parse_kobe_event(self, raw_event)

        # If no [FINAL STATUS] message was found, create an error
        if self.is_final_status_present is False:
            self.current_app.errors_list.append(Error(
                Constants.FATAL, 0, -1, datetime.min, "Final Status Message Missing"))

    def initialize_workflow_dict(self):
        """
        :summary: Populate current application workflow dictionary {name: workflow object}
        so that we can access workflow objects and assign additional variables that need
        to be set:

        """
        self.current_app.workflow_dict = OrderedDict()
        self.current_app.workflow_dict[Constants.NULL_WORKFLOW] = Workflow(Constants.NULL_WORKFLOW, datetime.min)
        for raw_event in self.raw_event_list:
            if raw_event.operation_type == Constants.RUNNING_WORKFLOW:
                workflow_name = self.parse_workflow_name(raw_event.message)
                self.current_app.workflow_dict[workflow_name] = Workflow(workflow_name)

    @staticmethod
    def parse_kobe_event(self, raw_event):
        """
        :summary raw_events are mapped to functions by accessing raw_event.operation_type:

        :param self: The current instance of the class
        :param raw_event: The RawEvent object to parse

        """
        dispatch_table = {
            Constants.LOG_NAME: self.handle_log_name_and_path,
            Constants.RUN_ID: self.handle_generated_run_id,
            Constants.START: self.handle_starting_line,
            Constants.END: self.handle_ending_line,
            Constants.IMPORT: self.handle_import,
            Constants.RUNNING_WORKFLOW: self.handle_new_workflow,
            Constants.INGESTING_WORKFLOW: self.handle_new_workflow,
            Constants.SQOOP_JAVA_LOG: self.handle_sqoop_java,
            Constants.TRANSFORM: self.handle_transform,
            Constants.SKIP_TRANSFORMATION: self.handle_transform,
            Constants.VALIDATE: self.handle_validate,
            Constants.SKIP_VALIDATION: self.handle_validate,
            Constants.MERGE: self.handle_merge,
            Constants.ERROR: self.handle_error,
            Constants.R3D3_Status: self.handle_r3d3_status,
            Constants.FINAL_STATUS: self.handle_final_status,
            Constants.SUCCESS: self.handle_new_workflow,
            Constants.FAILED: self.handle_new_workflow,
            Constants.WF_ERROR: self.handle_final_status,
            Constants.RETRY: self.handle_retry}

        # Fetch the proper function from the dispatch table
        kobe_function = dispatch_table[raw_event.operation_type]
        kobe_function(self, raw_event)

    @staticmethod
    def create_incomplete_event(self):
        """
        :summary called to check if previous event is a starting one.
        if it is, then we know an error has interrupted an operation
        and use this error as the ending event to create an operation:

        :param self: The current instance of the class

        """
        raw_event = None
        error_link_id = None
        if self.import_starting_event is not None:
            raw_event = self.import_starting_event
            self.import_starting_event = None
        elif self.sqoop_java_starting_event is not None:
            raw_event = self.sqoop_java_starting_event
            self.sqoop_java_starting_event = None
        elif self.validate_starting_event is not None:
            raw_event = self.validate_starting_event
            self.validate_starting_event = None
        elif self.transform_starting_event is not None:
            raw_event = self.transform_starting_event
            self.transform_starting_event = None
        elif self.merge_starting_event is not None:
            self.merge_starting_event = None

        if raw_event is not None:
            error_link_id = self.current_app.name + str(raw_event.line_number)
            incomplete_operation = Operation(0, raw_event.line_number, raw_event.line_number, raw_event.thread_id,
                                             raw_event.operation_type, raw_event.message, False,
                                             raw_event.bytes_transferred, error_link_id,
                                             self.current_workflow_name)
            self.current_app.workflow_dict.get(self.current_workflow_name).add_operation(incomplete_operation)

    @staticmethod
    def handle_log_name_and_path(self, raw_event):
        """
        :summary: assign log_file_path and log_file_name to the current application
        so that people who are troubleshooting know where to look for the log

        :param self: The current instance of the class
        :param raw_event: The RawEvent object

        """
        file_path_tokens = raw_event.message.split("/")
        if len(file_path_tokens) > 1:
            self.current_app.log_file_name = file_path_tokens[len(file_path_tokens) - 1]
            file_path_tokens = file_path_tokens[:-1]
            file_path = ""
            for path in file_path_tokens:
                file_path += path + "/"
            self.current_app.log_file_path = file_path

        else:
            self.current_app.log_file_name = file_path_tokens[0]
            self.current_app.log_file_path = ""

    @staticmethod
    def handle_retry(self, raw_event):
        """
        :summary: Placeholder method, retry parsing not yet supported

        :param self: The current instance of the class
        :param raw_event: The RawEvent object

        """
        self.current_app.workflow_dict.get(self.current_workflow_name).retries += 1

    @staticmethod
    def handle_generated_run_id(self, raw_event):
        self.current_app.id = raw_event.message

    @staticmethod
    def handle_starting_line(self, raw_event):
        self.current_app.name = self.parse_app_name(raw_event)
        self.current_app.start_date = raw_event.date

    @staticmethod
    def parse_app_name(raw_event):
        app_name = raw_event.message.replace("\n", "")
        app_name = app_name.split(": ")
        return app_name[1]

    @staticmethod
    def handle_ending_line(self, raw_event):
        self.current_app.end_date = raw_event.date

    @staticmethod
    def parse_workflow_name(message):
            trimmed_message = message.replace(" ", "")
            token_string = trimmed_message.split("-")
            if len(token_string) < 2:
                print " - was not present in token_string[1] in parse_sqoop_bytes"
                return Constants.NULL_WORKFLOW
            return token_string[1]

    @staticmethod
    def lookup_workflow_name(self, message):
        for key in self.current_app.workflow_dict.keys():
            if key in message:
                return key
        print "no matching workflow found in ingesting message"
        return Constants.NULL_WORKFLOW

    @staticmethod
    def handle_new_workflow(self, raw_event):
        workflow_name = self.lookup_workflow_name(self, raw_event.message)
        workflow = self.current_app.workflow_dict.get(workflow_name)
        workflow.date = self.current_app.start_date
        workflow.application_name = self.current_app.name
        workflow.log_file_name = self.current_app.log_file_name
        workflow.log_file_path = self.current_app.log_file_path
        self.current_workflow_name = workflow_name

    @staticmethod
    def split_workflow_name_id(message):
        message = message.split("-")
        if len(message) < 2:
            return "workflow name and id were not split correctly"
        return message

    @staticmethod
    def handle_import(self, raw_event):
        """
        :summary: Handle raw events of 'Import' type and create a new Operation object from these RawEvents

        :param self: The current instance of the class
        :param raw_event: The starting or ending RawEvent object

        """
        if raw_event.is_starting is False:
            if self.import_starting_event is not None:
                if self.import_starting_event.date == -1 or raw_event.date == -1:
                    import_error = Error(Constants.IMPORT, raw_event.thread_id, raw_event.line_number,
                                         raw_event.date, "Invalid Import date")
                    self.current_app.workflow_dict.get(self.current_workflow_name).errors_list.append(import_error)
                elif raw_event.thread_id == self.import_starting_event.thread_id:
                    duration = (raw_event.date - self.import_starting_event.date).total_seconds()
                    error_link_id = self.current_app.name + str(self.import_starting_event.line_number)
                    operation = Operation(int(duration), self.import_starting_event.line_number, raw_event.line_number,
                                          raw_event.thread_id, raw_event.operation_type, raw_event.message, True,
                                          raw_event.bytes_transferred, error_link_id, self.current_workflow_name)
                    self.current_app.workflow_dict.get(self.current_workflow_name).add_operation(operation)
                    self.current_app.workflow_dict.get(self.current_workflow_name).rows_pulled += raw_event.rows_pulled
                    self.import_starting_event = None
        else:
            if raw_event.is_starting:
                self.import_starting_event = raw_event
        self.previous_raw_event = raw_event

    @staticmethod
    def handle_sqoop_java(self, raw_event):
        """
        :summary: Handle raw events of 'Sqoop Java' type and create a new Operation object from these RawEvents

        :param self: The current instance of the class
        :param raw_event: The starting or ending RawEvent object

        """
        if raw_event.is_starting is False:
            if self.sqoop_java_starting_event is not None:
                if self.sqoop_java_starting_event.date == -1 or raw_event.date == -1:
                    self.current_app.workflow_dict.get(self.current_workflow_name).errors_list.append(
                        Error(Constants.SQOOP_JAVA_LOG, raw_event.thread_id, raw_event.line_number, raw_event.date, "Invalid Sqoop Java date"))
                elif raw_event.thread_id == self.sqoop_java_starting_event.thread_id:
                    duration = (raw_event.date - self.sqoop_java_starting_event.date).total_seconds()
                    error_link_id = self.current_app.name + str(self.sqoop_java_starting_event.line_number)
                    operation = Operation(
                        int(duration), self.sqoop_java_starting_event.line_number, raw_event.line_number,
                        raw_event.thread_id, raw_event.operation_type, raw_event.message, True,
                        raw_event.bytes_transferred, error_link_id, self.current_workflow_name)
                    self.current_app.workflow_dict.get(self.current_workflow_name).add_operation(operation)
                    self.current_app.workflow_dict.get(self.current_workflow_name).bytes_transferred \
                        += raw_event.bytes_transferred
                    self.sqoop_java_starting_event = None
        else:
            if raw_event.is_starting:
                self.sqoop_java_starting_event = raw_event
        self.previous_raw_event = raw_event

    @staticmethod
    def handle_transform(self, raw_event):
        """
        :summary: Handle raw events of 'Transform' type and create a new Operation object from these RawEvents

        :param self: The current instance of the class
        :param raw_event: The starting or ending RawEvent object

        """
        if raw_event.is_starting is False and self.transform_starting_event is not None:
            if raw_event.operation_type == Constants.SKIP_TRANSFORMATION:
                duration = 0
            elif raw_event.thread_id == self.transform_starting_event.thread_id:
                duration = (raw_event.date - self.transform_starting_event.date).total_seconds()
            error_link_id = self.current_app.name + str(self.transform_starting_event.line_number)
            operation = Operation(
                        int(duration), self.transform_starting_event.line_number, raw_event.line_number, raw_event.thread_id,
                        raw_event.operation_type, raw_event.message, True, 0, error_link_id, self.current_workflow_name)
            self.current_app.workflow_dict.get(self.current_workflow_name).add_operation(operation)
            self.transform_starting_event = None
        else:
            if raw_event.is_starting:
                self.transform_starting_event = raw_event
        self.previous_raw_event = raw_event

    @staticmethod
    def handle_validate(self, raw_event):
        """
        :summary: Handle raw events of 'Validate' type and create a new Operation object from these RawEvents

        :param self: The current instance of the class
        :param raw_event: The starting or ending RawEvent object

        """
        if raw_event.is_starting is False and self.validate_starting_event is not None:
            if raw_event.operation_type == Constants.SKIP_VALIDATION:
                duration = 0
            elif raw_event.thread_id == self.validate_starting_event.thread_id:
                duration = (raw_event.date - self.validate_starting_event.date).total_seconds()
            error_link_id = self.current_app.name + str(self.validate_starting_event.line_number)
            operation = Operation(
                int(duration), self.validate_starting_event.line_number, raw_event.line_number, raw_event.thread_id,
                raw_event.operation_type, raw_event.message, True, 0, error_link_id, self.current_workflow_name)
            self.current_app.workflow_dict.get(self.current_workflow_name).add_operation(operation)
            self.validate_starting_event = None
        else:
            if raw_event.is_starting:
                self.validate_starting_event = raw_event
        self.previous_raw_event = raw_event

    @staticmethod
    def handle_merge(self, raw_event):
        """
        :summary: Handle raw events of 'Merge' type and create a new Operation object from these RawEvents

        :param self: The current instance of the class
        :param raw_event: The starting or ending RawEvent object

        """

        if self.merge_starting_event is not None and raw_event.is_starting is False:
            if raw_event.thread_id == self.merge_starting_event.thread_id:
                duration = (raw_event.date - self.merge_starting_event.date).total_seconds()
                error_link_id = self.current_app.name + str(self.merge_starting_event.line_number)
                operation = Operation(
                    int(duration), self.merge_starting_event.line_number, raw_event.line_number, raw_event.thread_id,
                    raw_event.operation_type, raw_event.message, True, 0, error_link_id, self.current_workflow_name)
                self.current_app.workflow_dict.get(self.current_workflow_name).add_operation(operation)
                self.merge_starting_event = None
        else:
            if raw_event.is_starting:
                self.merge_starting_event = raw_event
        self.previous_raw_event = raw_event

    @staticmethod
    def handle_error(self, raw_event):
        """
        :summary: Handle raw events of 'Error' type and create a new Operation object from these RawEvents

        :param self: The current instance of the class
        :param raw_event: The starting or ending RawEvent object

        """

        self.current_app.are_errors_in_workflows = True

        if self.current_line_number > raw_event.line_number:
            self.current_workflow_name = Constants.NULL_WORKFLOW

        if "Validation Error!!" in raw_event.error.heading or "Failed Validation!!" in raw_event.error.heading:
            raw_event.error.operation_type = Constants.VALIDATE
        elif "The data may have shifted while loading." in raw_event.error.heading:
            raw_event.error.operation_type = Constants.VALIDATE
        elif "Improper values have been found in the columns of" in raw_event.error.heading:
            raw_event.error.operation_type = Constants.VALIDATE
        else:
            self.create_incomplete_event(self)

        raw_event.error.workflow_name = self.current_workflow_name
        self.current_app.workflow_dict.get(self.current_workflow_name).errors_list.append(raw_event.error)

    @staticmethod
    def handle_r3d3_status(self, raw_event):
        """
        :summary: Handles a RawEvent of r3d3 type and sets the current is_r3d3 status

        :param self: The current instance of the class
        :param raw_event: The starting or ending RawEvent object

        """
        self.current_app.workflow_dict.get(self.current_workflow_name).is_r3d3 = (raw_event.message == "True")

    @staticmethod
    def handle_final_status(self, raw_event):
        """
        :summary: Handles a RawEvent of 'Final Status' type and checks for workflow errors

        :param self: The current instance of the class
        :param raw_event: The starting or ending RawEvent object

        """
        self.is_final_status_present = True
        self.after_final_status = True
        if raw_event.operation_type == Constants.WF_ERROR:
            # this raw_event.error is a list of errors instead of and Error object
            self.current_app.failed_workflows_list = raw_event.error

    @staticmethod
    def calculate_event_duration(starting_event, ending_event):
        return ending_event.date - starting_event.date

    @staticmethod
    def generate_list_of_thread_numbers(self):
        """
        :summary: Generates a list of all valid thread numbers for the current app

        :param self: The current instance of the class

        """
        for event in self.raw_event_list:
            if event.thread_id not in self.thread_id_list:
                self.thread_id_list.append(event.thread_id)

    @staticmethod
    def calculate_total_durations_for_workflow_and_app(self):
        """
        :summary: Calculates the total elapsed runtime durations for both the app and workflows

        :param self: The current instance of the class
        """
        app = self.current_app
        for key in app.workflow_dict.keys():
            dictionary = app.workflow_dict.get(key)

            # Calculate per workflow
            dictionary.total_duration += int(dictionary.import_duration) + int(
                dictionary.transform_duration) + int(dictionary.validation_duration) + int(dictionary.merge_duration)

            # Calculate for app
            app.import_duration += dictionary.import_duration
            app.transform_duration += dictionary.transform_duration
            app.validation_duration += dictionary.validation_duration
            app.merge_duration += dictionary.merge_duration

        app.total_duration += app.import_duration + app.transform_duration + app.merge_duration

        app.import_duration = int(app.import_duration)
        app.transform_duration = int(app.transform_duration)
        app.validation_duration = int(app.validation_duration)
        app.merge_duration = int(app.merge_duration)
        app.total_duration = int(app.total_duration)

    @staticmethod
    def calculate_app_rows_pulled(self):
        app = self.current_app
        for key in app.workflow_dict.keys():
            workflow = app.workflow_dict.get(key)
            self.current_app.rows_pulled += workflow.rows_pulled

    @staticmethod
    def calculate_app_bytes_transferred(self):
        app = self.current_app
        for key in app.workflow_dict.keys():
            workflow = app.workflow_dict.get(key)
            self.current_app.bytes_transferred += workflow.bytes_transferred
        self.current_app.bytes_transferred = self.current_app.bytes_transferred

    @staticmethod
    def sort_operations_by_line_numbers(self):
        """
        :summary: This is called because in FileParser, errors are created first,
        so the error raw events will occur first, however, we want to parse
        the raw events based on the order of line numbers:

        :param self: The current instance of the class

        """
        for key in self.current_app.workflow_dict:
            workflow_operations_list = self.current_app.workflow_dict.get(key).operations_list
            workflow_operations_list.sort(key=lambda x: x.start_line_number, reverse=False)

    @staticmethod
    def link_error_to_operation(self):
        """
        :summary: Iterates through the list of errors and links them to the corresponding workflows

        :param self: The current instance of the class

        """
        for key in self.current_app.workflow_dict:
            workflow = self.current_app.workflow_dict.get(key)
            for error in workflow.errors_list:
                for index in range(0, len(workflow.operations_list)):
                    operation = workflow.operations_list[index]
                    if index < len(workflow.operations_list) - 1:
                        next_operation = workflow.operations_list[index + 1]
                        if operation.start_line_number < error.line_number < next_operation.start_line_number:
                            error.operation_link_id = operation.error_link_id
                    else:
                        if operation.start_line_number < error.line_number:
                            error.operation_link_id = operation.error_link_id
