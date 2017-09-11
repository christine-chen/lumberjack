from datetime import datetime
from .. Constants.Constants import Constants
from .. Objects.LogLine import LogLine
from .. Objects.SqoopLogEntry import SqoopLogEntry
from .. Objects.Error import Error
from .. Objects.RawEvent import RawEvent


class FileParser:

    # The list of LogLine objects
    log_line_list = None

    # The list of all sqoop log messages as well as their corresponding dates and messages
    sqoop_log_dictionary = None

    # The final list of raw events
    raw_event_list = None

    # The current sqoop java log message
    current_sqoop_message = ""

    # The current error traceback message
    current_error_traceback_message = ""

    # Specifies if a previous scoop call has been encountered
    is_first_sqoop_call = True

    # Specifies if the current app is an r3d3 app
    r3d3_status = False

    current_error_line = None

    current_log_line = None

    current_line_number = 1

    previous_line_number = -1

    def __init__(self):
        self.log_line_list = []
        self.sqoop_log_dictionary = {}
        self.raw_event_list = []

    def run(self, log_file_name):
        """
        :summary: Parses a log file and returns a sorted list of RawEvent objects

        :param: log_file_name:
        :return: A sorted list of
         objects
        """

        raw_log_line_list = open(log_file_name, "r").readlines()
        self.raw_event_list.append(RawEvent(0, datetime.min, 0, Constants.LOG_NAME, log_file_name, 0, 0, True))
        self.parse_log(self, raw_log_line_list)
        self.produce_stats(self)

        # Sort the list of RawEvent objects by line number before returning
        self.raw_event_list.sort(key=lambda e: e.line_number)
        return self.raw_event_list

    @staticmethod
    def parse_log(self, raw_log_line_list):
        """
        :summary: Handles the parsing logic for a raw log file

        :param raw_log_line_list: The list of LogLine objects
        :param self: The current instance of the class
        """
        log_line = self.parse_log_line(self, raw_log_line_list[0])
        self.handle_starting_date(self, log_line)

        # Iterate though every line in the log file and check if each line has a valid thread id
        for line in raw_log_line_list:
            if "Thread-" in line or "MainThread" in line:
                self.parse_kobe_line(self, line)
            else:
                self.parse_non_kobe_line(self, line)
            self.current_line_number += 1

        # Generate a unique hash key and add the last sqoop log to the sqoop_log_dictionary
        hash_key = self.create_date_thread_hash(self.current_log_line)
        self.sqoop_log_dictionary[hash_key] = SqoopLogEntry(self.previous_line_number, self.current_sqoop_message)

        # Handle the last error message after exiting the loop
        if self.current_error_line is not None:
            self.create_error_as_raw_event(self, Constants.ERROR, self.current_error_line,
                                           self.current_error_traceback_message)

    @staticmethod
    def parse_kobe_line(self, line):
        """
        :summary: Parses a standard log line with a valid timestamp and thread id

        :param line: The raw text of the current line
        :param self: The current instance of the class
        """
        self.current_log_line = self.parse_log_line(self, line)
        self.log_line_list.append(self.current_log_line)

        if self.current_error_line is not None:
            self.create_error_as_raw_event(self, Constants.ERROR, self.current_error_line,
                                           self.current_error_traceback_message)
            self.current_error_line = None
            self.current_error_traceback_message = ""

        if " (ERROR) " in line:
            self.current_error_line = self.current_log_line

        if "Running Sqoop" in line and "WARNING" in line:
            if self.is_first_sqoop_call is False:
                hash_key = self.create_date_thread_hash(self.current_log_line)
                self.sqoop_log_dictionary[hash_key] = SqoopLogEntry(self.previous_line_number,
                                                                    self.current_sqoop_message)
                self.current_sqoop_message = line

            self.previous_line_number = self.current_line_number
            self.is_first_sqoop_call = False

    @staticmethod
    def parse_non_kobe_line(self, line):
        """
        :summary: Parses a non-standard log line

        :param line: The raw text of the current line
        :param self: The current instance of the class
        """
        if "WF_ERRORS:" in line:
            app_failures = self.parse_wf_errors(line)
            raw_event = RawEvent(0, None, None, Constants.WF_ERROR, line, 0, 0, None)
            raw_event.error = app_failures
            self.raw_event_list.append(raw_event)
        if self.current_error_line is not None:
            self.current_error_traceback_message += line
        elif "app_name" not in line:
            self.current_sqoop_message += line
        if "app_name" in line:
            self.parse_r3d3_status(self, line)

    @staticmethod
    def parse_wf_errors(line):
        """
        :summary: Parse info from the WF_ERRORS (Workflow Errors) line

        :param line: The raw text of the WF_ERRORS line
        :return: The parsed string
        """
        line = line.split("WF_ERRORS: ")[1]
        line = line.split("\n")[0]
        line = line.replace("'", "")
        line = line.replace("[", "")
        line = line.replace("]", "")
        line = line.replace(" ", "")
        line = line.split(",")
        for element in line:
            element.replace("'", "")
        return line

    @staticmethod
    def create_error_as_raw_event(self, error_type, log_line, traceback):
        error_heading = ""
        if "Unexpected error:<" in log_line.message:
            return
        if "Query failed, retrying" in log_line.message:
            raw_event = RawEvent(log_line.line_number, log_line.date, log_line.thread_id,
                                 Constants.RETRY, "", 0, 0, True)
            self.raw_event_list.append(raw_event)
            return

        elif "Unexpected error:<" not in log_line.message:
            traceback_line_list = traceback.split("\n")
            i = len(traceback_line_list) - 1
            while i > 0:
                if len(traceback_line_list[i]) > 1:
                    error_heading = traceback_line_list[i]
                    break
                i -= 1

        traceback = traceback.replace(error_heading, "")
        if log_line.message[0:20] not in error_heading:
            traceback = log_line.message + traceback

        raw_event = RawEvent(log_line.line_number, log_line.date, log_line.thread_id, Constants.ERROR, "", 0, 0, True)
        raw_event.error = Error(error_type, log_line.thread_id, log_line.line_number, log_line.date, traceback)
        raw_event.error.heading = error_heading
        self.raw_event_list.append(raw_event)

    @staticmethod
    def create_date_thread_hash(log_line):
        time = log_line.date
        time_string = str(log_line.thread_id) + (time.strftime("%Y%m%d%H%M%S%f"))
        return int(time_string)

    @staticmethod
    def parse_log_line(self, line):
        line = line.split(" - ")
        if len(line) > 1:
            date = self.parse_date(line[Constants.DATE_INDEX_RAW_LINE])
            thread_number = self.parse_thread(line[Constants.THREAD_INDEX_RAW_LINE])
            message = line[Constants.MESSAGE_INDEX_RAW_LINE]
            if "RUNNING" in line[len(line) - 1]:
                message += " - RUNNING"
            elif "- SUCCESS" in line[len(line) - 1]:
                message += " - SUCCESS"
            elif "- FAILED" in line[len(line) - 1]:
                message += " - FAILED"
            log_type = self.parse_log_type(line[Constants.LOG_TYPE_RAW_LINE])
            log_line = LogLine(self.current_line_number, date, thread_number, message, log_type)
            return log_line

    @staticmethod
    def parse_log_type(line):
        line = line.replace("(", "")
        line = line.replace(")", "")
        line = line.replace(" ", "")
        return line

    @staticmethod
    def parse_r3d3_status(self, line):
        """
        :summary: Parse the r3d3 line to determine if the current app is r3d3 type

        :param self: The current instance of the class
        :param line: The raw text of the current log line
        """
        if "IS_R3D3" in line:
            token_string = line.split("'IS_R3D3':")[1]
            status_string = token_string.split(" ")
            r3d3_status = "False"
            if "True" in status_string[1]:
                r3d3_status = "True"
                self.r3d3_status = True
            self.log_line_list.append(LogLine(
                self.current_line_number, datetime.min, 0, r3d3_status, Constants.R3D3_Status))

    @staticmethod
    def parse_thread(line):
        """
        :summary: attempt to parse the thread number from the current line

        :returns: an integer value representing the current thread number
        or -1 if the thread number is invalid
        """
        if "MainThread" in line:
            return 0
        int_value = ""
        for c in line:
            if c.isdigit():
                int_value += c

        # Return -1 if the thread id is invalid
        if int_value == "":
            return -1
        return int(int_value)

    @staticmethod
    def parse_date(line):
        """
        :summary: attempt to parse a datetime value from the current line

        :param line: The raw text of the current line
        :return: a datetime object or datetime.min if the date is invalid
        """
        line = line.replace("(", "")
        line = line.replace(")", "")
        try:
            line = line[:len(line) - 4]
            date = datetime.strptime(line, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return datetime.min
        return date

    @staticmethod
    def parse_java_log_date(line):
        try:
            time_structure = datetime.strptime(line, "%y/%m/%d %H:%M:%S ")
        except ValueError:
            return datetime.min
        return time_structure

    @staticmethod
    def parse_starting_java_sqoop_date(self, current_tuple):
        for hash_value in self.sqoop_log_dictionary.keys():
            log_entry = self.sqoop_log_dictionary.get(hash_value)
            if log_entry.line_number == current_tuple.line_number:
                lines = log_entry.message.split("\n")
                if len(lines) < 2:
                    self.create_error_as_raw_event(self, Constants.SQOOP_JAVA_LOG, log_entry, "")
                    return datetime.min

                first_line = lines[2]
                split_by_space = first_line.split("INFO ")

                if len(split_by_space) < 2:
                    return datetime.min

                date_line = split_by_space[0]
                return self.parse_java_log_date(date_line)

    @staticmethod
    def parse_ending_java_sqoop_date(self, current_tuple):
        for hash_value in self.sqoop_log_dictionary.keys():
            log_entry = self.sqoop_log_dictionary.get(hash_value)
            if log_entry.line_number == current_tuple.line_number:
                message = log_entry.message
                split_by_retrieved = message.split("Retrieved ")
                if len(split_by_retrieved) < 2:
                    self.create_error_as_raw_event(self, Constants.SQOOP_JAVA_LOG, log_entry, "")
                    return datetime.min

                date_lines = split_by_retrieved[0]
                final_date_line_list = date_lines.split("\n")
                date_line = final_date_line_list[len(final_date_line_list) - 1]
                split_by_space = date_line.split("INFO ")

                if len(split_by_space) < 2:
                    self.create_error_as_raw_event(self, Constants.SQOOP_JAVA_LOG, log_entry, "")
                    return datetime.min

                date_line = split_by_space[0]
                return self.parse_java_log_date(date_line)

    @staticmethod
    def parse_sqoop_bytes(self, log_line):
        data_transferred = 0
        for hash_value in self.sqoop_log_dictionary.keys():
            sqoop_log_entry = self.sqoop_log_dictionary.get(hash_value)
            if sqoop_log_entry.line_number == log_line.line_number:
                if "Transferred" not in sqoop_log_entry.message:
                    self.create_error_as_raw_event(self, Constants.SQOOP_JAVA_LOG, sqoop_log_entry, "")
                    return -1
                else:
                    byte_string = sqoop_log_entry.message.split("Transferred ")[1]
                    if len(byte_string) < 2:
                        self.create_error_as_raw_event(self, Constants.SQOOP_JAVA_LOG, sqoop_log_entry, "")
                        return -1

                    token_list = byte_string.split(" ")
                    if len(token_list) < 2:
                        self.create_error_as_raw_event(self, Constants.SQOOP_JAVA_LOG, sqoop_log_entry, "")
                        return -1

                    token = self.remove_commas_from_byte_number(token_list[0])
                    data_transferred = float(token)

                    # Convert the data_transferred value into bytes before returning
                    if token_list[1] == "KB":
                        data_transferred = data_transferred * 10**3
                    elif token_list[1] == "GB":
                        data_transferred = data_transferred * 10**9
                    elif token_list[1] == "MB":
                        data_transferred = data_transferred * 10**6
        return data_transferred

    @staticmethod
    def parse_workflow_id(self, log_line):
        token_string = log_line.message.split("Workflow Id : ")
        if len(token_string) < 2:
            self.create_error_as_raw_event(self, Constants.SQOOP_JAVA_LOG, log_line, "Error parsing workflow name", "")
            return "Null Workflow"
        token_string = token_string[1].split(" - RUNNING")
        if len(token_string) < 2:
            self.create_error_as_raw_event(self, Constants.SQOOP_JAVA_LOG, log_line, "Error parsing workflow name", "")
            return "Null Workflow"
        return int(token_string[0])

    @staticmethod
    def calculate_number_sqoop_errors(self, current_line):
        errors = 0
        for hash_value in self.sqoop_log_dictionary.keys():
            log_entry = self.sqoop_log_dictionary.get(hash_value)
            if log_entry.line_number == current_line.line_number:
                log_entry.message.split("retrying...\n")
        return errors

    @staticmethod
    def remove_commas_from_byte_number(byte_number):
        byte_number = byte_number.replace(",", "")
        return byte_number

    @staticmethod
    def parse_run_id(current_line):
        app_name = current_line.message.replace("\n", "")
        app_name = app_name.split(" :")
        return int(app_name[1])

    @staticmethod
    def parse_rows_pulled(current_message):
        message = current_message.split("rows")
        rows = ""
        for char in message[0]:
            if char.isdigit():
                rows += char
        return int(rows)

    @staticmethod
    def handle_starting_date(self, current_line):
        raw_event = RawEvent(current_line.line_number, current_line.date, current_line.thread_id,
                             Constants.START, current_line.message, 0, 0, True)
        self.raw_event_list.append(raw_event)

    @staticmethod
    def handle_ending_date(self, current_line):
        raw_event = RawEvent(current_line.line_number, current_line.date, current_line.thread_id,
                             Constants.END, current_line.message, 0, 0, False)
        self.raw_event_list.append(raw_event)

    @staticmethod
    def handle_generated_app_id(self, current_line):
        current_line.message = self.parse_run_id(current_line)
        raw_event = RawEvent(current_line.line_number, current_line.date, current_line.thread_id, Constants.RUN_ID,
                             current_line.message, 0, 0, True)
        self.raw_event_list.append(raw_event)

    @staticmethod
    def handle_r3d3_import(self, current_line):
        raw_event = RawEvent(current_line.line_number, current_line.date, current_line.thread_id, Constants.IMPORT,
                             current_line.message, 0, 0, True)
        self.raw_event_list.append(raw_event)

    @staticmethod
    def handle_ingesting(self, current_line):

        # Create and add a new import event
        if self.r3d3_status is False:
            raw_event = RawEvent(current_line.line_number, current_line.date, current_line.thread_id, Constants.IMPORT,
                                 current_line.message, 0, 0, True)
            self.raw_event_list.append(raw_event)

        # Create an add a new INGESTING_WORKFLOW event
        raw_event = RawEvent(current_line.line_number, current_line.date, current_line.thread_id,
                             Constants.INGESTING_WORKFLOW, current_line.message, 0, 0, False)
        self.raw_event_list.append(raw_event)

    @staticmethod
    def handle_running_workflow(self, current_line):
        raw_event = RawEvent(current_line.line_number, current_line.date, current_line.thread_id,
                             Constants.RUNNING_WORKFLOW, current_line.message, 0, 0, False)
        self.raw_event_list.append(raw_event)

    @staticmethod
    def handle_running_sqoop_version(self, current_line):
        bytes_transferred = self.parse_sqoop_bytes(self, current_line)
        sqoop_errors = self.calculate_number_sqoop_errors(self, current_line)
        sqoop_starting_date = self.parse_starting_java_sqoop_date(self, current_line)
        sqoop_ending_date = self.parse_ending_java_sqoop_date(self, current_line)

        # Create RawEvent objects
        sqoop_log_starting_event = RawEvent(current_line.line_number, sqoop_starting_date, current_line.thread_id,
                                            Constants.SQOOP_JAVA_LOG, "msg", 0, bytes_transferred, True)
        sqoop_log_ending_event = RawEvent(current_line.line_number, sqoop_ending_date, current_line.thread_id,
                                          Constants.SQOOP_JAVA_LOG, "msg", 0, bytes_transferred, False)
        sqoop_log_ending_event.errors = sqoop_errors

        # Add the RawEvent objects to the current list
        self.raw_event_list.append(sqoop_log_starting_event)
        self.raw_event_list.append(sqoop_log_ending_event)

    @staticmethod
    def handle_successfully_pulled(self, current_line):
        rows = self.parse_rows_pulled(current_line.message)
        raw_event = RawEvent(current_line.line_number, current_line.date, current_line.thread_id, Constants.IMPORT,
                             current_line.message, 0, 0, False, rows)
        self.raw_event_list.append(raw_event)

    @staticmethod
    def handle_starting_table_transform(self, current_line):
        raw_event = RawEvent(current_line.line_number, current_line.date, current_line.thread_id, Constants.TRANSFORM,
                             current_line.message, 0, 0, True)
        self.raw_event_list.append(raw_event)

    @staticmethod
    def handle_transform_completed(self, current_line):
        raw_event = RawEvent(current_line.line_number, current_line.date, current_line.thread_id, Constants.TRANSFORM,
                             current_line.message, 0, 0, False)
        self.raw_event_list.append(raw_event)

    @staticmethod
    def handle_skip_transform(self, current_line):
        raw_event = RawEvent(current_line.line_number, current_line.date, current_line.thread_id,
                             Constants.SKIP_TRANSFORMATION, current_line.message, 0, 0, False)
        self.raw_event_list.append(raw_event)

    @staticmethod
    def handle_starting_merge(self, current_line):
        raw_event = RawEvent(current_line.line_number, current_line.date, current_line.thread_id, Constants.MERGE,
                             current_line.message, 0, 0, True)
        self.raw_event_list.append(raw_event)

    @staticmethod
    def handle_run_succeeded(self, current_line):
        raw_event = RawEvent(current_line.line_number, current_line.date, current_line.thread_id, Constants.MERGE,
                             current_line.message, 0, 0, False)
        self.raw_event_list.append(raw_event)

    @staticmethod
    def handle_validating(self, current_line):
        raw_event = RawEvent(current_line.line_number, current_line.date, current_line.thread_id, Constants.VALIDATE,
                             current_line.message, 0, 0, True)
        self.raw_event_list.append(raw_event)

    @staticmethod
    def handle_done_validating(self, current_line):
        raw_event = RawEvent(current_line.line_number, current_line.date, current_line.thread_id, Constants.VALIDATE,
                             current_line.message, 0, 0, False)
        self.raw_event_list.append(raw_event)

    @staticmethod
    def handle_skip_validating(self, current_line):
        raw_event = RawEvent(current_line.line_number, current_line.date, current_line.thread_id,
                             Constants.SKIP_VALIDATION,
                             current_line.message, 0, 0, False)
        self.raw_event_list.append(raw_event)

    @staticmethod
    def handle_r3d3_status(self, current_line):
        raw_event = RawEvent(current_line.line_number, current_line.date, current_line.thread_id, Constants.R3D3_Status,
                             current_line.message, 0, 0, False)
        self.raw_event_list.append(raw_event)

    @staticmethod
    def handle_final_status(self, current_line):
        raw_event = RawEvent(current_line.line_number, current_line.date, current_line.thread_id,
                             Constants.FINAL_STATUS, current_line.message, 0, 0, False)
        self.raw_event_list.append(raw_event)

    @staticmethod
    def handle_success_workflow(self, current_line):
        raw_event = RawEvent(current_line.line_number, current_line.date, current_line.thread_id, Constants.SUCCESS,
                             current_line.message, 0, 0, False)
        self.raw_event_list.append(raw_event)

    @staticmethod
    def handle_failed_workflow(self, current_line):
        raw_event = RawEvent(current_line.line_number, current_line.date, current_line.thread_id, Constants.FAILED,
                             current_line.message, 0, 0, False)
        self.raw_event_list.append(raw_event)

    @staticmethod
    def produce_stats(self):
        for current_line in self.log_line_list:
            message = current_line.message

            if self.r3d3_status and ("Executing query:" in message or "Executing query:" in message):
                if "import" in message or "IMPORT" in message or "Import" in message:
                    self.handle_r3d3_import(self, current_line)

            elif "Generated Run id" in message:
                self.handle_generated_app_id(self, current_line)

            elif " - RUNNING" in message:
                self.handle_running_workflow(self, current_line)

            elif "Ingesting" in message:
                self.handle_ingesting(self, current_line)

            elif "Running Sqoop version" in message and "WARNING" in current_line.log_type:
                self.handle_running_sqoop_version(self, current_line)

            elif "Successfully pulled" in message:
                self.handle_successfully_pulled(self, current_line)

            elif "Transforming imported table" in message:
                self.handle_starting_table_transform(self, current_line)

            elif "Transform completed" in message:
                self.handle_transform_completed(self, current_line)

            elif "Skipping table transformation" in message:
                self.handle_skip_transform(self, current_line)

            elif "Starting merge" in message:
                self.handle_starting_merge(self, current_line)

            elif "Run succeeded" in message or "Unexpected error:" in message or "Run failed" in message:
                self.handle_run_succeeded(self, current_line)

            elif "Validating imported table" in message:
                self.handle_validating(self, current_line)

            elif "EIN:" in message:
                self.handle_done_validating(self, current_line)

            elif "skipping validation" in message:
                self.handle_skip_validating(self, current_line)

            elif "[FINAL STATUS]" in message:
                self.handle_final_status(self, current_line)

            elif current_line.log_type == Constants.R3D3_Status:
                self.handle_r3d3_status(self, current_line)

            elif " - SUCCESS" in message:
                self.handle_success_workflow(self, current_line)

            elif " - FAILED" in message:
                self.handle_failed_workflow(self, current_line)
        self.handle_ending_date(self, self.log_line_list[len(self.log_line_list) - 1])
