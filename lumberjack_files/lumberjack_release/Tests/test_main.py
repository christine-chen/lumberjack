from .. Backend.Parsers.FileParser import FileParser
from .. Backend.Parsers.KobeParser import KobeParser
from unittest import TestCase
import os


class TestMain(TestCase):

    # The current FileParser instance
    file_parser = None

    # The current KobeParser instance
    kobe_parser = None

    # The first App object representing a single ingestion run
    app1 = None

    # The second App object representing a single ingestion run
    app2 = None

    # The third App object representing a single ingestion run
    app3 = None

    def setUp(self):

        print "CURRENT"
        print "CURRENT FILES: " + str(os.listdir(os.curdir))
        # Generate app1 from failures6.log
        self.file_parser = FileParser()
        raw_events_list = self.file_parser.run("lumberjack_release/Backend/Logs/failures6.log")
        self.kobe_parser = KobeParser()
        self.app1 = self.kobe_parser.run(raw_events_list)

        # Generate app2 from failures2.log
        self.file_parser = FileParser()
        raw_events_list = self.file_parser.run("lumberjack_release/Backend/Logs/failures2.log")
        self.kobe_parser = KobeParser()
        self.app2 = self.kobe_parser.run(raw_events_list)

        # Generate app3 from failures3.log (r3d3 type)
        self.file_parser = FileParser()
        raw_events_list = self.file_parser.run("lumberjack_release/Backend/Logs/failures3.log")
        self.kobe_parser = KobeParser()
        self.app3 = self.kobe_parser.run(raw_events_list)

    def test_app_info_parsed_correctly(self):
        print "============================"
        print "APP id: " + str(self.app1.id)
        print "APP name: " + str(self.app1.name)
        print "APP start_date: " + str(self.app1.start_date)
        print "APP end_date: " + str(self.app1.end_date)
        print "APP bytes_transferred: " + str(self.app1.bytes_transferred)
        print "APP total_duration: " + str(self.app1.total_duration)
        print "APP import_duration: " + str(self.app1.import_duration)
        print "APP total_ingestion_time" + str(self.app1.total_ingestion_time)
        print "APP transform_duration: " + str(self.app1.transform_duration)
        print "APP validation_duration: " + str(self.app1.validation_duration)
        print "APP merge_duration: " + str(self.app1.merge_duration)
        print "APP rows_pulled: " + str(self.app1.rows_pulled)
        print "APP log_file_name: " + str(self.app1.log_file_name)
        print "APP successful_workflows_list Count: " + str(len(self.app1.successful_workflows_list))
        print "APP failed_workflows_list: " + str(len(self.app1.failed_workflows_list))
        print "APP errors_list Count: " + str(len(self.app1.errors_list))
        print "APP workflow_dict Count: " + str(len(self.app1.workflow_dict))
        print "APP are_errors_in_workflows: " + str(self.app1.are_errors_in_workflows)

        # Verify that the metadata for app1 (failures6.log) has been parsed properly
        self.assertEqual(self.app1.id, "1050590", "Verify that the app id is correct")
        self.assertEqual(self.app1.name, "qbogbl_r3d3", "Verify that the app name is correct")
        self.assertEqual(str(self.app1.start_date), "2017-06-09 11:03:59", "Verify that the app start_date is correct")
        self.assertEqual(str(self.app1.end_date), "2017-06-09 11:28:09", "Verify that the app end_date is correct")
        self.assertEqual(self.app1.bytes_transferred, 0, "Verify that the app bytes_transferred is correct")
        self.assertEqual(self.app1.total_ingestion_time, 1450, "Verify that the app total_ingestion_time is correct")
        self.assertEqual(self.app1.total_duration, 4440, "Verify that the app total_duration is correct")
        self.assertEqual(self.app1.import_duration, 1362, "Verify that the app import_duration is correct")
        self.assertEqual(self.app1.transform_duration, 2953, "Verify that the app import_duration is correct")
        self.assertEqual(self.app1.validation_duration, 0, "Verify that the app validation_duration is correct")
        self.assertEqual(self.app1.merge_duration, 125, "Verify that the app merge_duration is correct")
        self.assertEqual(self.app1.rows_pulled, 5767962411, "Verify that the app rows_pulled is correct")
        self.assertEqual(self.app1.log_file_name, "failures6.log", "Verify that the app's log_file_name is correct")
        self.assertEqual(self.app1.log_file_path, "lumberjack_release/Backend/Logs/", "Verify that the app's log_file_path is correct")
        self.assertEqual(len(self.app1.successful_workflows_list), 2, "Verify that the app success_count is correct")
        self.assertEqual(len(self.app1.failed_workflows_list), 2, "Verify that the app failure_count is correct")
        self.assertEqual(len(self.app1.errors_list), 0, "Verify that the app errors_list count is correct")
        self.assertEqual(len(self.app1.workflow_dict.keys()), 4, "Verify the workflow_dict count is correct")
        self.assertEqual(self.app1.are_errors_in_workflows, True, "Verify that are_errors_in_workflows is correct")

        print "APP2 id: " + str(self.app2.id)
        print "APP2 name: " + str(self.app2.name)
        print "APP2 start_date: " + str(self.app2.start_date)
        print "APP2 end_date: " + str(self.app2.end_date)
        print "APP2 bytes_transferred: " + str(self.app2.bytes_transferred)
        print "APP2 total_duration: " + str(self.app2.total_duration)
        print "APP2 import_duration: " + str(self.app2.import_duration)
        print "APP2 total_ingestion_time: " + str(self.app2.total_ingestion_time)
        print "APP2 transform_duration: " + str(self.app2.transform_duration)
        print "APP2 validation_duration: " + str(self.app2.validation_duration)
        print "APP2 merge_duration: " + str(self.app2.merge_duration)
        print "APP2 rows_pulled: " + str(self.app2.rows_pulled)
        print "APP2 log_file_name: " + str(self.app2.log_file_name)
        print "APP2 successful_workflows_list Count: " + str(len(self.app2.successful_workflows_list))
        print "APP2 failed_workflows_list: " + str(len(self.app2.failed_workflows_list))
        print "APP2 errors_list Count: " + str(len(self.app2.errors_list))
        print "APP2 successful_workflows_list Count: " + str(len(self.app2.successful_workflows_list))
        print "APP2 failed_workflows_list Count: " + str(len(self.app2.failed_workflows_list))
        print "APP2 workflow_dict Count: " + str(len(self.app2.workflow_dict))
        print "APP2 are_errors_in_workflows: " + str(self.app2.are_errors_in_workflows)

        # Verify that the metadata for app2 (failures2.log) has been parsed properly
        self.assertEqual(self.app2.id, "1051678", "Verify that the app id is correct for app2")
        self.assertEqual(self.app2.name, "sonora", "Verify that the app name is correct for app2")
        self.assertEqual(str(self.app2.start_date), "2017-06-14 12:44:27",
                         "Verify that the app start_date is correct for app2")
        self.assertEqual(str(self.app2.end_date), "2017-06-14 15:37:48",
                         "Verify that the app end_date is correct for app2")
        self.assertEqual(str(self.app2.bytes_transferred), "55459532466.8",
                         "Verify that the app bytes_transferred is correct for app2")
        self.assertEqual(self.app2.total_duration, 59371, "Verify that the app total_duration is correct for app2")
        self.assertEqual(self.app2.import_duration, 30599, "Verify that the app import_duration is correct for app2")
        self.assertEqual(self.app2.transform_duration, 5903,
                         "Verify that the app transform_duration is correct for app2")
        self.assertEqual(self.app2.total_ingestion_time, 10401,
                         "Verify that the app total_ingestion_time is correct for app2")
        self.assertEqual(self.app2.validation_duration, 1447,
                         "Verify that the app validation_duration is correct for app2")
        self.assertEqual(self.app2.merge_duration, 22869, "Verify that the app merge_duration is correct for app2")
        self.assertEqual(self.app2.rows_pulled, 343922637, "Verify that the app rows_pulled is correct for app2")
        self.assertEqual(self.app2.log_file_name, "failures2.log",
                         "Verify that the app's log_file_name is correct for app2")
        self.assertEqual(len(self.app2.successful_workflows_list), 173,
                         "Verify that the app success_count is correct for app2")
        self.assertEqual(len(self.app2.failed_workflows_list), 3,
                         "Verify that the app failure_count is correct for app2")
        self.assertEqual(len(self.app2.errors_list), 0, "Verify that the app errors_list count is correct for app2")
        self.assertEqual(len(self.app2.workflow_dict.keys()), 176, "Verify the workflow_dict count is correct for app2")
        self.assertEqual(self.app2.are_errors_in_workflows, True,
                         "Verify that are_errors_in_workflows is correct for app2")

        print "APP3 id: " + str(self.app3.id)
        print "APP3 name: " + str(self.app3.name)
        print "APP3 start_date: " + str(self.app3.start_date)
        print "APP3 end_date: " + str(self.app3.end_date)
        print "APP3 bytes_transferred: " + str(self.app3.bytes_transferred)
        print "APP3 total_duration: " + str(self.app3.total_duration)
        print "APP3 import_duration: " + str(self.app3.import_duration)
        print "APP3 total_ingestion_time" + str(self.app3.total_ingestion_time)
        print "APP3 transform_duration: " + str(self.app3.transform_duration)
        print "APP3 validation_duration: " + str(self.app3.validation_duration)
        print "APP3 merge_duration: " + str(self.app3.merge_duration)
        print "APP3 rows_pulled: " + str(self.app3.rows_pulled)
        print "APP3 log_file_name: " + str(self.app3.log_file_name)
        print "APP3 successful_workflows_list Count: " + str(len(self.app3.successful_workflows_list))
        print "APP3 failed_workflows_list: " + str(len(self.app3.failed_workflows_list))
        print "APP3 errors_list Count: " + str(len(self.app3.errors_list))
        print "APP3 successful_workflows_list Count: " + str(len(self.app3.successful_workflows_list))
        print "APP3 failed_workflows_list Count: " + str(len(self.app3.failed_workflows_list))
        print "APP3 workflow_dict Count: " + str(len(self.app3.workflow_dict))
        print "APP3 are_errors_in_workflows: " + str(self.app3.are_errors_in_workflows)

        # Verify that the metadata for app3 (failures3.log) has been parsed properly
        self.assertEqual(self.app3.id, "1050399", "Verify that the app id is correct for app3")
        self.assertEqual(self.app3.name, "qbogbl_r3d3", "Verify that the app name is correct for app3")
        self.assertEqual(str(self.app3.start_date), "2017-06-08 16:37:19",
                         "Verify that the app start_date is correct for app3")
        self.assertEqual(str(self.app3.end_date), "2017-06-08 16:40:06",
                         "Verify that the app end_date is correct for app3")
        self.assertEqual(self.app3.bytes_transferred, 0, "Verify that the app bytes_transferred is correct for app3")
        self.assertEqual(self.app3.total_duration, 404, "Verify that the app total_duration is correct for app3")
        self.assertEqual(self.app3.import_duration, 212, "Verify that the app import_duration is correct for app3")
        self.assertEqual(self.app3.transform_duration, 188, "Verify that the app import_duration is correct for app3")
        self.assertEqual(self.app3.validation_duration, 0,
                         "Verify that the app validation_duration is correct for app3")
        self.assertEqual(self.app3.merge_duration, 4, "Verify that the app merge_duration is correct for app3")
        self.assertEqual(self.app3.rows_pulled, 3719363, "Verify that the app rows_pulled is correct for app3")
        self.assertEqual(self.app3.log_file_name, "failures3.log",
                         "Verify that the app's log_file_name is correct for app3")
        self.assertEqual(len(self.app3.successful_workflows_list), 1,
                         "Verify that the app success_count is correct for app3")
        self.assertEqual(len(self.app3.failed_workflows_list), 1,
                         "Verify that the app failure_count is correct for app3")
        self.assertEqual(len(self.app3.errors_list), 0, "Verify that the app errors_list count is correct for app3")
        self.assertEqual(len(self.app3.workflow_dict.keys()), 2, "Verify the workflow_dict count is correct for app3")
        self.assertEqual(self.app3.are_errors_in_workflows, True,
                         "Verify that are_errors_in_workflows is correct for app3")

    def test_workflow_info_parsed_correctly(self):

        # Fetch a workflow from app1 (failures6.log)
        key1 = self.app1.workflow_dict.keys()[1]
        workflow1 = self.app1.workflow_dict.get(key1)

        # Print workflow info
        print "WORKFLOW name: " + str(workflow1.name)
        print "WORKFLOW date: " + str(workflow1.date)
        print "WORKFLOW total_duration: " + str(workflow1.total_duration)
        print "WORKFLOW import_duration: " + str(workflow1.import_duration)
        print "WORKFLOW transform_duration: " + str(workflow1.transform_duration)
        print "WORKFLOW validation_duration: " + str(workflow1.validation_duration)
        print "WORKFLOW merge_duration: " + str(workflow1.merge_duration)
        print "WORKFLOW bytes_transferred: " + str(workflow1.bytes_transferred)
        print "WORKFLOW rows_pulled: " + str(workflow1.rows_pulled)
        print "WORKFLOW operations_list Count: " + str(len(workflow1.operations_list))
        print "WORKFLOW errors_list Count: " + str(len(workflow1.errors_list))
        print "WORKFLOW is_r3d3: " + str(workflow1.is_r3d3)
        print "WORKFLOW application_name: " + str(workflow1.application_name)
        print "WORKFLOW log_file_name: " + str(workflow1.log_file_name)
        print "WORKFLOW log_file_path: " + str(workflow1.log_file_path)

        # Verify that the info for the workflow has been properly parsed
        self.assertEqual(workflow1.name, "TXDETAILS_VW", "Verify that workflow name is correct")
        self.assertEqual(str(workflow1.date), "2017-06-09 11:03:59", "Verify that workflow date is correct")
        self.assertEqual(workflow1.total_duration, 3402, "Verify that workflow total_duration is correct")
        self.assertEqual(workflow1.import_duration, 894.0, "Verify that workflow import_duration is correct")
        self.assertEqual(workflow1.transform_duration, 2507.0, "Verify that workflow transform_duration is correct")
        self.assertEqual(workflow1.validation_duration, 0, "Verify that workflow validation_duration is correct")
        self.assertEqual(workflow1.merge_duration, 1.0, "Verify that workflow merge_duration is correct")
        self.assertEqual(workflow1.bytes_transferred, 0, "Verify that workflow bytes_transferred is correct")
        self.assertEqual(workflow1.rows_pulled, 5431275618, "Verify that workflow rows_pulled is correct")
        self.assertEqual(len(workflow1.operations_list), 26, "Verify that the workflow has the correct operation count")
        self.assertEqual(len(workflow1.errors_list), 2, "Verify that the workflow has the correct error count")
        self.assertEqual(workflow1.is_r3d3, False, "Verify that workflow is_r3d3 value is correct")
        self.assertEqual(workflow1.log_file_name, "failures6.log", "Verify that workflow log_file_name is correct")
        self.assertEqual(workflow1.log_file_path, "lumberjack_release/Backend/Logs/",
                         "Verify that workflow log_file_path is correct")

        # Fetch a workflow from app2 (failures2.log)
        key2 = self.app2.workflow_dict.keys()[1]
        workflow2 = self.app2.workflow_dict.get(key2)

        # Print workflow2 info
        print "WORKFLOW2 name: " + str(workflow2.name)
        print "WORKFLOW2 date: " + str(workflow2.date)
        print "WORKFLOW2 total_duration: " + str(workflow2.total_duration)
        print "WORKFLOW2 import_duration: " + str(workflow2.import_duration)
        print "WORKFLOW2 transform_duration: " + str(workflow2.transform_duration)
        print "WORKFLOW2 validation_duration: " + str(workflow2.validation_duration)
        print "WORKFLOW2 merge_duration: " + str(workflow2.merge_duration)
        print "WORKFLOW2 bytes_transferred: " + str(workflow2.bytes_transferred)
        print "WORKFLOW2 rows_pulled: " + str(workflow2.rows_pulled)
        print "WORKFLOW2 operations_list Count: " + str(len(workflow2.operations_list))
        print "WORKFLOW2 errors_list Count: " + str(len(workflow2.errors_list))
        print "WORKFLOW2 is_r3d3: " + str(workflow2.is_r3d3)
        print "WORKFLOW2 application_name: " + str(workflow2.application_name)

        # Verify that the info for the workflow has been properly parsed
        self.assertEqual(workflow2.name, "FD_BASE_FILTER", "Verify that workflow2 name is correct")
        self.assertEqual(str(workflow2.date), "2017-06-14 12:44:27", "Verify that workflow2 date is correct")
        self.assertEqual(workflow2.total_duration, 174, "Verify that workflow2 total_duration is correct")
        self.assertEqual(workflow2.import_duration, 74.0, "Verify that workflow2 import_duration is correct")
        self.assertEqual(workflow2.transform_duration, 18.0, "Verify that workflow2 transform_duration is correct")
        self.assertEqual(workflow2.validation_duration, 0, "Verify that workflow2 validation_duration is correct")
        self.assertEqual(workflow2.merge_duration, 82.0, "Verify that workflow2 merge_duration is correct")
        self.assertEqual(workflow2.bytes_transferred, 5041.0, "Verify that workflow2 bytes_transferred is correct")
        self.assertEqual(workflow2.rows_pulled, 41, "Verify that workflow2 rows_pulled is correct")
        self.assertEqual(len(workflow2.operations_list), 5, "Verify that the workflow2 has the correct operation count")
        self.assertEqual(len(workflow2.errors_list), 0, "Verify that the workflow2 has the correct error count")
        self.assertEqual(workflow2.is_r3d3, False, "Verify that workflow2's 'is_r3d3' value is correct")
        self.assertEqual(workflow2.application_name, "sonora", "Verify that workflow2 application_name is correct")

        # Fetch a workflow from app3 (failures2.log)
        key3 = self.app2.workflow_dict.keys()[1]
        workflow3 = self.app2.workflow_dict.get(key3)

        # Print workflow3 info
        print "WORKFLOW3 name: " + str(workflow3.name)
        print "WORKFLOW3 date: " + str(workflow3.date)
        print "WORKFLOW3 total_duration: " + str(workflow3.total_duration)
        print "WORKFLOW3 import_duration: " + str(workflow3.import_duration)
        print "WORKFLOW3 transform_duration: " + str(workflow3.transform_duration)
        print "WORKFLOW3 validation_duration: " + str(workflow3.validation_duration)
        print "WORKFLOW3 merge_duration: " + str(workflow3.merge_duration)
        print "WORKFLOW3 bytes_transferred: " + str(workflow3.bytes_transferred)
        print "WORKFLOW3 rows_pulled: " + str(workflow3.rows_pulled)
        print "WORKFLOW3 operations_list Count: " + str(len(workflow3.operations_list))
        print "WORKFLOW3 errors_list Count: " + str(len(workflow3.errors_list))
        print "WORKFLOW3 is_r3d3: " + str(workflow3.is_r3d3)
        print "WORKFLOW3 application_name: " + str(workflow3.application_name)

        # Verify that the info for workflow2 has been properly parsed
        self.assertEqual(workflow3.name, "FD_BASE_FILTER", "Verify that workflow3 name is correct")
        self.assertEqual(str(workflow3.date), "2017-06-14 12:44:27", "Verify that workflow3 date is correct")
        self.assertEqual(workflow3.total_duration, 174, "Verify that workflow3 total_duration is correct")
        self.assertEqual(workflow3.import_duration, 74.0, "Verify that workflow3 import_duration is correct")
        self.assertEqual(workflow3.transform_duration, 18.0, "Verify that workflow3 transform_duration is correct")
        self.assertEqual(workflow3.validation_duration, 0, "Verify that workflow3 validation_duration is correct")
        self.assertEqual(workflow3.merge_duration, 82.0, "Verify that workflow3 merge_duration is correct")
        self.assertEqual(workflow3.bytes_transferred, 5041.0, "Verify that workflow3 bytes_transferred is correct")
        self.assertEqual(workflow3.rows_pulled, 41, "Verify that workflow3 rows_pulled is correct")
        self.assertEqual(len(workflow3.operations_list), 5,
                         "Verify that the workflow3 has the correct operation count")
        self.assertEqual(len(workflow3.errors_list), 0, "Verify that the workflow3 has the correct error count")
        self.assertEqual(workflow3.is_r3d3, False, "Verify that workflow3's 'is_r3d3' value is correct")
        self.assertEqual(workflow3.application_name, "sonora", "Verify that workflow3 application_name is correct")

    def test_operation_info_parsed_correctly(self):

        # Fetch an operation from app1 (failures6.log)
        key1 = self.app1.workflow_dict.keys()[1]
        workflow1 = self.app1.workflow_dict.get(key1)
        operation1 = workflow1.operations_list[4]

        # Print operation info
        print "OPERATION error_link_id: " + str(operation1.error_link_id)
        print "OPERATION duration: " + str(operation1.duration)
        print "OPERATION start_line_number: " + str(operation1.start_line_number)
        print "OPERATION end_line_number: " + str(operation1.end_line_number)
        print "OPERATION thread_id: " + str(operation1.thread_id)
        print "OPERATION operation_type: " + str(operation1.operation_type)
        print "OPERATION bytes_transferred: " + str(operation1.bytes_transferred)
        print "OPERATION workflow_name: " + str(operation1.workflow_name)
        print "OPERATION message: " + str(operation1.message)
        print "OPERATION completed: " + str(operation1.completed)
        print "OPERATION errors_list Count: " + str(len(operation1.errors_list))

        # Verify that the info for operation1 has been properly parsed
        self.assertEqual(operation1.duration, 73.0, "Verify that operation duration is correct")
        self.assertEqual(operation1.error_link_id, "qbogbl_r3d34322", "Verify that operation error_link_id is correct")
        self.assertEqual(operation1.start_line_number, 4322, "Verify that operation start_line_number is correct")
        self.assertEqual(operation1.end_line_number, 4324, "Verify that operation end_line_number is correct")
        self.assertEqual(operation1.thread_id, 10, "Verify that operation thread_id is correct")
        self.assertEqual(operation1.operation_type, "Import", "Verify that operation_type duration is correct")
        self.assertEqual(operation1.bytes_transferred, 0, "Verify that operation bytes_transferred is correct")
        self.assertTrue("Successfully pulled 202311277" in operation1.message,
                        "Verify that operation message is correct")
        self.assertEqual(operation1.workflow_name, "TXDETAILS_VW",
                         "Verify that the operation's workflow_name is correct")
        self.assertEqual(operation1.completed, True, "Verify that operation completed status is correct")
        self.assertEqual(len(operation1.errors_list), 0, "Verify that operation errors_list count is correct")

        # Fetch an operation from app2 (failures2.log)
        key2 = self.app2.workflow_dict.keys()[1]
        workflow2 = self.app2.workflow_dict.get(key2)
        operation2 = workflow2.operations_list[4]

        # Print operation info
        print "OPERATION2 error_link_id: " + str(operation2.error_link_id)
        print "OPERATION2 duration: " + str(operation2.duration)
        print "OPERATION2 start_line_number: " + str(operation2.start_line_number)
        print "OPERATION2 end_line_number: " + str(operation2.end_line_number)
        print "OPERATION2 thread_id: " + str(operation2.thread_id)
        print "OPERATION2 operation_type: " + str(operation2.operation_type)
        print "OPERATION2 bytes_transferred: " + str(operation2.bytes_transferred)
        print "OPERATION2 workflow_name: " + str(operation2.workflow_name)
        print "OPERATION2 message: " + str(operation2.message)
        print "OPERATION2 completed: " + str(operation2.completed)
        print "OPERATION2 errors_list Count: " + str(len(operation2.errors_list))

        # Verify that the info for the operation has been properly parsed
        self.assertEqual(operation2.duration, 82.0, "Verify that operation2 duration is correct")
        self.assertEqual(operation2.error_link_id, "sonora1968", "Verify that operation2 error_link_id is correct")
        self.assertEqual(operation2.start_line_number, 1968, "Verify that operation2 start_line_number is correct")
        self.assertEqual(operation2.end_line_number, 2509, "Verify that operation2 end_line_number is correct")
        self.assertEqual(operation2.thread_id, 1, "Verify that operation2 thread_id is correct")
        self.assertEqual(operation2.operation_type, "Merge",
                         "Verify that operation_type duration is correct for operation2")
        self.assertEqual(operation2.bytes_transferred, 0,
                         "Verify that operation bytes_transferred is correct for operation2")
        self.assertTrue("Run succeeded" in operation2.message, "Verify that operation2 message is correct")
        self.assertEqual(operation2.workflow_name, "FD_BASE_FILTER",
                         "Verify that the operation2's workflow_name is correct")
        self.assertEqual(operation2.completed, True, "Verify that operation2 completed status is correct")
        self.assertEqual(len(operation2.errors_list), 0, "Verify that operation2 errors_list count is correct")

        # Fetch an operation from app3 (failures3.log)
        key3 = self.app3.workflow_dict.keys()[1]
        workflow3 = self.app3.workflow_dict.get(key3)
        operation3 = workflow3.operations_list[4]

        # Print operation3 info
        print "OPERATION3 error_link_id: " + str(operation3.error_link_id)
        print "OPERATION3 duration: " + str(operation3.duration)
        print "OPERATION3 start_line_number: " + str(operation3.start_line_number)
        print "OPERATION3 end_line_number: " + str(operation3.end_line_number)
        print "OPERATION3 thread_id: " + str(operation3.thread_id)
        print "OPERATION3 operation_type: " + str(operation3.operation_type)
        print "OPERATION3 bytes_transferred: " + str(operation3.bytes_transferred)
        print "OPERATION3 workflow_name: " + str(operation3.workflow_name)
        print "OPERATION3 message: " + str(operation3.message)
        print "OPERATION3 completed: " + str(operation3.completed)
        print "OPERATION3 errors_list Count: " + str(len(operation3.errors_list))

        # Verify that the info for the operation3 has been properly parsed
        self.assertEqual(operation3.duration, 29.0, "Verify that operation3 duration is correct")
        self.assertEqual(operation3.error_link_id, "qbogbl_r3d3776", "Verify that operation3 error_link_id is correct")
        self.assertEqual(operation3.start_line_number, 776, "Verify that operation3 start_line_number is correct")
        self.assertEqual(operation3.end_line_number, 875, "Verify that operation3 end_line_number is correct")
        self.assertEqual(operation3.thread_id, 14, "Verify that operation3 thread_id is correct")
        self.assertEqual(operation3.operation_type, "Import",
                         "Verify that operation_type duration is correct for operation3")
        self.assertEqual(operation3.bytes_transferred, 0,
                         "Verify that operation bytes_transferred is correct for operation3")
        self.assertTrue("pulled 58865" in operation3.message, "Verify that operation3 message is correct")
        self.assertEqual(operation3.workflow_name, "ADDRESSES_VW",
                         "Verify that the operation3's workflow_name is correct")
        self.assertEqual(operation3.completed, True, "Verify that operation3 completed status is correct")
        self.assertEqual(len(operation3.errors_list), 0, "Verify that operation3 errors_list count is correct")

    def test_error_info_parsed_correctly(self):

        # Fetch the first error from app1 (failures6.log)
        key1 = self.app1.workflow_dict.keys()[1]
        workflow1 = self.app1.workflow_dict.get(key1)
        error1 = workflow1.errors_list[0]

        # Print error info
        print "ERROR operation_link_id: " + str(error1.operation_link_id)
        print "ERROR operation_type: " + str(error1.operation_type)
        print "ERROR line_number: " + str(error1.line_number)
        print "ERROR thread_id: " + str(error1.thread_id)
        print "ERROR heading: " + str(error1.heading)
        print "ERROR date: " + str(error1.date)
        print "ERROR traceback: " + str(error1.traceback)
        print "ERROR workflow_name: " + str(error1.workflow_name)
        print "ERROR is_error_heading_assigned: " + str(error1.is_error_heading_assigned)

        # Verify that the info for error1 has been properly parsed
        self.assertEqual(error1.operation_link_id, "qbogbl_r3d34740", "Verify that error operation_link_id is correct")
        self.assertEqual(error1.operation_type, "Error", "Verify that error operation_type is correct")
        self.assertEqual(error1.line_number, 5084, "Verify that error line_number is correct")
        self.assertEqual(error1.thread_id, 1, "Verify that error thread_id is correct")
        self.assertTrue("HiveServer2Error" in error1.heading, "Verify that error heading is correct")
        self.assertEqual(str(error1.date), "2017-06-09 11:28:09", "Verify that error date is correct")
        self.assertTrue("line 590, in err_if_rpc_not_ok" in error1.traceback, "Verify that error traceback is correct")
        self.assertEqual(error1.workflow_name, "TXDETAILS_VW", "Verify that error workflow_name is correct")
        self.assertEqual(error1.is_error_heading_assigned, False, "Verify that is_error_heading_assigned is correct")

        # Fetch an error from app2 (failures2.log)
        key2 = self.app2.workflow_dict.keys()[0]
        workflow2 = self.app2.workflow_dict.get(key2)
        error2 = workflow2.errors_list[0]

        # Print error2 info
        print "ERROR2 operation_link_id: " + str(error2.operation_link_id)
        print "ERROR2 operation_type: " + str(error2.operation_type)
        print "ERROR2 line_number: " + str(error2.line_number)
        print "ERROR2 thread_id: " + str(error2.thread_id)
        print "ERROR2 heading: " + str(error2.heading)
        print "ERROR2 date: " + str(error2.date)
        print "ERROR2 traceback: " + str(error2.traceback)
        print "ERROR2 workflow_name: " + str(error2.workflow_name)
        print "ERROR2 is_error_heading_assigned: " + str(error2.is_error_heading_assigned)

        # Verify that the info for the error2 has been properly parsed
        self.assertEqual(error2.operation_link_id, None, "Verify that error2 operation_link_id is correct")
        self.assertEqual(error2.operation_type, "Error", "Verify that error2 operation_type is correct")
        self.assertEqual(error2.line_number, 86056, "Verify that error2 line_number is correct")
        self.assertEqual(error2.thread_id, 0, "Verify that error2 thread_id is correct")
        self.assertTrue("There are some tasks that failed" in error2.heading, "Verify that error2 heading is correct")
        self.assertEqual(str(error2.date), "2017-06-14 15:37:48", "Verify that error2 date is correct")
        self.assertTrue("line 41, in run" in error2.traceback, "Verify that error2 traceback is correct")
        self.assertEqual(error2.workflow_name, "Null Workflow", "Verify that error2 workflow_name is correct")
        self.assertEqual(error2.is_error_heading_assigned, False,
                         "Verify that error2's is_error_heading_assigned is correct")

        # Fetch an error from app3 (failures3.log)
        key3 = self.app3.workflow_dict.keys()[1]
        workflow3 = self.app3.workflow_dict.get(key3)
        error3 = workflow3.errors_list[0]

        # Print error3 info
        print "ERROR3 operation_link_id: " + str(error3.operation_link_id)
        print "ERROR3 operation_type: " + str(error3.operation_type)
        print "ERROR3 line_number: " + str(error3.line_number)
        print "ERROR3 thread_id: " + str(error3.thread_id)
        print "ERROR3 heading: " + str(error3.heading)
        print "ERROR3 date: " + str(error3.date)
        print "ERROR3 traceback: " + str(error3.traceback)
        print "ERROR3 workflow_name: " + str(error3.workflow_name)
        print "ERROR3 is_error_heading_assigned: " + str(error3.is_error_heading_assigned)

        # Verify that the info for the error3 has been properly parsed
        self.assertEqual(error3.operation_link_id, "qbogbl_r3d31331", "Verify that error3 operation_link_id is correct")
        self.assertEqual(error3.operation_type, "Error", "Verify that error3 operation_type is correct")
        self.assertEqual(error3.line_number, 1372, "Verify that error3 line_number is correct")
        self.assertEqual(error3.thread_id, 1, "Verify that error3 thread_id is correct")
        self.assertTrue("SemanticException [Error 10004]" in error3.heading, "Verify that error3 heading is correct")
        self.assertEqual(str(error3.date), "2017-06-08 16:40:06", "Verify that error3 date is correct")
        self.assertTrue("line 803, in _rpc" in error3.traceback, "Verify that error3 traceback is correct")
        self.assertEqual(error3.workflow_name, "ADDRESSES_VW", "Verify that error3 workflow_name is correct")
        self.assertEqual(error3.is_error_heading_assigned, False,
                         "Verify that error3's is_error_heading_assigned is correct")

    def tearDown(self):
        print "TearDown"


