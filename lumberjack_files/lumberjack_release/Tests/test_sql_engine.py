from .. Backend.Database.SqlEngine import run_sql, fetch_app_from_database
from .. Backend.Parsers.FileParser import FileParser
from .. Backend.Parsers.KobeParser import KobeParser
from unittest import TestCase


class TestSqlEngine(TestCase):

    # The current FileParser instance
    file_parser = None

    # The current KobeParser instance
    kobe_parser = None

    # An App object representing a single ingestion run
    app1 = None

    def setUp(self):

        # Generate app1 from smallLog.log
        self.file_parser = FileParser()
        raw_events_list = self.file_parser.run("../Backend/Logs/failures6.log")
        self.kobe_parser = KobeParser()
        self.app1 = self.kobe_parser.run(raw_events_list)

    def test_save_load_from_database(self):
        """
        :summary: Verify that an app is properly stored and loaded by verifying all app fields
        as well as the info for workflows, operations and errors belonging to the app
        """

        # Save the app to the database
        run_sql(self.app1)

        # Load the app from the database
        sql_app = fetch_app_from_database(self.app1.id)

        # Verify that the metadata for app1 (smallLog.log) has been loaded properly
        self.assertEqual(self.app1.id, sql_app.id, "Verify that the app id is correct")
        self.assertEqual(self.app1.name, sql_app.name, "Verify that the app name is correct")
        self.assertEqual(str(self.app1.start_date), str(sql_app.start_date), "Verify that the app start_date is correct")
        self.assertEqual(str(self.app1.end_date), str(sql_app.end_date), "Verify that the app end_date is correct")
        self.assertEqual(self.app1.bytes_transferred, sql_app.bytes_transferred,
                         "Verify that the app bytes_transferred is correct")
        self.assertEqual(self.app1.total_duration, sql_app.total_duration,
                         "Verify that the app total_duration is correct")
        self.assertEqual(self.app1.import_duration, sql_app.import_duration,
                         "Verify that the app import_duration is correct")
        self.assertEqual(self.app1.transform_duration, sql_app.transform_duration,
                         "Verify that the app import_duration is correct")
        self.assertEqual(self.app1.validation_duration, sql_app.validation_duration,
                         "Verify that the app validation_duration is correct")
        self.assertEqual(self.app1.merge_duration, sql_app.merge_duration,
                         "Verify that the app merge_duration is correct")
        self.assertEqual(len(self.app1.successful_workflows_list), len(sql_app.successful_workflows_list),
                         "Verify that the app success_count is correct")
        self.assertEqual(len(self.app1.failed_workflows_list), len(sql_app.failed_workflows_list),
                         "Verify that the app failure_count is correct")
        self.assertEqual(len(self.app1.errors_list), len(sql_app.errors_list),
                         "Verify that the app errors_list count is correct")
        self.assertEqual(len(self.app1.workflow_dict.keys()), len(sql_app.workflow_dict.keys()),
                         "Verify the workflow_dict count is correct")
        self.assertEqual(self.app1.rows_pulled, sql_app.rows_pulled, "Verify that the app rows_pulled is correct")

        # Fetch the first workflow from the original app1 (smallLog.log)
        workflow_key = "TXDETAILS_VW"
        workflow1 = self.app1.workflow_dict.get(workflow_key)

        # Fetch the first workflow from sql_app
        sql_workflow = sql_app.workflow_dict.get(workflow_key)

        # Verify that the info for the workflow has been properly parsed
        print "Workflow1 Operation Count: " + str(len(workflow1.operations_list)) + " Sql Workflow Operation Count: " + \
              str(len(sql_workflow.operations_list))
        self.assertEqual(workflow1.name, sql_workflow.name, "Verify that workflow name is correct")
        self.assertEqual(workflow1.total_duration, sql_workflow.total_duration,
                         "Verify that workflow total_duration is correct")
        self.assertEqual(workflow1.import_duration, sql_workflow.import_duration,
                         "Verify that workflow import_duration is correct")
        self.assertEqual(workflow1.transform_duration, sql_workflow.transform_duration,
                         "Verify that workflow transform_duration is correct")
        self.assertEqual(workflow1.validation_duration, sql_workflow.validation_duration,
                         "Verify that workflow validation_duration is correct")
        self.assertEqual(workflow1.merge_duration, sql_workflow.merge_duration,
                         "Verify that workflow merge_duration is correct")
        self.assertEqual(workflow1.bytes_transferred, sql_workflow.bytes_transferred,
                         "Verify that workflow bytes_transferred is correct")
        self.assertEqual(workflow1.rows_pulled, sql_workflow.rows_pulled, "Verify that workflow rows_pulled is correct")
        self.assertEqual(len(workflow1.operations_list), len(sql_workflow.operations_list),
                         "Verify that the workflow has the correct operation count")
        self.assertEqual(len(workflow1.errors_list), len(sql_workflow.errors_list),
                         "Verify that the workflow has the correct error count")
        self.assertEqual(workflow1.is_r3d3, sql_workflow.is_r3d3, "Verify that workflow is_r3d3 value is correct")
        self.assertEqual(workflow1.application_name, sql_workflow.application_name,
                         "Verify that workflow application_name is correct")

        # Fetch the first operation from the original app1 (smallLog.log)
        operation1 = workflow1.operations_list[0]

        # Fetch the first operation from sql_app
        sql_operation = workflow1.operations_list[0]

        # Verify that the info for the operation has been properly parsed
        self.assertEqual(operation1.duration, sql_operation.duration, "Verify that operation duration is correct")
        self.assertEqual(operation1.error_link_id, sql_operation.error_link_id,
                         "Verify that operation error_link_id is correct")
        self.assertEqual(operation1.start_line_number, sql_operation.start_line_number,
                         "Verify that operation start_line_number is correct")
        self.assertEqual(operation1.end_line_number, sql_operation.end_line_number,
                         "Verify that operation end_line_number is correct")
        self.assertEqual(operation1.thread_id, sql_operation.thread_id, "Verify that operation thread_id is correct")
        self.assertEqual(operation1.operation_type, sql_operation.operation_type,
                         "Verify that operation_type duration is correct")
        self.assertEqual(operation1.bytes_transferred, sql_operation.bytes_transferred,
                         "Verify that operation bytes_transferred is correct")
        self.assertEqual(len(operation1.errors_list), len(sql_operation.errors_list),
                         "Verify that operation errors_list count is correct")
        self.assertEqual(operation1.message, sql_operation.message, "Verify that operation message is correct")
        self.assertEqual(operation1.completed, sql_operation.completed,
                         "Verify that operation completed status is correct")

        # Fetch the first error from workflow1 (smallLog.log)
        error1 = workflow1.errors_list[0]

        # Fetch the first error from sql_workflow
        sql_error = sql_workflow.errors_list[0]

        # Verify that the info for the error has been properly parsed
        self.assertEqual(error1.operation_link_id, sql_error.operation_link_id,
                         "Verify that error operation_link_id is correct")
        self.assertEqual(error1.operation_type, sql_error.operation_type, "Verify that error operation_type is correct")
        self.assertEqual(error1.line_number, sql_error.line_number, "Verify that error line_number is correct")
        self.assertEqual(error1.thread_id, sql_error.thread_id, "Verify that error thread_id is correct")
        self.assertTrue("HiveServer2Error" in sql_error.heading, "Verify that error heading is correct")
        self.assertEqual(str(error1.date), str(sql_error.date), "Verify that error date is correct")
        self.assertEqual(error1.is_error_heading_assigned, sql_error.is_error_heading_assigned,
                         "Verify that is_error_heading_assigned is correct")
        self.assertTrue("line 590, in err_if_rpc_not_ok" in sql_error.traceback,
                        "Verify that error traceback is correct")

    def tearDown(self):
        print "Tear Down"
