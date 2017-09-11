from sqlalchemy import Column, Integer, VARCHAR, DATETIME, Boolean, ForeignKey, create_engine, exc
from sqlalchemy.orm import sessionmaker, lazyload
from sqlalchemy.ext.declarative import declarative_base
from .. Objects.App import App
from .. Objects.Workflow import Workflow
from .. Objects.Operation import Operation
from .. Objects.Error import Error


base = declarative_base()
engine = create_engine('sqlite:///lumberjack.db?check_same_thread=False')
base.metadata.bind = engine
db_session = sessionmaker(bind=engine)
session = db_session()


class AppRow(base):
    __tablename__ = "apps"

    id = Column("id", Integer, primary_key=True)
    name = Column("name", VARCHAR)
    start_date = Column("start_date", DATETIME)
    end_date = Column("end_date", DATETIME)
    bytes_transferred = Column("bytes_transferred", Integer)
    total_duration = Column("total_duration", Integer)
    import_duration = Column("import_duration", Integer)
    total_ingestion_time = Column("total_ingestion_time", Integer)
    transform_duration = Column("transform_duration", Integer)
    validation_duration = Column("validation_duration", Integer)
    merge_duration = Column("merge_duration", Integer)
    rows_pulled = Column("rows_pulled", Integer)
    log_file_name = Column("log_file_name", VARCHAR)
    log_file_path = Column("log_file_path", VARCHAR)
    are_errors_in_workflows = Column("are_errors_in_workflows", Boolean)
    insert_date = Column("insert_date", DATETIME)
    change_date = Column("change_date", DATETIME)


class WorkflowRow(base):
    __tablename__ = "workflows"

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    name = Column("name", VARCHAR)
    date = Column("date", VARCHAR)
    total_duration = Column("total_duration", Integer)
    import_duration = Column("import_duration", Integer)
    transform_duration = Column("transform_duration", Integer)
    validation_duration = Column("validation_duration", Integer)
    merge_duration = Column("merge_duration", Integer)
    bytes_transferred = Column("bytes_transferred", Integer)
    rows_pulled = Column("rows_pulled", Integer)
    has_succeeded = Column("has_succeeded", Boolean)
    retries = Column("retries", Integer)
    is_r3d3 = Column("is_r3d3", Boolean)
    application_name = Column("application_name", VARCHAR)
    log_file_name = Column("log_file_name", VARCHAR)
    log_file_path = Column("log_file_path", VARCHAR)
    insert_date = Column("insert_date", DATETIME)
    change_date = Column("change_date", DATETIME)
    apps_id = Column(Integer, ForeignKey("apps.id"))

    # Foreign Keys
    # apps = relationship("AppRow", foreign_keys=[apps_id])


class OperationRow(base):
    __tablename__ = "operations"

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    duration = Column("duration", Integer)
    start_line_number = Column("start_line_number", Integer)
    end_line_number = Column("end_line_number", Integer)
    thread_id = Column("thread_id", Integer)
    operation_type = Column("operation_type", VARCHAR)
    bytes_transferred = Column("bytes_transferred", Integer)
    message = Column("message", VARCHAR)
    workflow_name = Column("workflow_name", VARCHAR)
    completed = Column("completed", Boolean)
    #insert_date = Column("insert_date", DATETIME)
    #change_date = Column("change_date", DATETIME)

    # Foreign Keys
    apps_id = Column(Integer, ForeignKey("apps.id"))
    workflows_id = Column(Integer, ForeignKey("workflows.id"))
    error_link_id = Column(VARCHAR, ForeignKey("errors.operation_link_id"))


class ErrorRow(base):
    __tablename__ = "errors"

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    operation_type = Column("operation_type", VARCHAR)
    line_number = Column("line_number", Integer)
    thread_id = Column("thread_id", Integer)
    date = Column("date", DATETIME)
    heading = Column("heading", VARCHAR)
    traceback = Column("traceback", VARCHAR)
    workflow_name = Column("workflow_name", VARCHAR)
    insert_date = Column("total_duration", DATETIME)
    change_date = Column("change_date", DATETIME)

    # Foreign Keys
    apps_id = Column(Integer, ForeignKey("apps.id"))
    workflows_id = Column(Integer, ForeignKey("workflows.id"))
    operation_link_id = Column(VARCHAR, ForeignKey("operations.error_link_id"))


def run_sql(new_app):
    base.metadata.create_all(engine)
    add_app_to_database(new_app)


def add_app_to_database(app):
    app_row = AppRow(id=app.id, name=app.name, start_date=app.start_date, end_date=app.end_date,
                     bytes_transferred=app.bytes_transferred, total_duration=app.total_duration,
                     import_duration=app.import_duration, total_ingestion_time=app.total_ingestion_time,
                     transform_duration=app.transform_duration, validation_duration=app.validation_duration,
                     merge_duration=app.merge_duration, rows_pulled=app.rows_pulled, log_file_name=app.log_file_name,
                     log_file_path=app.log_file_path, are_errors_in_workflows=app.are_errors_in_workflows)
    print "ADD APP"
    session.add(app_row)

    # Abort if app id is not unique
    try:
        session.commit()
    except exc.SQLAlchemyError:
        print "SQL Error: Duplicate App id"
        session.rollback()
        return

    for key in app.workflow_dict.keys():
        workflow = app.workflow_dict.get(key)
        add_workflow_to_database(workflow, app)
        
        for operation in workflow.operations_list:
            add__operation_to_database(operation, workflow, app)
            
        for error in workflow.errors_list:
            add__error_to_database(error, app)
    # try:
    #     session.commit()
    # except exc.SQLAlchemyError:
    #     print "SQL Error: " + str(exc.SQLAlchemyError.message)
    session.commit()


def add_workflow_to_database(workflow, app):
    workflow_row = WorkflowRow(name=workflow.name, date=workflow.date, total_duration=workflow.total_duration,
                               import_duration=workflow.import_duration, transform_duration=workflow.transform_duration,
                               validation_duration=workflow.validation_duration, merge_duration=workflow.merge_duration,
                               bytes_transferred=workflow.bytes_transferred, rows_pulled=workflow.rows_pulled,
                               has_succeeded=workflow.has_succeeded, retries=workflow.retries, is_r3d3=workflow.is_r3d3,
                               application_name=workflow.application_name, log_file_name=workflow.log_file_name,
                               log_file_path=workflow.log_file_path, apps_id=app.id)
    session.add(workflow_row)


def add__operation_to_database(operation, workflow, app):
    operation_row = OperationRow(error_link_id=operation.error_link_id, duration=operation.duration,
                                 start_line_number=operation.start_line_number,
                                 end_line_number=operation.end_line_number, thread_id=operation.thread_id,
                                 operation_type=operation.operation_type, bytes_transferred=operation.bytes_transferred,
                                 message=operation.message, workflow_name=operation.workflow_name,
                                 workflows_id=workflow.id, completed=operation.completed, apps_id=app.id)

    session.add(operation_row)


def add__error_to_database(error, app):
    error_row = ErrorRow(operation_link_id=error.operation_link_id, operation_type=error.operation_type,
                         line_number=error.line_number, thread_id=error.thread_id, heading=error.heading,
                         date=error.date, traceback=error.traceback, workflow_name=error.workflow_name, apps_id=app.id)
    session.add(error_row)


def fetch_app_from_database(run_id):
    sql_app = session.query(AppRow).options(lazyload('*')).filter_by(id=run_id).first()

    if sql_app is None:
        return None

    new_app = App()
    new_app.id = run_id
    new_app.name = sql_app.name
    new_app.start_date = sql_app.start_date
    new_app.end_date = sql_app.end_date
    new_app.bytes_transferred = sql_app.bytes_transferred
    new_app.total_duration = sql_app.total_duration
    new_app.import_duration = sql_app.import_duration
    new_app.total_ingestion_time = sql_app.total_ingestion_time
    new_app.transform_duration = sql_app.transform_duration
    new_app.validation_duration = sql_app.validation_duration
    new_app.merge_duration = sql_app.merge_duration
    new_app.rows_pulled = sql_app.rows_pulled
    new_app.log_file_name = sql_app.log_file_name
    new_app.log_file_path = sql_app.log_file_path
    new_app.are_errors_in_workflows = sql_app.are_errors_in_workflows

    # Fetch each workflow belonging to the current app
    for workflow in session.query(WorkflowRow).options(lazyload('*')).filter_by(apps_id=run_id):
        new_workflow = Workflow(workflow.name, workflow.id)
        new_workflow.date = workflow.date
        new_workflow.total_duration = workflow.total_duration
        new_workflow.import_duration = workflow.import_duration
        new_workflow.transform_duration = workflow.transform_duration
        new_workflow.validation_duration = workflow.validation_duration
        new_workflow.merge_duration = workflow.merge_duration
        new_workflow.bytes_transferred = workflow.bytes_transferred
        new_workflow.rows_pulled = workflow.rows_pulled
        new_workflow.has_succeeded = workflow.has_succeeded
        new_workflow.retries = workflow.retries
        new_workflow.is_r3d3 = workflow.is_r3d3
        new_workflow.application_name = workflow.application_name
        new_workflow.log_file_name = workflow.log_file_name
        new_workflow.log_file_path = workflow.log_file_path

        # Fetch each operation belonging to the current workflow
        for operation in session.query(OperationRow).options(lazyload('*')).filter_by(apps_id=run_id):
            if operation.workflow_name == workflow.name:
                new_operation = Operation(operation.duration, operation.start_line_number, operation.end_line_number,
                                          operation.thread_id, operation.operation_type, operation.message,
                                          operation.completed, operation.bytes_transferred, operation.error_link_id,
                                          workflow.name)
                new_workflow.operations_list.append(new_operation)

        # Fetch each error belonging to the current workflow
        for error in session.query(ErrorRow).options(lazyload('*')).filter_by(apps_id=run_id):
            if error.workflow_name == workflow.name:
                new_error = Error(error.operation_type, error.thread_id, error.line_number, error.date, error.traceback)
                new_error.heading = error.heading
                new_error.operation_link_id = error.operation_link_id
                new_workflow.errors_list.append(new_error)

        # Add the workflow to the current app
        new_app.workflow_dict[workflow.name] = new_workflow

        if new_workflow.has_succeeded:
            new_app.successful_workflows_list.append(new_workflow)
        else:
            new_app.failed_workflows_list.append(new_workflow)
    return new_app


def fetch_most_recent_app_from_database():
    most_recent_app = session.query(AppRow).order_by(AppRow.insert_date).first()
    app = fetch_app_from_database(most_recent_app.id)
    return app


def fetch_operation_averages_from_database(app_name, workflow_name):
    filtered_applications = session.query(AppRow).filter_by(name=app_name)
    average_list = []
    importing = 0
    transforming = 0
    validating = 0
    merging = 0
    filtered_applications = session.query(AppRow).filter_by(name=app_name)
    counter = 0
    for application in filtered_applications:
        app = fetch_app_from_database(application.id)
        workflow = app.workflow_dict.get(workflow_name)
        if workflow is not None:
            importing += workflow.import_duration
            transforming += workflow.transform_duration
            validating += workflow.validation_duration
            merging += workflow.merge_duration
        counter += 1
    average_list.append(importing // counter)
    average_list.append(transforming // counter)
    average_list.append(validating // counter)
    average_list.append(merging // counter)
    return average_list


def fetch_last_week_information(app_name, workflow_name):
    operation_to_data_dict = {"importing": [], "transforming": [], "validating": [], "merging": []}
    last_seven_application_runs = session.query(AppRow).filter_by(name=app_name).order_by(AppRow.insert_date).limit(7).all()
    for application in last_seven_application_runs:
        app = fetch_app_from_database(application.id)
        workflow = app.workflow_dict.get(workflow_name)
        operation_to_data_dict.get("importing").append(workflow.import_duration)
        operation_to_data_dict.get("transforming").append(workflow.transform_duration)
        operation_to_data_dict.get("validating").append(workflow.validation_duration)
        operation_to_data_dict.get("merging").append(workflow.merge_duration)
    return operation_to_data_dict
