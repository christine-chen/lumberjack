from flask import Flask, render_template
from .. Backend.Parsers.FileParser import FileParser
from .. Backend.Parsers.KobeParser import KobeParser
from .. Backend.Database.SqlEngine import run_sql, fetch_most_recent_app_from_database,\
                                                    fetch_operation_averages_from_database, fetch_last_week_information
import os

# The current Flask instance
application = Flask(__name__)

# The list of current apps
app_list = []

# The
average_operations_list = []


def main():
    parse_all_logs_in_directory("lumberjack_release/Backend/Logs/")
    # file_parser = FileParser()
    # raw_events_list = file_parser.run("lumberjack_release/Backend/Logs/failures2.log")
    # kobe_parser = KobeParser()
    # new_app = kobe_parser.run(raw_events_list)
    # return new_app


def parse_all_logs_in_directory(path):
    print "PARSE"
    print str(os.listdir(path))
    for root, dirs, files in os.walk(path):
        for filename in files:
            if filename.endswith(".log"):
                print "PARSE: " + filename
                file_parser = FileParser()
                raw_events_list = file_parser.run(path + filename)
                kobe_parser = KobeParser()
                new_app = kobe_parser.run(raw_events_list)
                app_list.append(new_app)
                run_sql(new_app)


@application.route("/")
def hello():
    return "hello world"


@application.route("/<application_name>")
def apps(application_name):
    return render_template("application.html", app=current_app, app_name=application_name)


@application.route("/<application_name>/<workflow_name>")
def workflows(application_name, workflow_name):
    current_workflow = None
    for current_key in current_app.workflow_dict.keys():
        workflow = current_app.workflow_dict.get(current_key)
        if workflow_name == workflow.name:
            current_workflow = workflow
    print current_app.workflow_dict.keys()
    average = fetch_operation_averages_from_database(application_name, current_workflow.name)
    history = fetch_last_week_information(application_name, current_workflow.name)
    return render_template("workflow.html", app_name=application_name, workflow=current_workflow,
                           average=average, history=history)

main()
current_app = fetch_most_recent_app_from_database()


print "========= Starting App ========="
print current_app.log_file_name
print current_app.name

if __name__ == "__main__":
    application.run(host="127.0.0.1", port=5000, debug=False, threaded=True)
