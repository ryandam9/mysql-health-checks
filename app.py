import argparse
import glob
import logging
import os
import sys
from datetime import datetime

from config import database, port, user
from database.execute_query import mysql_execute_query_sqlalchemy
from utils import generate_html_table_from_df, print_messages

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s %(levelname)-8s %(module)s %(message)s"
)


def read_config_data():
    """
    Reads Database, Port, and User from config.py file.
    Also, reads DB host & password from user's home directory.

    Returns a dictionary that holds all details required to connect
    to the database.
    """
    # User's home directory.
    home_directory = os.path.expanduser("~")

    # Read DB hostname & password from User's home directory.
    try:
        with open(home_directory + "/.db.txt", "r") as pgpass_file:
            for line in pgpass_file:
                if line.startswith("host"):
                    host = line.split("=")[1].strip()
                elif line.startswith("password"):
                    password = line.split("=")[1].strip()
    except Exception as error:
        logging.error(f"Unable to read {home_directory}/.db.txt file.")
        logging.error(str(error))
        logging.error(
            f"Create {home_directory}/.db.txt file with the following contents:"
        )
        logging.error("host = <host>")
        logging.error("password = <password>")
        sys.exit(1)

    db_config = {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
    }

    return db_config


def execute_queries(db_config: dict, requested_query: int):
    """
    Executes the queries in "./queries" directory using Pandas,
    and formats the result as HTML table.

    Input:
    ------
    db_config      : A Dictionary holding DB connection parameters
    requested_query: A query ID. It should be one of the queries in
                     "queries" directory.

    Returns
    -------
    A tuple.

    Entry 1: A String containing results of all queries formatted as HTML table.
    Entry 2: HTML Data table ids
    """
    sql_files = []
    queries_directory = "./queries"

    # Read all the sql file names
    for file_name in glob.glob(f"{queries_directory}/*.sql"):
        sql_files.append(os.path.basename(file_name))

    sql_files.sort(key=lambda x: int(x.split("_")[0]))

    html_table_string = ""
    data_tables = ""

    for file_no, file_name in enumerate(sql_files):
        file_no += 1

        if requested_query != 0 and file_no != requested_query:
            continue

        with open(f"{queries_directory}/" + file_name) as query_file:
            query = query_file.read()
            query = query.replace(";", " ")

            print_messages([[query]], headers=[file_name])

            # Execute the query
            df = mysql_execute_query_sqlalchemy(db_config, query, None)

            print(df)
            print("")

            if df is not None:
                html_table = generate_html_table_from_df(df, file_name)
                html_table_string += "<br>"
                html_table_string += html_table

                # Make the HTML table a DataTable
                data_tables += (
                    "\n" + "$('#" + file_name.replace(".sql", "") + "').DataTable();"
                )

    return sql_files, html_table_string, data_tables


def write_html_report(html_table_string, db_config, data_tables, sql_files):
    """ """
    # Read HTML template file
    html_template = ""

    with open("./templates/health_check_template.html", "r") as f:
        html_template = f.read()

    ts = datetime.now().strftime("%Y_%m_%d %H:%M")

    # The following Data placeholders are going to be replaced:
    #    1. HTML tables.
    #    2. Javascript to initialize the data tables.
    html_template = html_template.replace("[data_placeholder]", html_table_string)
    html_template = html_template.replace("[db_endpoint]", db_config["host"])
    html_template = html_template.replace("[db_name]", db_config["database"])
    html_template = html_template.replace("[db_user]", db_config["user"])
    html_template = html_template.replace("[rundate]", ts)

    # This is where JS Data tables are created.
    html_template = html_template.replace("[dynamic_tables]", data_tables)

    # Prepare table of contents as an ordered list
    table_of_contents = ""

    for entry in sql_files:
        entry_fmt = entry.replace(".sql", "")
        table_of_contents += f'<li class="list-group-item"><a href="#{entry_fmt}_wrapper">{entry_fmt}</a></li>'

    html_template = html_template.replace(
        "[table_of_contents_placeholder]", table_of_contents
    )

    ts = ts.replace(" ", "_").replace(":", "_")
    report_file = f"./reports/health_check_{ts}.html"

    with open(report_file, "w") as f:
        f.write(html_template)

    logging.info(f"HTML Report is available @ {report_file}")


def main():
    """
    Driver function
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--query_id", help="Query ID to be executed")

    args = parser.parse_args()

    if args.query_id is None:
        requested_query = 0
    else:
        requested_query = int(args.query_id)

    # Read DB Connection string details
    db_config = read_config_data()

    # Execute queries
    sql_files, html_table_string, data_tables = execute_queries(
        db_config, requested_query
    )

    # Write HTML report to a file
    write_html_report(html_table_string, db_config, data_tables, sql_files)


main()
