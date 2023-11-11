from tabulate import tabulate

from notes import notes


def generate_html_table_from_df(df, filename):
    """
    Generates a HTML table from a Pandas DataFrame.

    Parameters
    ----------
    df : Pandas DataFrame
        The DataFrame to be converted to an HTML table.

    filename : str
        SQL query file name. It is used as the "ID" of the html
        table.

    Returns
    -------
        html_table : str
    """

    # Convert column names to upper case
    df.columns = [col.upper() for col in df.columns]

    filename = filename.replace(".sql", "")
    filename_heading = filename.replace("_", " ").title()

    html_table = f"""
    <div class="p-5 mb-2 bg-light rounded-3" id="{filename}_wrapper">
    <h3>{filename_heading}</h3>
        <div class="container-fluid py-4">
            <div class="row">
                <div>
    """

    html_table += "\n"
    html_table += df.to_html(index=False)
    html_table = html_table.replace(
        '<table border="1" class="dataframe">',
        f'<table class="table table-striped table-bordered" id="{filename}">',
    )
    html_table = html_table.replace("<thead>", '<thead class="thead-dark">')
    html_table = html_table.replace('<tr style="text-align: right;">', "<tr>")

    html_table += """
            </div>
        </div>
    """

    html_msg = fetch_notes(filename)

    if html_msg != "":
        html_table += html_msg

    html_table += "</div>"
    html_table += "</div>"

    return html_table


def fetch_notes(filename):
    """
    Extracts Query specific notes
    """
    query_id = int(filename.split("_")[0])

    if query_id not in notes.keys():
        return ""

    msgs = notes[query_id]

    html_msg = """
    <div class="alert alert-info mt-2" role="alert">
    """

    html_msg += "<ul>"

    for msg in msgs:
        html_msg += "<li>" + msg + "</li>"

    html_msg += "</ul>"
    html_msg += "</div>"
    html_msg += "<br>"

    return html_msg


def print_messages(messages, headers):
    """
    Prints the messages in the input list.

    :param messages: List of Lists
    :param headers: List of strings
    :return: None
    """

    print(
        tabulate(
            messages,
            headers=headers,
            tablefmt="fancy_grid",
        )
    )
