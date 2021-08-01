from hdbcli import dbapi
import numpy as np
import itertools
import requests
import datetime
import logging
import yaml
import re

def connection_setup(address, port, user, password):
    connection = dbapi.connect(address = address,
                               port = port,
                               user = user,
                               password = password)

    if connection.isconnected() is True:
        logging.info("### Connection Established. ###")
        return(connection)

def return_count(connection, query):
    cursor = connection.cursor()
    result = cursor.execute(query)

    if result == True:
        return_number = [row for row in cursor]
        return(return_number[0][0])
    else:
        return(-1)

def raise_slack_alert(issues, results):
    if len(issues) > 0:
        text = "Last 24 hours:"
        for query_id, count_value in results.items():
            if count_value > 0:
                text = text + "\n" + "For DQ " + query_id + ": " + str(count_value) + " rows have issues."

        data = {"channel": "#sqap-stream-devs-alerts", "username": "Data Quality Report", "text" : text}
        url = "https://hooks.slack.com/services/M024GHP2K/CZ45Q0DKR/RbyQzTB3XLEeyOKZrOOMpvTx"
        requests.post(url = url, data = str(data))
    else:
        text = f"No Problems"
