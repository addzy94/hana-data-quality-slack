# Imports #
from collections import OrderedDict
from Crypto.Cipher import AES
import random, string, base64
from helper import *
import itertools
import argparse
import datetime
import logging
import yaml
import json
import sys
import re

CONNECTION = "HPQ-TRACKING"
CONNECTION_LOCATION = "connections.yaml"
SECRET_LOCATION = "riddler.txt"
DQ_QUERY_LOCATION = "dq_queries.yaml"

# Logging Configuration #
DATETIME = datetime.datetime.today()
CURRENT_DATE = DATETIME.strftime("%Y-%m-%d")
CURRENT_TIME = DATETIME.time().strftime("%H:%M:%S_%f")
FILE_NAME = "data_quality_{}_{}_logFile.log".format(CURRENT_DATE, CURRENT_TIME)
FORMAT_STRING = "%(asctime)s: %(levelname)s: %(funcName)s: Line %(lineno)d:  %(message)s"
logging.basicConfig(filename = FILE_NAME,
                    level = logging.INFO,
                    filemode = "w",
                    format = FORMAT_STRING)

report_format = logging.Formatter(FORMAT_STRING)
report_handler = logging.StreamHandler(sys.stdout)
report_handler.setLevel(logging.INFO)
report_handler.setFormatter(report_format)
logging.getLogger().addHandler(report_handler)

# Reading Connection File #
connections = []

try:
    with open(CONNECTION_LOCATION) as file:
        file_object = yaml.load_all(file, Loader = yaml.FullLoader)
        for doc in file_object:
            step = {}
            for k,v in doc.items():
                v = str(v).replace("\n", " ").strip()
                step[k] = v
            connections.append(step)
except Exception:
    logging.exception("Issue with Reading Connections File")
    sys.exit(-1)

# Getting Secret Key #
KEY = ""
try:
    with open(SECRET_LOCATION) as file:
        KEY = file.readline().replace("\n", "")
except Exception:
    logging.exception("Issue with getting Secret Key")
    sys.exit(-1)

# Checking Connection Information #
address = user = password = ""
port = 0

try:
    connection_present = 0
    for connection in connections:
        if connection["UniqueID"] == CONNECTION:
            connection_present = 1
            address = connection["Address"]
            port = int(connection["Port"])
            user = connection["Username"]

            decryption_suite = AES.new(KEY, AES.MODE_CFB, user.zfill(16)[:16])
            password = decryption_suite.decrypt(base64.b64decode(connection["EncryptedPassword"])).decode("utf-8")
            break
except Exception:
    logging.exception("Issue with Reading Connections file")
    sys.exit(-1)

# Reading DQ Query File #
RESULTS = OrderedDict()
QUERIES_LIST = OrderedDict()
ISSUES = OrderedDict()

try:
    with open(DQ_QUERY_LOCATION) as file:
        file_object = yaml.load_all(file, Loader = yaml.FullLoader)
        for query in file_object:
            query_info = query["Query Info"]
            query = re.sub(r"\n", " ", query["Query"]).strip()
            QUERIES_LIST[query_info] = query
except Exception:
    logging.exception("Issue with reading DQ Queries file")
    sys.exit(-1)

# Creating a Connection #
try:
    if connection_present == 1:
        connection = connection_setup(address, port, user, password)
    else:
        logging.error("Connection-ID not present in Connections File")
except Exception:
    logging.exception("Issue While Connecting")
    sys.exit(-1)

# Running DQ Queries #
try:
    for query_id, query in QUERIES_LIST.items():
        print(query_id, query)
        RESULTS[query_id] = return_count(connection, query)
        logging.info("Result for {}: {}.".format(query_id, RESULTS[query_id]))
except Exception:
    logging.exception("Issue with running the queries")
    sys.exit(-1)

# Return Results #
for query_id, count_value in RESULTS.items():
    if count_value > 0:
        ISSUES[query_id] = count_value

raise_slack_alert(ISSUES, RESULTS)
