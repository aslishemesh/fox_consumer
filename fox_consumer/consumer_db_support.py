import psycopg2
import pika
from fox_helper import FoxItem
import json
import jsonschema


class FoxConsumerPostgres(object):
    def __init__(self):
