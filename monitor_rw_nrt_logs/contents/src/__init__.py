import os
import logging
import sys
import io

import pandas as pd
import boto3
from collections import OrderedDict
from datetime import datetime, timedelta
import cartosql

# Constants
DATA_DIR = 'data'
TIMESTEP = {'days':1}
DATE_FORMAT = '%Y-%m-%d'

ACCESS_ID = os.environ.get('aws_access_key_id')
SECRET_KEY = os.environ.get('aws_secret_access_key')

s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_ID,
    aws_secret_access_key=SECRET_KEY
)
s3_resource = boto3.resource(
    's3',
    aws_access_key_id=ACCESS_ID,
    aws_secret_access_key=SECRET_KEY
)

s3_bucket = "wri-public-data"
s3_folder = "resourcewatch/papertrial/"

# Functions for reading and uploading data to/from S3
def read_from_S3(bucket, key, index_col=0):
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), index_col=[index_col], encoding="utf8", header=None, sep='\t')
    return(df)

def write_to_S3(df, bucket, key):
    csv_buffer = io.StringIO()
    # Need to set encoding in Python2... default of 'ascii' fails
    df.to_csv(csv_buffer, encoding='utf-8')
    s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())

# asserting table structure rather than reading from input
CARTO_TABLE = 'error_logs'
CARTO_SCHEMA = OrderedDict([
    ('datetime', 'timestamp'),
    ('dataset', 'text'),
    ('message', 'text')
])

LOG_LEVEL = logging.INFO
MAXROWS = 10000
MAXAGE = datetime.today() - timedelta(days=6)
CLEAR_TABLE_FIRST = True

# Generate UID
def genUID(date, pos_in_shp):
    return str('{}_{}'.format(date, pos_in_shp))

def getNewDates():
    '''Get new dates excluding existing'''
    new_dates = []
    date = datetime.today()
    while date > MAXAGE:
        date -= timedelta(**TIMESTEP)
        datestr = date.strftime(DATE_FORMAT)
        new_dates.append(datestr)
    return new_dates

def processNewData(exclude_ids):
    new_data = pd.DataFrame()
    new_dates = getNewDates()
    for date in new_dates:
        # 1. Fetch data from source
        logging.info(s3_bucket + '/' + s3_folder + date + '.tsv')
        log = read_from_S3(s3_bucket, s3_folder + date + '.tsv')

        logging.info(log.head())
        # 2. Extract rows with 'error:' in the last column
        logging.info('Parsing data')
        errors = log[log[log.columns[-1]].str.lower().str.contains('error:')]
        logging.debug(errors.head())
        # 3. Only keep some columns
        errors = errors[[1,8,9]]
        logging.debug(errors.head())
        write_to_S3(errors, s3_bucket, s3_folder + date+'_errors.tsv')

        # 4. Insert new observations
        #new_count = len(rows)
        #if new_count:
        #    logging.info('Pushing new rows')
        #    cartosql.insertRows(CARTO_TABLE, CARTO_SCHEMA.keys(),
        #                        CARTO_SCHEMA.values(), rows)


##############################################################
# General logic for Carto
# should be the same for most tabular datasets
##############################################################

def createTableWithIndices(table, schema, idField):
    '''Get existing ids or create table'''
    cartosql.createTable(table, schema)
#    cartosql.createIndex(table, idField, unique=True)

def getFieldAsList(table, field, orderBy=''):
    assert isinstance(field, str), 'Field must be a single string'
    r = cartosql.getFields(field, table, order='{}'.format(orderBy),
                           f='csv')
    return(r.text.split('\r\n')[1:-1])

def deleteExcessRows(table, max_rows, time_field, max_age=''):
    '''Delete excess rows by age or count'''
    num_dropped = 0
    if isinstance(max_age, datetime):
        max_age = max_age.isoformat()

    # 1. delete by age
    if max_age:
        r = cartosql.deleteRows(table, "{} < '{}'".format(time_field, max_age))
        num_dropped = r.json()['total_rows']

    # 2. get sorted ids (old->new)
    ids = getFieldAsList(CARTO_TABLE, 'cartodb_id', orderBy=''.format(TIME_FIELD))

    # 3. delete excess
    if len(ids) > max_rows:
        r = cartosql.deleteRowsByIDs(table, ids[:-max_rows])
        num_dropped += r.json()['total_rows']
    if num_dropped:
        logging.info('Dropped {} old rows from {}'.format(num_dropped, table))


def main():
    logging.basicConfig(stream=sys.stderr, level=LOG_LEVEL)
    logging.info('STARTING')

    if CLEAR_TABLE_FIRST:
        logging.info("Clearing table")
        if cartosql.tableExists(CARTO_TABLE):
            cartosql.dropTable(CARTO_TABLE)

    # 1. Check if table exists and create table
    existing_ids = []
    if cartosql.tableExists(CARTO_TABLE):
        existing_ids = getFieldAsList(CARTO_TABLE, '')
    else:
        createTableWithIndices(CARTO_TABLE, CARTO_SCHEMA, '')

    # 2. Iterively fetch, parse and post new data
    #num_new =
    processNewData(existing_ids)

    #existing_count = num_new + len(existing_ids)
    #logging.info('Total rows: {}, New: {}, Max: {}'.format(
    #    existing_count, num_new, MAXROWS))

    # 3. Remove old observations
    #deleteExcessRows(CARTO_TABLE, MAXROWS, TIME_FIELD, MAXAGE)

    logging.info('SUCCESS')
