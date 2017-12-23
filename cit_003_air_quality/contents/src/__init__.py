import os
import logging
import sys
import requests
from collections import OrderedDict
import src.carto as carto
import datetime
import hashlib

### Constants
DATA_DIR = 'data'
# max page size = 10000
DATA_URL = 'https://api.openaq.org/v1/measurements?limit=10000&include_fields=attribution&page={page}'
# always check first 10 pages
MIN_PAGES = 10

### asserting table structure rather than reading from input
PARAMS = ['pm25','pm10','so2','no2','o3','co','bc']
CARTO_TABLES = {
    'pm25':'cit_003a_air_quality_pm25',
    'pm10':'cit_003b_air_quality_pm10',
    'so2':'cit_003c_air_quality_so2',
    'no2':'cit_003d_air_quality_no2',
    'o3':'cit_003e_air_quality_o3',
    'co':'cit_003f_air_quality_co',
    'bc':'cit_003g_air_quality_bc',
}
CARTO_SCHEMA = OrderedDict([
    ("the_geom","geometry"),
    ("_UID","text"),
    ("utc","timestamp"),
    ("value","numeric"),
    ("parameter","text"),
    ("location","text"),
    ("city","text"),
    ("country","text"),
    ("unit","text"),
    ("attribution","text")
])
UID_FIELD = '_UID'
TIME_FIELD = 'utc'

CARTO_USER = os.environ.get('CARTO_USER')
CARTO_KEY = os.environ.get('CARTO_KEY')

# Carto limit at 10M?
MAXROWS = 1000000
MAXAGE = datetime.datetime.now() - datetime.timedelta(days=30)

### Generate UID
def genUID(obs):
    # location should be unique, plus measurement timestamp
    id_str = '{}_{}'.format(obs['location'], obs['date']['utc'])
    return hashlib.md5(id_str.encode('utf8')).hexdigest()

### Fetch and parse OpenAQ into separate tables by parameter
def process(exclude_ids):
    total_counts = dict(((param, 0) for param in PARAMS))
    page = 1
    new_count = 1
    # get and parse each page
    # stop when no new results or 100 pages
    while page <= MIN_PAGES or new_count and page < 100:
        logging.info("Fetching page {}".format(page))
        r = requests.get(DATA_URL.format(page=page))
        page += 1

        # separate row lists per param
        rows = dict(((param, []) for param in PARAMS))

        # parse data excluding existing observations
        for obs in r.json()['results']:
            param = obs['parameter']
            uid = genUID(obs)
            if uid not in exclude_ids[param] and 'coordinates' in obs:
                exclude_ids[param].append(uid)
                row = []
                for field in CARTO_SCHEMA.keys():
                    if field == 'the_geom':
                        geom = {
                            "type": "Point",
                            "coordinates": [
                                obs['coordinates']['longitude'],
                                obs['coordinates']['latitude']
                            ]
                        }
                        row.append(geom)
                    elif field == UID_FIELD:
                        row.append(uid)
                    elif field == TIME_FIELD:
                        row.append(obs['date'][TIME_FIELD])
                    elif field == 'attribution':
                        row.append(str(obs['attribution']))
                    else:
                        row.append(obs[field])
                rows[param].append(row)

        new_count = 0
        # insert new rows
        for param in PARAMS:
            count = len(rows[param])
            if count:
                logging.info('Pushing {} new {} rows'.format(
                    count, param))
                carto.insertRows(CARTO_TABLES[param],
                                 CARTO_SCHEMA,
                                 rows[param])
                new_count += count
            total_counts[param] += count
    return total_counts


def getTableIds(table, id_field, order):
    if carto.tableExists(table):
        r = carto.getFields(id_field, table, order=order, f='csv')
        # quick read 1-column csv to list
        return r.text.split('\r\n')[1:-1]
    else:
        logging.info('Table {} does not exist, creating'.format(
            table))
        carto.createTable(table, schema)
        carto.createIndex(table, index)
    return []


def dropOldRows(table, old_ids, new_count):
    old_count = len(old_ids)
    logging.info('{} previous rows: {}, new: {}, max: {}'.format(
        table, old_count, new_count, MAXROWS))

    # by max age
    delete_where = "{} < '{}'".format(
        TIME_FIELD, MAXAGE.isoformat())
    # by excess rows
    if old_count + new_count > MAXROWS:
        drop_ids = old_ids[min(MAXROWS, MAXROWS - new_count):]
        delete_where = '{} OR {} in ({})'.format(
            delete_where, UID_FIELD, ','.join(drop_ids))

    r = carto.deleteRows(table, delete_where)
    num_dropped = r.json()['total_rows']
    if num_dropped > 0:
        logging.info('Dropped {} old rows from {}'.format(
            num_dropped, table))

def main():
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)

    ### 1. Get existing uids, if none create tables
    existing_ids = {}
    for param in PARAMS:
        table = CARTO_TABLES[param]
        order = '{} desc'.format(TIME_FIELD)
        if carto.tableExists(table):
            r = carto.getFields(UID_FIELD,
                                table,
                                order=order,
                                f='csv')
            # quick read 1-column csv to list
            existing_ids[param] = r.text.split('\r\n')[1:-1]
        else:
            logging.info('Table {} does not exist, creating'.format(
                table))
            carto.createTable(table, CARTO_SCHEMA)
            carto.createIndex(table, TIME_FIELD)
            existing_ids[param] = []

    ### 2. Iterively fetch, parse and post new data
    # this is done all together because OpenAQ endpoint doesn't
    # support filtering by parameter
    total_counts = process(existing_ids)

    ### 3. Remove old observations
    for param in PARAMS:
        dropOldRows(CARTO_TABLES[param],
                    existing_ids[param],
                    total_counts[param])

    logging.info('SUCCESS')
