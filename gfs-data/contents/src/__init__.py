from __future__ import unicode_literals

import os
import sys
import urllib
import datetime
import logging
import subprocess
import eeUtil

# constants for GFS
SOURCE_URL = 'http://www.ftp.ncep.noaa.gov/data/nccf/com/gfs/prod/gfs.{date}{hr}/gfs.t{hr}z.pgrb2.0p25.f003'
FILENAME = '{date}{hr}'
VARIABLES = [
    'tmp2m',
    'spfh2m',
    'rh2m',
    'ugrd10m',
    'vgrd10m',
    'apcpsfc',
    'pwatclm',
    'tcdcclm',
    'dswrfsfc'
]
BANDS = {
    'tmp2m': '281',
    'spfh2m': '282',
    'rh2m': '284',
    'ugrd10m': '288',
    'vgrd10m': '289',
    'apcpsfc': '293',
    'pwatclm': '313',
    'tcdcclm': '320',
    'dswrfsfc': '335'
}
BAND_NAMES = [
    'temperature_2m_above_ground',
    'specific_humidity_2m_above_ground',
    'relative_humidity_2m_above_ground',
    'u_component_of_wind_10m_above_ground',
    'v_component_of_wind_10m_above_ground',
    'total_precipitation_surface',
    'precipitable_water_entire_atmosphere',
    'total_cloud_cover_entire_atmosphere',
    'downward_shortwave_radiation_flux'
]

DATA_DIR = 'data'

EE_COLLECTION = 'gfs'
EE_ASSET = 'gfs_{date}{hr}'
GS_FOLDER = 'gfs'

MAX_ASSETS = 2
DATE_FORMAT = '%Y%m%d'
TIME_FORMAT = '%Y%m%d%H'
HOURS = ('00', '06', '12', '18')
TIMESTEP = {'days': 1}


def getUrl(timestr):
    '''get source url from datestamp'''
    return SOURCE_URL.format(date=timestr[:8], hr=timestr[8:])


def getAssetName(timestr):
    '''get source url from datestamp'''
    return os.path.join(EE_COLLECTION, EE_ASSET.format(
        date=timestr[:8], hr=timestr[8:]))


def getFilename(timestr):
    '''get filename from datestamp'''
    return os.path.join(DATA_DIR, FILENAME.format(
        date=timestr[:8], hr=timestr[8:]))


def getTime(asset):
    '''return last 10 chars of asset name'''
    return os.path.splitext(asset)[0][-10:]


def getNewTimes(exclude_times):
    '''Get new dates excluding existing'''
    new_times = []
    date = datetime.datetime.today()
    for i in range(MAX_ASSETS):
        for hr in HOURS:
            timestr = date.strftime(DATE_FORMAT) + hr
            if timestr not in exclude_times:
                new_times.append(timestr)
        date -= datetime.timedelta(**TIMESTEP)
    return new_times


def convert(files):
    '''convert gfs gribs to tifs'''
    tifs = []
    for f in files:
        tif = os.path.join(DATA_DIR, '{}.tif'.format(getTime(f)))
        cmd = ['gdal_translate', '-of', 'Gtiff', '-a_srs', 'EPSG:4326',
               '-a_ullr', '-180', '90', '180', '-90']
        for v in VARIABLES:
            cmd += ['-b', BANDS[v]]
        cmd += [f, tif]
        logging.debug('Converting {} to {}'.format(f, tif))
        subprocess.call(cmd)
        tifs.append(tif)
    return tifs


def fetch(times):
    '''Fetch files by datestamp'''
    files = []
    for timestr in times:
        url = getUrl(timestr)
        f = getFilename(timestr)
        logging.debug('Fetching {}'.format(url))
        # New data may not yet be posted
        try:
            urllib.request.urlretrieve(url, f)
            files.append(f)
        except Exception as e:
            logging.warning('Could not fetch {}'.format(url))
    return files


def processNewData(existing_dates):
    '''fetch, process, upload, and clean new data'''
    # 1. Determine which files to fetch
    #new_dates = getNewTimes(existing_dates)
    new_dates = ['2018021018']

    # 2. Fetch new files
    logging.info('Fetching files')
    files = fetch(new_dates)
    files = [os.path.join(DATA_DIR, d) for d in new_dates]

    if files:
        # 3. Convert new files
        logging.info('Converting files')
        tifs = convert(files)

        # 4. Upload new files
        logging.info('Uploading files')
        dates = [getTime(tif) for tif in tifs]
        datestamps = [datetime.datetime.strptime(date, TIME_FORMAT)
                      for date in dates]
        assets = [getAssetName(date) for date in dates]
        eeUtil.uploadAssets(tifs, assets, GS_FOLDER, datestamps,
                            bands=BAND_NAMES)

        # 5. Delete local files
        logging.info('Cleaning local files')
        for tif in tifs:
            os.remove(tif)
        for f in files:
            os.remove(f)

        return assets
    return []


def checkCreateCollection(collection):
    '''List assests in collection else create new collection'''
    if eeUtil.exists(collection):
        return eeUtil.ls(collection)
    else:
        logging.info('{} does not exist, creating'.format(collection))
        eeUtil.createFolder(collection, True, public=True)
        return []


def deleteExcessAssets(dates, max_assets):
    '''Delete assets if too many'''
    # oldest first
    dates.sort()
    if len(dates) > max_assets:
        for date in dates[:-max_assets]:
            eeUtil.removeAsset(getAssetName(date))


def main():
    '''Ingest new data into EE and delete old data'''
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    logging.info('STARTING')

    # Initialize eeUtil
    eeUtil.initJson()

    # 1. Check if collection exists and create
    existing_assets = checkCreateCollection(EE_COLLECTION)
    existing_dates = [getTime(a) for a in existing_assets]

    # 2. Fetch, process, stage, ingest, clean
    new_assets = processNewData(existing_dates)
    new_dates = [getTime(a) for a in new_assets]

    # 3. Delete old assets
    existing_dates = existing_dates + new_dates
    logging.info('Existing assets: {}, new: {}, max: {}'.format(
        len(existing_dates), len(new_dates), MAX_ASSETS))
    deleteExcessAssets(existing_dates, MAX_ASSETS)

    logging.info('SUCCESS')
