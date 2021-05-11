import logging
# define logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) # set the level of logger first
# f_handler = logging.StreamHandler('logs')
f_handler = logging.FileHandler('logs')
f_handler.setLevel(logging.INFO)
# f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
f_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
f_handler.setFormatter(f_format)
logger.addHandler(f_handler)

# import from core modules
import os
from datetime import datetime, timedelta
import json
import time
import re

# import additional modules
import requests
from fake_useragent import UserAgent
from requests.exceptions import HTTPError
import pandas as pd
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


# the main function to run everything
def driver():
    logger.info('Running data collector')
    parent_dir = './data_dumps'

    # get the date time for creating folder etc
    curr_dt = datetime.now()

    # create the folder
    curr_folder = create_folder(curr_dt, parent_dir)
    logger.info('Created folder %s', curr_folder)

    # get data from api
    curr_data = run_api()

    # convert to data frame
    curr_data = get_raw_data_df(curr_data)
    if(len(curr_data) == 0):
        logger.warning('Empty DataFrame !')

    # add a last modified column
    curr_data['ts'] = curr_dt.strftime('%Y-%m-%d %H-%M')

    # filter only rows with available slots
    curr_data = curr_data.loc[curr_data['available_capacity'] > 0, :]

    # save to disk
    curr_data.to_csv(os.path.join(curr_folder, 'slots_data.csv'), index=False)

    logger.info('Driver run complete')

# create a folder with current date time to store the data
def create_folder(curr_dt, parent_dir):
    curr_dir = os.path.join(parent_dir,
                '{}'.format(curr_dt.strftime('%Y_%m_%d_%H_%M')))

    if(not os.path.isdir(curr_dir)):
        os.mkdir(curr_dir)

    return curr_dir

def get_response(url):
    user_agent = UserAgent()

    retry_strategy = Retry(
            total=3,
            backoff_factor=1, # default is 0 and must be non zero
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    headers = {'User-Agent': user_agent.random}
    response = http.get(url, headers=headers)
    return response

def get_districts_list():
    with open('district_ids.json', 'r') as f:
        data = json.load(f).get('district_ids')

    return data

# run the api on the list of districts and return the combined data
def run_api():
    logger.info('Running districts API')

    # by default run on the current date
    date_today = datetime.today()
    base_url = r'https://cdn-api.co-vin.in/api/v2'

    vaccine_calendar_by_district_url = r'{}/appointment/sessions/public/calendarByDistrict'.format(base_url)

    custom_district_list = get_districts_list()

    combined = []
    for district_id in custom_district_list:
        logger.info('Running district id %s', district_id)
        url = vaccine_calendar_by_district_url + '?district_id={}&date={}'\
                        .format(district_id, date_today.strftime('%d-%m-%Y'))

        try:
            sessions_list = get_response(url).json()
            sessions_list = sessions_list.get('centers')
            combined += sessions_list
        except:
            logger.warning('district id %s failed, ignoring', district_id)
            pass

    logger.info('Districts API run complete')

    return combined

def get_raw_data_df(json_data):
    logger.info('Converting JSON to DataFrame')
    # columns to pick from json
    cols_center = ['name', 'address', 'block_name', 'state_name', 'district_name',
                    'pincode', 'from', 'to', 'fee_type']
    cols_session = ['date', 'available_capacity', 'min_age_limit', 'vaccine', 'slots']
    df_dict = dict([(k, []) for k in cols_center + cols_session])
    # two levels of looping, one for center, another for sessions
    for x in json_data:
        for y in x.get('sessions'):
            for col in cols_center:
                df_dict[col].append(x[col])
            for col in cols_session:
                if(isinstance(y.get(col), list)):
                    df_dict[col].append(', '.join(list(map(str, y[col]))))
                else:
                    df_dict[col].append(y[col])

    centers_df = pd.DataFrame(df_dict)[cols_center + cols_session]
    logger.info('conversion complete')
    return centers_df

if __name__ == '__main__':
    # hacky scheduler
    while(1):
        t = datetime.now()
        if(t.second == 0):
            driver()
