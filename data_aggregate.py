# import from core modules
import os
from datetime import datetime, timedelta
import json
import time
import re

import pandas as pd
import numpy as np

data_dir = 'data_dumps'

results = []
# loop through the data dumps and store
for folder in os.listdir(data_dir):
    _path = os.path.join(data_dir, folder, 'slots_data.csv')

    if os.path.exists(_path):
        df_temp = pd.read_csv(_path)
        df_temp = df_temp.loc[df_temp['available_capacity'] > 0, :]

        # ignore if the df has 0 rows
        if(len(df_temp) == 0):
            continue

        dt = datetime.strptime(folder, '%Y_%m_%d_%H_%M')

        df_temp['date'] = pd.to_datetime(df_temp['date'])

        # create year, date month etc
        df_temp['api_run_year'] = dt.year
        df_temp['api_run_month'] = dt.month
        df_temp['api_run_day'] = dt.day
        df_temp['api_run_hour'] = dt.hour
        df_temp['api_run_minute'] = dt.minute

        df_temp['date_diff_api_run_date'] = (df_temp['date'].dt.date - dt.date()).dt.days

        results.append(df_temp)

# combine
results = pd.concat(results, axis=0, ignore_index=True)

results.to_csv(os.path.join('analysis_data', 'combined_data.csv'), index=False)
