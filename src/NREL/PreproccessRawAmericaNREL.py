import pandas as pd
import os
import pysolar.solar as solar
import numpy as np
from tqdm import tqdm


# Config
input_path = r'/opt/project/data/NREL/california_2006/raw'
output_path = r'/opt/project/data/NREL/california_2006/processed'
hours_to_use = [5, 19 ]

if not os.path.exists(output_path):
    os.makedirs(output_path)

all_files_list = os.listdir(input_path)
all_files_list = [file for file in all_files_list if file.startswith('Actual')]

# Waitbaar
total_files = len(all_files_list)
progress_bar = tqdm(total=total_files, desc='Processing Files', unit='file')

for cur_file in all_files_list:
    lat, lon, year, pv_type, capacity = os.path.splitext(cur_file)[0].split('_')[1:6]
    ID = f"{lat}_{lon}_{pv_type}_{capacity}"
    lat = float(lat)
    lon = float(lon)

    cur_file_path = os.path.join(input_path, cur_file)

    cur_df = pd.read_csv(cur_file_path)
    cur_df['LocalTime'] = pd.to_datetime(cur_df['LocalTime'])
    cur_df.rename(columns={"Power(MW)": "power_mw", "LocalTime": "Time"}, inplace=True)
    cur_df = cur_df.query(f"Time.dt.hour >= {hours_to_use[0]} and Time.dt.hour <= {hours_to_use[1]}").reset_index(drop=True)

    cur_df['Time'] = pd.to_datetime(cur_df[['Year', 'Month', 'Day', 'Hour', 'Minute']])
    cur_df['Time'] = cur_df['Time'].dt.tz_localize('utc')  # This sets tzinfo to UTC

    cur_df['altitude'] = pd.DataFrame([solar.get_altitude(lat, lon, cur_time) for cur_time in cur_df['Time']])
    cur_df['azimuth'] = pd.DataFrame([solar.get_azimuth(lat, lon, cur_time) for cur_time in cur_df['Time']])

    cur_df['csi_haurwitz'] = 1098 * np.sin(np.radians(cur_df['altitude'])) * np.exp(
        -0.057 / (np.sin(np.radians(cur_df['altitude']))))
    cur_df['GHI_norm'] = cur_df['GHI'] / cur_df['csi_haurwitz']

    cur_df_to_save = cur_df[['Time', 'power_mw_norm', 'altitude', 'azimuth', 'power_mw', 'csi_haurwitz']]

    output_file_path = os.path.join(output_path, os.path.splitext(cur_file)[0] + '.pkl')
    cur_df_to_save.to_pickle(output_file_path)
    progress_bar.update(1)

progress_bar.close()
