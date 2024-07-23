import pandas as pd
import os
import pysolar.solar as solar
import numpy as np
from tqdm import tqdm


# Config
input_path = r'/opt/project/data/NREL/raw'
output_path = r'/opt/project/data/NREL/processed'
hours_to_use = [1, 17] #This is in UTC time zone

if not os.path.exists(output_path):
    os.makedirs(output_path)

all_files_list = os.listdir(input_path)
# Waitbaar
total_files = len(all_files_list)
progress_bar = tqdm(total=total_files, desc='Processing Files', unit='file')

for cur_file in all_files_list:
    ID, lat, lon, year = os.path.splitext(cur_file)[0].split('_')
    lat = float(lat)
    lon = float(lon)

    cur_file_path = os.path.join(input_path, cur_file)
    cur_df = pd.read_csv(cur_file_path, skiprows=2)

    cur_df = cur_df.query(f"Hour >= {hours_to_use[0]} and Hour <= {hours_to_use[1]}").reset_index(drop=True)

    cur_df['Time'] = pd.to_datetime(cur_df[['Year', 'Month', 'Day', 'Hour', 'Minute']])
    cur_df['Time'] = cur_df['Time'].dt.tz_localize('utc')  # This sets tzinfo to UTC

    cur_df['altitude'] = pd.DataFrame([solar.get_altitude(lat, lon, cur_time) for cur_time in cur_df['Time']])
    cur_df['azimuth'] = pd.DataFrame([solar.get_azimuth(lat, lon, cur_time) for cur_time in cur_df['Time']])

    cur_df['csi_haurwitz'] = 1098 * np.sin(np.radians(cur_df['altitude'])) * np.exp(
        -0.057 / (np.sin(np.radians(cur_df['altitude']))))
    cur_df['GHI_norm'] = cur_df['GHI'] / cur_df['csi_haurwitz']

    cur_df_to_save = cur_df[['Time', 'GHI_norm', 'altitude', 'azimuth', 'GHI', 'csi_haurwitz']]

    output_file_path = os.path.join(output_path, os.path.splitext(cur_file)[0] + '.pkl')
    cur_df_to_save.to_pickle(output_file_path)
    progress_bar.update(1)

progress_bar.close()
