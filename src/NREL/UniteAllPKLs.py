import pandas as pd
import os
from tqdm import tqdm


# Config
input_path = r'/opt/project/data/NREL/processed'
output_path = r'/opt/project/data/NREL/united_processed_data'


sensors_location = pd.DataFrame(columns=['ID', 'Longitude', 'Latitude'])
all_files_list = os.listdir(input_path)

if not os.path.exists(output_path):
    os.makedirs(output_path)


# Waitbaar
total_files = len(all_files_list)
progress_bar = tqdm(total=total_files, desc='Processing Files', unit='file')


for cur_file in all_files_list:
    ID, lat, lon, year = cur_file.split('_')
    lat = float(lat)
    lon = float(lon)

    cur_sensors_location = pd.DataFrame({'ID': [ID], 'Latitude': [lat], 'Longitude': [lon]})
    new_id = sensors_location.query(f"ID == '{ID}'").empty
    if new_id:
        sensors_location = pd.concat([sensors_location, cur_sensors_location], ignore_index=True)

    column_mapping = {'GHI': f"GHI_{ID}",
                      'GHI_norm': f"GHI_norm_{ID}"}

    cur_file_path = os.path.join(input_path, cur_file)
    cur_df = pd.read_pickle(cur_file_path)


    if all_files_list.index(cur_file) == 0:
        cur_df = cur_df.rename(columns=column_mapping)
        merged_df = cur_df
    else:
        relevant_cur_df = cur_df[['Time', 'GHI', 'GHI_norm']]
        relevant_cur_df = relevant_cur_df.rename(columns=column_mapping)
        if set(relevant_cur_df.columns).issubset(set(merged_df.columns)):
            if len(merged_df.columns) == len(cur_df.columns):
                merged_df = pd.concat([merged_df, cur_df.rename(columns=column_mapping)])
            else:
                merged_df = merged_df.set_index('Time').combine_first(relevant_cur_df.set_index('Time')).reset_index()
        else:
            merged_df = pd.merge(merged_df, relevant_cur_df, on=["Time"], how="outer")
    progress_bar.update(1)

merged_df = merged_df.sort_values('Time').reset_index(drop=True)

output_united_data_path = os.path.join(output_path, "united_data.csv")
output_sensors_location_path = os.path.join(output_path, "sensors_location.csv")

merged_df.to_csv(output_united_data_path)
sensors_location.to_csv(output_sensors_location_path)

progress_bar.close()
