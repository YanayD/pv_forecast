import pandas as pd
import os
from tqdm import tqdm

# Config
input_path = r'C:\Users\Yanay\Documents\GitHub\solarcasting\IsraelData\input\United_processed_data\united_data'
output_path = r'C:\Users\Yanay\Documents\GitHub\solarcasting\IsraelData\input\United_processed_data'

if not os.path.exists(output_path):
    os.makedirs(output_path)

sensor_to_predict = "1878195"
num_taps_to_use = 1 # every tap 15 min
num_taps_in_future_to_predict = 1 # should be at least 1
hours_to_use = [5, 13] #This is in UTC time zone

data_df = pd.DataFrame()  # Initialize an empty DataFrame for concatenating cur_data_df
target_df = pd.DataFrame()  # Initialize an empty DataFrame for concatenating cur_target_df

united_data = pd.read_pickle(input_path)
united_data = united_data.query(f"Time.dt.hour >= {hours_to_use[0]} and Time.dt.hour <= {hours_to_use[1]}").reset_index(drop=True)

# Drop Unused data for training
GHI_to_drop_columns = united_data.columns[united_data.columns.str.contains('GHI') & ~united_data.columns.str.contains('norm')]
united_data = united_data.drop(GHI_to_drop_columns, axis = 1)
united_data = united_data.drop(["csi_haurwitz"], axis = 1)

grouped_united_data = united_data.groupby(united_data['Time'].dt.date)

for group_name, group_data in tqdm(grouped_united_data, desc="Processing groups"):

    cur_target_df = group_data[['Time', f"GHI_norm_{sensor_to_predict}"]][
                    num_taps_to_use + num_taps_in_future_to_predict - 1:]

    cur_data_df = group_data.copy()

    exclude_columns = ['Time', 'azimuth', 'altitude']
    updated_columns = {col: col + "_-0" if col not in exclude_columns else col for col in cur_data_df.columns}
    cur_data_df = cur_data_df.rename(columns=updated_columns)


    for i_tap in range(1, num_taps_to_use):
        delayed_df = group_data.shift(i_tap).drop(['Time', 'azimuth', 'altitude'], axis = 1)
        updated_columns = {col: col + "_-" + str(i_tap) for col in delayed_df.columns}
        delayed_df = delayed_df.rename(columns=updated_columns)
        cur_data_df = pd.concat([cur_data_df, delayed_df], axis=1)

    cur_data_df = cur_data_df[num_taps_to_use - 1:-num_taps_in_future_to_predict]

    data_df = pd.concat([data_df, cur_data_df], ignore_index=True)
    target_df = pd.concat([target_df, cur_target_df], ignore_index=True)

output_data_path = os.path.join(output_path, "data")
output_target_path = os.path.join(output_path, "target")

data_df.to_pickle(output_data_path)
target_df.to_pickle(output_target_path)