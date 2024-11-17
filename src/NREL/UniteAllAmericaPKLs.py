import os
import pandas as pd
from tqdm import tqdm


def extract_year(file_name):
    """Extracts the year from the file name."""
    return file_name.split('_')[3]


def load_and_process_file(file_path, id_col, lat_col, lon_col):
    """Loads the file and processes its data, returning the relevant DataFrame and sensor location."""
    df = pd.read_pickle(file_path)
    df = df.rename(columns={'power_mw': f"power_mw_{id_col}", 'power_mw_norm': f"power_mw_norm_{id_col}"})
    sensor_location = pd.DataFrame({'ID': [id_col], 'Latitude': [lat_col], 'Longitude': [lon_col]})
    return df, sensor_location


def merge_dataframes(df_main, df_to_merge, ID):
    """Merges two DataFrames on the 'Time' column, combining them on common columns."""
    if df_main is None:
        return df_to_merge
    return pd.merge(df_main, df_to_merge[['Time', f"power_mw_{ID}", f"power_mw_norm_{ID}"]], on="Time", how="outer").reset_index(drop=True)


def process_files_for_year(year, file_list, input_path):
    """Processes all files for a given year, merging data and collecting sensor locations."""
    merged_df = None
    sensors_location = pd.DataFrame(columns=['ID', 'Latitude', 'Longitude'])

    for file_name in tqdm(file_list, desc=f"Processing year {year}"):
        if extract_year(file_name) != year:
            continue

        file_path = os.path.join(input_path, file_name)
        _, lat, lon, _, pv_type, capacity, _, _ = file_name.split('_')
        ID = f"{lat}_{lon}_{pv_type}_{capacity}"
        lat, lon = float(lat), float(lon)

        cur_df, cur_sensor_location = load_and_process_file(file_path, ID, lat, lon)

        if ID not in sensors_location['ID'].values:
            sensors_location = pd.concat([sensors_location, cur_sensor_location], ignore_index=True)

        merged_df = merge_dataframes(merged_df, cur_df, ID)

    return merged_df, sensors_location


def main():
    input_path = r'/opt/project/data/NREL/california_2006/processed'
    output_path = r'/opt/project/data/NREL/california_2006/united_processed_data'

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    all_files_list = os.listdir(input_path)
    years = set(extract_year(file_name) for file_name in all_files_list)

    for year in years:
        merged_df, sensors_location = process_files_for_year(year, all_files_list, input_path)

        if merged_df is not None:
            output_united_data_path = os.path.join(output_path, f"united_data_{year}.csv")
            output_sensors_location_path = os.path.join(output_path, f"sensors_location_{year}.csv")

            merged_df.sort_values('Time').reset_index(drop=True).to_csv(output_united_data_path, index=False)
            sensors_location.to_csv(output_sensors_location_path, index=False)


if __name__ == "__main__":
    main()
