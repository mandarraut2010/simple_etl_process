import glob
import pandas as pd
from datetime import datetime


def extract_csv(csv_file, df):

    for csv in csv_file:
        csv_df = pd.read_csv(csv)
        df = pd.concat([df, csv_df], ignore_index=True)
    return df


def extract_json(json_file, df):
    for js in json_file:
        json_df = pd.read_json(js, lines=True)
        df = pd.concat([df, json_df], ignore_index=True)
    return df


def transform(df):
    df['height'] = round(df.height * 0.0254, 2)
    df['weight'] = round(df.weight * 0.45359237, 2)
    return df


def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open('logfile.txt', "a") as f:
        f.write(timestamp + ',' + message + '\n')


def main(csv_file, json_file):
    log_progress("ETL Job Started")
    df = pd.DataFrame(columns=['name', 'height', 'weight'])
    log_progress("Extract phase Started")
    csv_data = extract_csv(csv_file, df)
    json_data = extract_json(json_file, df)
    final_data = pd.concat([csv_data, json_data], ignore_index=True)
    log_progress("Extract phase Ended")
    log_progress("Transform phase Started")
    transformed_data = transform(final_data)
    print("Transformed Data")
    log_progress("Transform phase Ended")
    log_progress("Load phase Started")
    target_loc = 'loaded_data.csv'
    load_data(target_loc, transformed_data)
    log_progress("Load phase Ended")
    log_progress("ETL Job Ended")


if __name__ == '__main__':
    csv_files = glob.glob('s*.csv')
    json_files = glob.glob('*.json')
    main(csv_files, json_files)
