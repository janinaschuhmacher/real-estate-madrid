import sys
sys.path.append("/Desktop/repos/real_estate_madrid")

from global_variables import DATA_DIRECTORY

import shutil
import pandas as pd
import errno
import os
from datetime import datetime


def initialize_csv(
    file_name: str = "idealista_data.csv",
    data_dir: str = DATA_DIRECTORY,
) -> pd.DataFrame:
    """
    Function that reads existing idealista data from data directory.

    Args:
        file_name (str, optional): Name of exisiting idealista data. Defaults to "idealista_data.csv".
        data_dir (str, optional): Directory of existing idealista data. Defaults to DATA_DIRECTORY.

    Returns:
        pd.DataFrame: Dataframe with existing idealista data.
    """
    file_history = data_dir + file_name
    try:
        df_existing_data = pd.read_csv(
            file_history,
            header=0,
            dtype={"propertyCode": object, "externalReference": object},
        ).fillna("")

    except FileNotFoundError:
        df_existing_data = pd.DataFrame()

    return df_existing_data


def backup_idealista_data(
    file_name: str,
    backup_file_name: str = None,
    data_dir: str = DATA_DIRECTORY,
) -> None:
    """
    Function that writes dataframe with data that was extracted from the idealista API
    to a csv file.

    Args:
        file_name (str): File that will be backed up.
        backup_file_name (str, optional): Name of the backup file. Defaults to filename_backup_todays_date.csv.
        data_dir (str, optional): Directory to the original file.. Defaults to DATA_DIRECTORY.
    """
    # set working directory
    os.chdir("real_estate_madrid")
    file_src = os.path.join(data_dir, file_name)
    if backup_file_name is None:
        backup_file_name = (
            file_name.strip(".csv")
            + "_backup_"
            + datetime.today().strftime("%Y-%m-%d")
            + ".csv"
        )
    file_dest = os.path.join(data_dir, "idealista_data_backups/", backup_file_name)
    try:
        shutil.copy(file_src, file_dest)
    except IOError as e:
        # ENOENT(2): file does not exist, raised also on missing dest parent dir
        print(
            "No existing backup folder idealista_data_backups/ found. Folder will be created."
        )
        if e.errno != errno.ENOENT:
            raise
        # try creating parent directories
        # try:
        #     os.makedirs(os.path.dirname(file_dest))
        #     shutil.copy(file_src, file_dest)
        #     print("Backup for file ", file_name, "created as", file_dest)


def append_idealista_data(
    df_new_data: pd.DataFrame,
    data_dir: str = DATA_DIRECTORY,
    file_name: str = "idealista_data.csv",
    df_existing_data: pd.DataFrame = None,
) -> None:
    """
    Function that appends rows to idealista data csv or creates a new csv
    if existing data cannot be appended.

    Args:
        df_new_data (pd.DataFrame): Dataframe with new idealista data.
        data_dir (str, optional): File location.  Defaults to DATA_DIRECTORY.
        file_name (str): File that will be appended. Defaults to "idealista_data.csv".
        df_existing_data (pd.DataFrame, optional): Dataframe with existing idealista data. Defaults to None.
    """
    file_history = os.path.join(data_dir, file_name)
    # get df with existing data
    if df_existing_data is None:
        df_existing_data = initialize_csv()

    # check if columns are identical
    if set(df_new_data.columns) == set(df_existing_data.columns):
        # append csv
        df_combined_data = pd.concat([df_existing_data, df_new_data])
        df_combined_data.to_csv(file_history, index=False)
        print(len(df_new_data), "lines written to", file_history)

    else:
        missing_columns = set(df_existing_data.columns) - set(df_new_data.columns)
        # less columns in new dataframe
        if len(missing_columns) >= 1:
            print(len(missing_columns), "column(s) were not returned:")
            print(list(missing_columns))

        new_columns = set(df_new_data.columns) - set(df_existing_data.columns)
        # additional columns in new dataframe
        if len(new_columns) >= 1:
            print(len(new_columns), "new column(s) were returned:\n", new_columns)

        # write new data to file
        new_file_name = (
            file_name.strip(".csv")
            + "_"
            + datetime.today().strftime("%Y-%m-%d")
            + ".csv"
        )
        file_dest = os.path.join(data_dir, new_file_name)
        df_new_data.to_csv(file_dest, index=False)


def remove_duplicates_from_csv(
    file_name: str,
    data_dir: str = DATA_DIRECTORY,
) -> None:
    """
    Function that removes duplicate rows from a csv file.
    Used to deduplicate that idealista data files.

    Args:
        file_name (str): File that will be deduplicated.
        data_dir (str, optional): File location. Defaults to DATA_DIRECTORY.
    """
    file_history = data_dir + file_name
    df = pd.read_csv(file_history)
    len_original = len(df)
    df.drop_duplicates(
        subset=["propertyCode", "price", "size"], inplace=True, keep="last"
    )
    len_dedup = len(df)
    if len_dedup < len_original:
        print(
            len_original - len_dedup, "duplicates were removed from file", file_history
        )
    df.to_csv(file_history, index=False)
