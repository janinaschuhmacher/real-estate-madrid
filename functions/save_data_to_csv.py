import shutil
from utils.global_variables import DATA_DIRECTORY
import pandas as pd
from os.path import exists


def backup_idealista_data(
    file_name: str,
    data_dir: str = DATA_DIRECTORY,
) -> None:
    """
    Function that writes dataframe with data that was extracted from the idealista API
    to a csv file.

    :param file_name: File that will be backed up.
    :param data_dir: Directory to the original file.
    """
    file_src = data_dir + file_name
    file_dest = data_dir + "idealista_data_backups/" + file_name
    if exists(file_src):
        shutil.copyfile(file_src, file_dest)
        print("Backup for file ", file_name, "created as", file_dest)
    else:
        print("No existing file ", file_name, "found.")


def append_idealista_data(
    file_name: str,
    df_new_data: pd.DataFrame,
    data_dir: str = DATA_DIRECTORY,
) -> None:
    """Function that appends rows

    :param file_name: File that will be appended.
    :param df_new_data: Dataframe with new idealista data.
    :param data_dir: File location. Defaults to DATA_DIRECTORY.
    """
    file_history = data_dir + file_name
    if exists(file_history):
        df_new_data.to_csv(file_history, mode="a", index=False, header=False)
        print(len(df_new_data), "lines written to", file_history)
    else:
        df_new_data.to_csv(file_history, index=False)
        print("New file", file_history, "created.")


def remove_duplicates_from_csv(
    file_name: str,
    data_dir: str = DATA_DIRECTORY,
) -> None:
    """
    Function that removes duplicate rows from a csv file.
    Used to deduplicate that idealista data files.

    :param file_name: File that will be deduplicated.
    :param data_dir: File location.
    """
    file_history = data_dir + file_name
    df = pd.read_csv(file_history)
    len_original = len(df)
    df.drop_duplicates(
        subset=["propertyCode", "price", "size"],
        inplace=True,
    )
    len_dedup = len(df)
    if len_dedup < len_original:
        print(
            len_original - len_dedup, "duplicates were removed from file", file_history
        )
    df.to_csv(file_history, index=False)
