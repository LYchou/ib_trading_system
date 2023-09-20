import pandas as pd
import pytz
import os


def group_execution_and_commission_by_date(exceutions:pd.DataFrame, commissions:pd.DataFrame, valid_clientId:int=None):

    """
    Generate a dictionary based on the US/Eastern dates in the exceutions dataframe for a specified ClientId.
    
    This function will merge the 'exceutions' and 'commissions' dataframes based on the 'ExecId' and then group the merged
    dataframe by the US/Eastern dates. Only the records corresponding to the given 'valid_clientId' are considered.
    
    Parameters:
    - exceutions (pd.DataFrame): The exceutions dataframe with transaction details.
    - commissions (pd.DataFrame): The commissions dataframe with commission details for each transaction.
    - valid_clientId (int or str, optional): The specific ClientId for which records should be fetched. If not provided (None), records for all ClientIds are considered.
    
    Returns:
    - dict: A dictionary with 'US/Eastern' dates as keys. Each key corresponds to a tuple of sub-dataframes:
            (sub_exceutions, sub_commissions), representing the filtered exceutions and commissions for that date.
            
    Example:
    result = group_execution_and_commission_by_date(exceutions_df, commissions_df, 1002)
    # result is a dictionary where each key is a US/Eastern date and each value is a tuple of dataframes 
    # corresponding to that date's exceutions and commissions for the provided ClientId.
    """
    
    # Merge the two dataframes on 'ExecId'
    merged_df = pd.merge(exceutions, commissions, on="ExecId", how="inner")
    
    # Convert the 'Time' column to datetime format with Taipei timezone
    merged_df['Time'] = pd.to_datetime(merged_df['Time'], utc=True).dt.tz_convert('Asia/Taipei')
    
    # Convert the 'Time' column to 'US/Eastern' timezone and extract the date
    merged_df['Time(US/Eastern)'] = merged_df['Time'].dt.tz_convert('US/Eastern').dt.date
    
    # Group by the 'US/Eastern' date and generate the dictionary
    datewise_dict = {}
    
    for group, sub_df in merged_df.groupby('Time(US/Eastern)'):
        if valid_clientId!=None:
            sub_df = sub_df[sub_df['ClientId']==valid_clientId]
        sub_commissions = sub_df[commissions.columns].reset_index(drop=True)
        sub_exceutions = sub_df[exceutions.columns].reset_index(drop=True)
        datewise_dict[group] = (sub_exceutions, sub_commissions)
    
    return datewise_dict

def lastest_date_callback(datewise_dict:dict):
    """
    Fetch the exceutions and commissions dataframes corresponding to the latest date in the datewise_dict dictionary.
    
    Parameters:
    - datewise_dict (dict): A dictionary with 'US/Eastern' dates as keys. Each key corresponds to a tuple of sub-dataframes:
            (sub_exceutions, sub_commissions), representing the filtered exceutions and commissions for that date.
    
    Returns:
    - tuple: A tuple of two dataframes (exceutions, commissions) corresponding to the latest date in datewise_dict.
    """
    
    # Check if datewise_dict is empty
    if not datewise_dict:
        raise ValueError("The datewise_dict dictionary is empty.")
    
    # Fetch the latest date
    latest_date = max(datewise_dict.keys())
    
    # Fetch the exceutions and commissions dataframes for the latest date
    exceutions, commissions = datewise_dict[latest_date]
    
    return exceutions, commissions


def makedirs(folder_path:str):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
