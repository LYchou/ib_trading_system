import os

def makedirs(folder_path:str):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)