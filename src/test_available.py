import random
import pyktok as pyk
import pandas as pd
from tqdm import tqdm
import os
import time

# Check if TikTok is available
def test_available(url,sleep=4,meta_fn = './test'):
    try:
        pyk.save_tiktok(url,False,meta_fn,verbose=False)
    except:
        print(f"TikTok {url} is not available")
        time.sleep(random.randint(1, sleep))
        return 0
    time.sleep(random.randint(1, sleep))
    os.remove(meta_fn)
    return 1

def get_all_data():
    output_path = './Tiktok/data/output'
    data_folders = os.listdir(output_path)
    print("Data Folders in Use: ",data_folders)
    df = pd.DataFrame()
    for folder in tqdm(data_folders,desc='Gathering Data'):
        for file in os.listdir(os.path.join(output_path,folder)):
            if file.startswith('output_hashtags'):
                df = pd.concat([df,pd.read_csv(output_path+'/'+folder+'/'+file)])
    return df

if __name__ == "__main__":
    df = get_all_data()
    print(df.head())
    print(df.info())
    tqdm.pandas()
    df['available'] = df['video_link'].progress_apply(test_available)
    df['last_availablity_check'] = pd.Series([pd.Timestamp.now('US/Pacific')]*len(df)) 
    df['last_availablity_check'] = df.last_availablity_check.dt.tz_localize(None)
    print(f"{len(df[df.available == 0])} TikToks are not available")
    try:
        df.to_excel('./Tiktok/data/available.xlsx',index=False)
    except:
        df.to_csv('./Tiktok/data/available.csv',index=False)