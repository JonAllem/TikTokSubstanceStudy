import argparse
import pyktok as pyk
import pandas as pd
from tqdm import tqdm
import os
import time


def get_metadata_and_video(path,video_output):
    print(path)
    df = pd.read_csv(path)
    print(df)
    print()
    try:
        pyk.save_tiktok_multi(df.video_link.to_list(),True,video_output+'/video_output.csv',7)
    except:
        print("Error",path)
        time.sleep(10)
        pyk.save_tiktok_multi(df.video_link.to_list(),True,video_output+'/video_output.csv',7)
        return
    # df.video_link.apply(lambda x: pyk.save_tiktok(x,True,'data\output\output_hashtags_cannabis_edibles\downnload'))

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_is_dir_of_dir', default=True)
    parser.add_argument('--input_dir_ignore',nargs='*',help='Input Files to ignore; Example test.txt', type=str, default=None)
    parser.add_argument('--input_path', default='./data/output')
    args = parser.parse_args()
    print(args.input_dir_ignore)
    if args.input_is_dir_of_dir: #* If the input is the entire directory
        input_folders = os.listdir(args.input_path)
        for input_folder in tqdm(input_folders,position=0):
            print(args.input_path+'/'+input_folder)
            if args.input_path+'/'+input_folder not in args.input_dir_ignore:
                input_files = os.listdir(args.input_path+'/'+input_folder+'/')
                for file in tqdm(input_files,position=1):
                    path = os.path.join(args.input_path+'/'+input_folder+'/','videos')
                    if not os.path.exists(path): os.makedirs(path)
                    input_path = args.input_path+'/'+input_folder+'/'+file
                    if file.split('_')[0]=='output':
                        get_metadata_and_video(path=input_path,video_output=path)