from TikTokApi import TikTokApi
from TikTokApi.exceptions import TikTokException
from TikTokApi.tiktok import ERROR_CODES
import pandas as pd
from tqdm import tqdm
from utils import simple_dict
import argparse
import os
import pyktok as pyk
import logging
logging.basicConfig(filename='./log/hashtags_exception.log', filemode='w',
    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',level=logging.INFO)
logging.info('Hashtag Exception Logging Started')

def getTikTok(hashtags:list,limit:int=None,dir=False,op_path=None,ip_file:str=None,topn:int=None): #TODO: Rename this function
    """
    Get TikTok video stats by hashtag and save to CSV file.

    Parameters
    ----------
    hashtags : list
        List of hashtags to search for
    limit : int
        Number of videos to search for
    """
    all_tiktok_topn = pd.DataFrame()
    
    with TikTokApi() as api:
        progress_hashtags = tqdm(hashtags,position=0)
        hashtags_video_count = dict() #* Number of videos for each hashtag
        op_file_name = ip_file.replace('input','output').replace('.txt','').strip()
        path = os.path.join(op_path,op_file_name)
        if not os.path.exists(path): os.makedirs(path)
        if dir: logging.info(f'Logging for Output Dir {path}')
        else: logging.info(f'Logging for Output File {path}')
        for hashtag in progress_hashtags:
            logging.info(f'Logging Hashtag {hashtag}')
            progress_hashtags.set_description("Processing hashtag: {}".format(hashtag))

            tag = api.hashtag(name=hashtag)
            # print(tag.info_full())
            tag_info = tag.info_full()
            try:
                n_videos = tag_info['challengeInfo']['stats']['videoCount']
            except TikTokException as e:
                logging.exception(f"Exception {ERROR_CODES[str(e).split('->')[0].strip()]} occured on the hashtag {hashtag}",exc_info=True)
                print(f"Exception {ERROR_CODES[str(e).split('->')[0].strip()]} occured on the hashtag {hashtag}",e)
                continue
            hashtags_video_count[hashtag] = n_videos
            if limit is not None: #* if limit is set, use that instead
                print('Limit is set to',limit)
                logging.info(f'Limit is set to {limit} for hashtag {hashtag}')
                if n_videos >= limit: #* if the number of videos is greater than the limit, use the limit, otherwise use the number of videos
                    n_videos = limit
            n_videos= 100
            progress_hashtags.set_postfix_str("Total videos in process: {}".format(n_videos))

            processed_videos = []
            print(n_videos,sum(1 for _ in api.hashtag(name='funny').videos(n_videos)))
            for video in tqdm(tag.videos(n_videos),position=1,desc='Processing videos',leave=False):
                processed_videos.append(simple_dict(video.as_dict))
            video_df = pd.DataFrame(processed_videos)
            
            # print(video_df)
            video_df.to_csv(path+'/'+(f'{hashtag}_hashtag_{n_videos}_videos.csv' if limit is not None else f'{hashtag}_hashtag_all_videos.csv'), index=False)
            all_tiktok_topn = pd.concat([all_tiktok_topn,video_df])
            logging.info(f'Hashtag {hashtag} logging successful')
        all_tiktok_topn.sort_values(by=['n_plays','n_likes'],ascending=False).iloc[:topn].to_csv(path+'/'+op_file_name+'.csv', index=False)
        print(f'Number of videos for each hashtag\n{hashtags_video_count}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_is_dir',type=bool,help='If the Input directory is a file or a directory',default=False)
    parser.add_argument('--input_path',type=str,help='Input File Path or Input Directory Path',default='./data/input/input_hashtags_test.txt')
    parser.add_argument('--input_file_ignore',nargs='*',help='Input Files to ignore; Example test.txt', type=str, default=None)
    parser.add_argument('--output_limit',type=int,help='Limit on Input from TikTokAPI',default=None)
    parser.add_argument('--output_dir',type=str,default='./data/output/',help='Output file directory')
    parser.add_argument('--output_format',type=str,help='Format of output file',default='csv')
    parser.add_argument('--topn',type=int, help='Top N videos wanted in output',default=20)
    parser.add_argument('--method',type=str,choices=['pyktok','tiktokapi'],default='tiktokapi')
    args = parser.parse_args()

    if args.input_is_dir: #* If the input is the entire directory
        input_files = os.listdir(args.input_path)
        for file in input_files:
            if file not in args.input_file_ignore:
                with open((args.input_path if args.input_path[-1] in ['/','\\'] else args.input_path+'/')+file) as f:
                    multitopic_input_hashtags = f.read().splitlines()
                f.close()
                print(multitopic_input_hashtags)
                if args.method == 'pytok':
                    getTikTok(multitopic_input_hashtags,dir=args.input_is_dir,op_path=args.output_dir,ip_file=file,topn=args.topn)
                else:
                    raise NotImplementedError
                    
    else:
        with open(args.input_path) as f:
            input_hashtags = f.read().splitlines()
        f.close()
        print(input_hashtags)
        getTikTok(input_hashtags,dir=args.output_dir,op_path=args.output_dir,ip_file=args.input_path.split('/')[-1],topn=args.topn,limit=100)
        