import pandas as pd
import os
import shutil


video_paths = [file for file in os.listdir('./') if file.endswith(".mp4")]
print(video_paths)
path = './data/output/'
for folder in os.listdir(path)[1:]:
    if folder!= 'output_hashtags_test':
        for file in os.listdir(path+folder):
            if file.split('_')[0]!='output' and file!='videos' and file.endswith('.csv'):
                df = pd.read_csv(path+folder+'/'+file)
                # print('7020029620258950406' in df.video_id.astype(str).to_list())
                src_path_list = [video_path for video_path in video_paths if video_path.split('_')[-1][:-4] in df.video_id.astype(str).unique()]
                print(src_path_list)
                for src_path in src_path_list:
                    src = './'+src_path
                    dest_dir = os.path.join('./data/downloads',folder.split('output_hashtags_')[-1])
                    if not os.path.exists(dest_dir): os.makedirs(dest_dir)
                    dest = os.path.join(dest_dir,src_path)
                    print(src,dest)
                    if os.path.exists(src): shutil.move(src,dest)
        # print(df)
        # df.drop_duplicates(subset=['video_id']).sort_values(by=['n_plays','n_likes'],ascending=False).iloc[:20].to_csv(path+folder+'/'+folder+'.csv',index=False)