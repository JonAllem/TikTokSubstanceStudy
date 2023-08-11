import pandas as pd
import os

path = './data/output/'
for folder in os.listdir(path)[1:]:
    if folder!= 'output_hashtags_test':
        df =pd.DataFrame()
        for file in os.listdir(path+folder):
            if file.split('_')[0]!='output' and file!='videos' and file.endswith('.csv'):
                print(folder,file)
                tag = file.split('_')[0]
                temp = pd.read_csv(path+folder+'/'+file)
                temp.insert(0,'hashtag',[tag]*len(temp))
                df = pd.concat([df,temp])
        print(df)
        df.drop_duplicates(subset=['video_id']).sort_values(by=['n_plays','n_likes'],ascending=False).iloc[:20].to_csv(path+folder+'/'+folder+'.csv',index=False)
