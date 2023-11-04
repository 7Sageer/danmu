import pandas as pd
import psycopg2
from tqdm import tqdm
import ast

# 数据库连接参数
params = {
    "host": "localhost",
    "database": "danmu",
    "user": "myuser",
    "password": "newpassword"
}

# 连接到数据库
conn = psycopg2.connect(**params)
cur = conn.cursor()

# 分块读取CSV文件
chunk_size = 50  
total_rows = 7865
chunks = pd.read_csv('/media/videos.csv', chunksize=chunk_size)
progress_bar = tqdm(total=total_rows, unit="row")
for chunk in chunks:
    video_info_data = []
    video_status_data = []
    video_view_data = []
    video_action_data = []
    
    for index, row in chunk.iterrows():
        # 准备video_info数据
        video_info_data.append((row['BV'], row['Title'], row['Duration'], row['Description']))
        
        # 准备video_status数据
        video_status_data.append((row['BV'], row['Owner Mid'], row['Commit Time'], row['Review Time'], row['Public Time'], row['Reviewer']))
        
        # 准备video_view数据
        for user_id, view_duration in ast.literal_eval(row['View']):
            video_view_data.append((row['BV'], user_id, view_duration))
        
        # 准备video_action数据
        for action_type, user_ids in zip(['like', 'coin', 'favorite'], ['Like', 'Coin', 'Favorite']):
            for user_id in ast.literal_eval(row[user_ids]):
                video_action_data.append((row['BV'], user_id, action_type))
                
    # 执行批量插入
    cur.executemany("""
    INSERT INTO video_info (video_id, title, duration, description)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (video_id) DO NOTHING;
    """, video_info_data)
    
    cur.executemany("""
    INSERT INTO video_status (video_id, owner_id, create_time, review_time, public_time, reviewer_id)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (video_id) DO NOTHING;
    """, video_status_data)
    
    cur.executemany("""
    INSERT INTO video_view (video_id, user_id, view_duration)
    VALUES (%s, %s, %s)
    ON CONFLICT (video_id, user_id) DO NOTHING;
    """, video_view_data)
    
    cur.executemany("""
    INSERT INTO video_action (video_id, user_id, action)
    VALUES (%s, %s, %s)
    ON CONFLICT (video_id, user_id, action) DO NOTHING;
    """, video_action_data)
    progress_bar.update(len(chunk))
    conn.commit()

# 提交事务


# 关闭游标和连接
cur.close()
conn.close()
