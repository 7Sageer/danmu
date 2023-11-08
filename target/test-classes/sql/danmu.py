import pandas as pd
import psycopg2
from tqdm import tqdm
import ast
# 数据库连接参数
params = {
    "host": "localhost",
    "database": "danmu",
    "user": "postgres",
    "password": "747576"
}

# 连接到数据库
conn = psycopg2.connect(**params)
cur = conn.cursor()
cur.execute("truncate table danmu cascade;")

# 读取CSV文件
df_danmu = pd.read_csv('data/danmu.csv')

# 遍历每行数据
for index, row in tqdm(df_danmu.iterrows(), total=df_danmu.shape[0]):
    cur.execute("""
    INSERT INTO danmu (video_id, user_id, time, content)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
    """, (row['BV'], row['Mid'], row['Time'], row['Content']))

# 提交事务
conn.commit()

# 关闭游标和连接
cur.close()
conn.close()
